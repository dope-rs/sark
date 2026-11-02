use dope_quic::wire::stream::{Direction, Id, Kind};
use o3::{
    alloc,
    buffer::{resident, segments},
    depot::map::{self, flat},
};
use sark_core::manifold::stream::Boundary;
use sark_http::{field::metadata::section::rules, payload::response::Method};

use crate::{
    conn::{
        self, event,
        io::ingress::{
            receive::Recv,
            stream::{Barrier, push, state},
            wait,
        },
    },
    stream,
};

pub(in crate::conn::io::ingress) fn direction(id: Id) -> Result<Direction, conn::Error> {
    match id.kind() {
        Kind::ClientBidi => Ok(Direction::Bidi),
        Kind::ClientUni | Kind::ServerUni => Ok(Direction::Uni),
        Kind::ServerBidi => Err(conn::Error::RequestStream {
            stream_id: id.get(),
        }),
    }
}

pub(super) enum Uni<'a, 'd> {
    Unclassified(Unclassified<'a, 'd>),
    Control(&'a mut Recv<'d>),
    Push(&'a mut Recv<'d>, &'a mut push::State),
    QpackEncoder(&'a mut Recv<'d>),
    QpackDecoder(&'a mut Recv<'d>),
    Unknown(&'a mut Recv<'d>),
}

pub(in crate::conn) enum Release {
    Missing,
    NotHeld,
    Released,
}

enum State {
    Uni,
    Request(state::State),
    Push(push::State),
    Unknown,
}

pub(in crate::conn) struct Entry<'d> {
    id: Id,
    recv: Recv<'d>,
    state: State,
}

const _: () = assert!(size_of::<Entry<'static>>() == 12 * size_of::<usize>());

/// A receive identity whose originating request becomes visible with its output.
pub(in crate::conn) enum Outgoing<'a, 'd> {
    Existing {
        state: &'a mut state::State,
        method: Method,
    },
    New {
        slot: map::Vacancy<&'a mut flat::Table<Entry<'d>>, u64>,
        entry: Entry<'d>,
    },
}

impl Outgoing<'_, '_> {
    pub(in crate::conn) fn commit(self) {
        match self {
            Self::Existing { state, method } => state.method = Some(method),
            Self::New { slot, entry } => {
                slot.insert(entry);
            }
        }
    }
}

pub(super) struct Unclassified<'a, 'd> {
    entry: map::Found<&'a mut flat::Table<Entry<'d>>>,
    critical: &'a mut CriticalSet<'d>,
    active: usize,
    limit: usize,
}

impl<'d> Unclassified<'_, 'd> {
    pub(super) fn recv(&mut self) -> &mut Recv<'d> {
        &mut self.entry.get_mut().recv
    }

    pub(super) fn classify(
        self,
        kind: stream::UniType,
        role: event::Role,
    ) -> Result<(), conn::Error> {
        let id = self.entry.get().id;
        if !kind.is_critical() && self.active > self.limit {
            return Err(conn::Error::StreamAdmission {
                stream_id: id.get(),
                active: self.active,
                capacity: self.limit,
            });
        }
        if matches!(kind, stream::UniType::Push)
            && (role != event::Role::Client || id.kind() != Kind::ServerUni)
        {
            return Err(conn::Error::PushStream {
                stream_id: id.get(),
                role,
            });
        }
        let vacant = match kind {
            stream::UniType::Control => self.critical.control.vacant(kind, id)?,
            stream::UniType::QpackEncoder => self.critical.qpack_encoder.vacant(kind, id)?,
            stream::UniType::QpackDecoder => self.critical.qpack_decoder.vacant(kind, id)?,
            stream::UniType::Push => {
                self.entry.into_mut().state = State::Push(push::State::default());
                return Ok(());
            }
            stream::UniType::Unknown(_) => {
                self.entry.into_mut().state = State::Unknown;
                return Ok(());
            }
        };
        let entry = self.entry.remove();
        vacant.insert(id, entry.recv);
        Ok(())
    }
}

pub(in crate::conn) struct Table<'d> {
    entries: flat::Table<Entry<'d>>,
    critical: CriticalSet<'d>,
    plan: map::Plan,
    limit: map::Plan,
}

impl<'d> Table<'d> {
    pub(in crate::conn) fn try_new(
        plan: map::Plan,
        limit: map::Plan,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            entries: flat::Table::try_new(plan)?,
            critical: CriticalSet::default(),
            plan,
            limit,
        })
    }

    pub(in crate::conn::io::ingress) fn receive(
        &mut self,
        id: Id,
        pool: &segments::Pool<resident::Shared<'d>>,
        supplied: Option<event::Token>,
    ) -> Result<&mut Recv<'d>, conn::Error> {
        let dir = direction(id)?;
        let maximum = self.plan.capacity().get() - self.critical.len();
        if let Some(recv) = self.critical.recv_mut(id) {
            recv.verify(supplied)?;
            return Ok(recv);
        }

        let active = self.entries.len();
        let limit = self.limit.capacity().get();
        match self.entries.entry(id.number(), |entry| entry.id == id) {
            Some(flat::Entry::Occupied(slot)) => {
                let recv = &mut slot.into_mut().recv;
                recv.verify(supplied)?;
                Ok(recv)
            }
            Some(flat::Entry::Vacant(slot)) => {
                if matches!(dir, Direction::Bidi) && active >= limit {
                    return Err(conn::Error::StreamCapacity {
                        stream_id: id.get(),
                        active,
                        capacity: limit,
                    });
                }
                if matches!(dir, Direction::Uni) && active >= maximum {
                    return Err(conn::Error::StreamAdmission {
                        stream_id: id.get(),
                        active,
                        capacity: maximum,
                    });
                }
                let token = match supplied {
                    Some(token) => token,
                    None => id.into(),
                };
                let state = if matches!(dir, Direction::Bidi) {
                    State::Request(state::State::default())
                } else {
                    State::Uni
                };
                let entry = slot.insert(Entry {
                    id,
                    recv: Recv::try_new(pool, token)?,
                    state,
                });
                Ok(&mut entry.recv)
            }
            None => Err(match dir {
                Direction::Bidi => conn::Error::StreamCapacity {
                    stream_id: id.get(),
                    active,
                    capacity: limit,
                },
                Direction::Uni => conn::Error::StreamAdmission {
                    stream_id: id.get(),
                    active,
                    capacity: maximum,
                },
            }),
        }
    }

    pub(in crate::conn) fn outgoing(
        &mut self,
        target: stream::Target,
        pool: &segments::Pool<resident::Shared<'d>>,
        method: Option<Method>,
    ) -> Result<Option<Outgoing<'_, 'd>>, conn::Error> {
        let Some(method) = method else {
            return Ok(None);
        };
        let id = target.stream_id();
        if id.kind() != Kind::ClientBidi {
            return Err(conn::Error::RequestStream {
                stream_id: id.get(),
            });
        }
        let token = match target.key() {
            Some(key) => event::Token::new(key.raw().get()).ok_or(conn::Error::Identity {
                raw: key.raw().get(),
            })?,
            None => id.into(),
        };
        let active = self.entries.len();
        let limit = self.limit.capacity().get();
        match self.entries.entry(id.number(), |entry| entry.id == id) {
            Some(flat::Entry::Occupied(entry)) => {
                let entry = entry.into_mut();
                entry.recv.verify(Some(token))?;
                let State::Request(state) = &mut entry.state else {
                    return Err(conn::Error::RequestStream {
                        stream_id: id.get(),
                    });
                };
                if state.method.is_some() {
                    return Err(conn::Error::Malformed {
                        stream_id: id.get(),
                        kind: rules::Kind::Request,
                    });
                }
                Ok(Some(Outgoing::Existing { state, method }))
            }
            Some(flat::Entry::Vacant(slot)) if active < limit => {
                let entry = Entry {
                    id,
                    recv: Recv::try_new(pool, token)?,
                    state: State::Request(state::State::request(method)),
                };
                Ok(Some(Outgoing::New { slot, entry }))
            }
            Some(flat::Entry::Vacant(_)) | None => Err(conn::Error::StreamCapacity {
                stream_id: id.get(),
                active,
                capacity: limit,
            }),
        }
    }

    pub(super) fn request_mut(&mut self, id: Id) -> Option<(&mut Recv<'d>, &mut state::State)> {
        let entry = self.entries.get_mut(id.number(), |entry| entry.id == id)?;
        match &mut entry.state {
            State::Request(state) => Some((&mut entry.recv, state)),
            State::Uni | State::Push(_) | State::Unknown => None,
        }
    }

    pub(super) fn uni_mut(&mut self, id: Id) -> Option<Uni<'_, 'd>> {
        if self.critical.contains(id) {
            return self.critical.get_mut(id);
        }
        let active = self.entries.len();
        let slot = match self.entries.entry(id.number(), |entry| entry.id == id) {
            Some(flat::Entry::Occupied(slot)) => slot,
            Some(flat::Entry::Vacant(_)) | None => return None,
        };
        if matches!(slot.get().state, State::Uni) {
            return Some(Uni::Unclassified(Unclassified {
                entry: slot,
                critical: &mut self.critical,
                active,
                limit: self.limit.capacity().get(),
            }));
        }
        let entry = slot.into_mut();
        match &mut entry.state {
            State::Push(state) => Some(Uni::Push(&mut entry.recv, state)),
            State::Unknown => Some(Uni::Unknown(&mut entry.recv)),
            State::Request(_) | State::Uni => None,
        }
    }

    pub(in crate::conn) fn recv_mut(&mut self, id: Id) -> Option<&mut Recv<'d>> {
        if let Some(recv) = self.critical.recv_mut(id) {
            return Some(recv);
        }
        self.entries
            .get_mut(id.number(), |entry| entry.id == id)
            .map(|entry| &mut entry.recv)
    }

    pub(in crate::conn) fn token(&self, id: Id) -> Option<event::Token> {
        if let Some(recv) = self.critical.recv(id) {
            return Some(recv.token);
        }
        self.entries
            .get(id.number(), |entry| entry.id == id)
            .map(|entry| entry.recv.token)
    }

    pub(in crate::conn) fn is_blocked(&self, id: Id) -> bool {
        if let Some(recv) = self.critical.recv(id) {
            return recv.wait.is_some();
        }
        self.entries
            .get(id.number(), |entry| entry.id == id)
            .is_some_and(|entry| {
                entry.recv.wait.is_some()
                    || match &entry.state {
                        State::Request(state) => matches!(
                            state.data_barrier,
                            Barrier::Queued | Barrier::Held | Barrier::Rejected
                        ),
                        State::Push(state) => {
                            state.is_waiting()
                                || matches!(
                                    state.message.data_barrier,
                                    Barrier::Queued | Barrier::Held | Barrier::Rejected
                                )
                        }
                        State::Uni | State::Unknown => false,
                    }
            })
    }

    pub(in crate::conn) fn deliver_data(&mut self, id: Id) {
        let Some(entry) = self.entries.get_mut(id.number(), |entry| entry.id == id) else {
            return;
        };
        let barrier = match &mut entry.state {
            State::Request(state) => &mut state.data_barrier,
            State::Push(state) => &mut state.message.data_barrier,
            State::Uni | State::Unknown => return,
        };
        if *barrier == Barrier::Queued {
            *barrier = Barrier::Held;
        }
    }

    pub(super) fn reject(&mut self, id: Id) -> Result<event::Token, conn::Error> {
        let entry = self
            .entries
            .get_mut(id.number(), |entry| entry.id == id)
            .ok_or(conn::Error::StreamMissing {
                stream_id: id.get(),
            })?;
        let state = match &mut entry.state {
            State::Request(state) => state,
            State::Push(state) => &mut state.message,
            State::Uni | State::Unknown => {
                return Err(conn::Error::RequestStream {
                    stream_id: id.get(),
                });
            }
        };
        state.data_barrier = Barrier::Rejected;
        Ok(entry.recv.token)
    }

    pub(in crate::conn) fn rejected(&self, id: Id) -> bool {
        self.entries
            .get(id.number(), |entry| entry.id == id)
            .is_some_and(|entry| match &entry.state {
                State::Request(state) => {
                    matches!(state.data_barrier, Barrier::Rejected | Barrier::Discarding)
                }
                State::Push(state) => matches!(
                    state.message.data_barrier,
                    Barrier::Rejected | Barrier::Discarding
                ),
                State::Uni | State::Unknown => false,
            })
    }

    pub(in crate::conn) fn data_held(&self, id: Id) -> bool {
        self.entries
            .get(id.number(), |entry| entry.id == id)
            .is_some_and(|entry| match &entry.state {
                State::Request(state) => state.data_barrier == Barrier::Held,
                State::Push(state) => state.message.data_barrier == Barrier::Held,
                State::Uni | State::Unknown => false,
            })
    }

    pub(in crate::conn) fn release_data(&mut self, id: Id) -> Release {
        let Some(entry) = self.entries.get_mut(id.number(), |entry| entry.id == id) else {
            return Release::Missing;
        };
        let held = match &mut entry.state {
            State::Request(state) => &mut state.data_barrier,
            State::Push(state) => &mut state.message.data_barrier,
            State::Uni | State::Unknown => return Release::NotHeld,
        };
        if *held != Barrier::Held {
            return Release::NotHeld;
        }
        *held = Barrier::Open;
        Release::Released
    }

    pub(in crate::conn) fn clear(
        &mut self,
        id: Id,
        budget: &mut conn::ByteBudget,
        waiters: &mut wait::Table,
        retain: bool,
    ) -> Result<Option<(event::Token, usize)>, conn::Error> {
        let Some(flat::Entry::Occupied(mut slot)) =
            self.entries.entry(id.number(), |entry| entry.id == id)
        else {
            return Ok(None);
        };
        let entry = slot.get_mut();
        let recv = &mut entry.recv;
        waiters.remove(&mut recv.wait).map_err(conn::Error::Wait)?;
        let result = (recv.token, recv.clear(budget)?);
        if retain && matches!(recv.boundary, Boundary::More) {
            let state = match &mut entry.state {
                State::Request(state) => state,
                State::Push(state) => &mut state.message,
                State::Uni | State::Unknown => {
                    return Err(conn::Error::RequestStream {
                        stream_id: id.get(),
                    });
                }
            };
            state.data_barrier = Barrier::Discarding;
        } else {
            slot.remove();
        }
        Ok(Some(result))
    }

    pub(super) fn close(&mut self, id: Id) -> Result<(), conn::Error> {
        if self.critical.contains(id) {
            return Err(conn::Error::ClosedCriticalStream(id.get()));
        }
        let entry = self
            .entries
            .remove(id.number(), |entry| entry.id == id)
            .ok_or(conn::Error::StreamMissing {
                stream_id: id.get(),
            })?;
        let buffered = crate::Source::len(&entry.recv.buf);
        let credit = entry.recv.credit();
        if buffered != 0 || credit == conn::CreditMode::Held {
            return Err(conn::Error::StreamResidual {
                stream_id: id.get(),
                buffered,
                held: credit,
            });
        }
        Ok(())
    }

    pub(in crate::conn) fn is_critical(&self, id: Id) -> bool {
        self.critical.contains(id)
    }

    pub(in crate::conn) fn is_encoder(&self, id: Id) -> bool {
        self.critical.qpack_encoder.contains(id)
    }
}

#[derive(Default)]
struct CriticalSet<'d> {
    control: Critical<'d>,
    qpack_encoder: Critical<'d>,
    qpack_decoder: Critical<'d>,
}

impl<'d> CriticalSet<'d> {
    fn len(&self) -> usize {
        usize::from(self.control.slot.is_some())
            + usize::from(self.qpack_encoder.slot.is_some())
            + usize::from(self.qpack_decoder.slot.is_some())
    }
    fn contains(&self, id: Id) -> bool {
        self.control.contains(id)
            || self.qpack_encoder.contains(id)
            || self.qpack_decoder.contains(id)
    }

    fn recv(&self, id: Id) -> Option<&Recv<'d>> {
        if let Some(recv) = self.control.recv(id) {
            return Some(recv);
        }
        if let Some(recv) = self.qpack_encoder.recv(id) {
            return Some(recv);
        }
        self.qpack_decoder.recv(id)
    }

    fn recv_mut(&mut self, id: Id) -> Option<&mut Recv<'d>> {
        if let Some(recv) = self.control.recv_mut(id) {
            return Some(recv);
        }
        if let Some(recv) = self.qpack_encoder.recv_mut(id) {
            return Some(recv);
        }
        self.qpack_decoder.recv_mut(id)
    }

    fn get_mut(&mut self, id: Id) -> Option<Uni<'_, 'd>> {
        if let Some(recv) = self.control.recv_mut(id) {
            return Some(Uni::Control(recv));
        }
        if let Some(recv) = self.qpack_encoder.recv_mut(id) {
            return Some(Uni::QpackEncoder(recv));
        }
        if let Some(recv) = self.qpack_decoder.recv_mut(id) {
            return Some(Uni::QpackDecoder(recv));
        }
        None
    }
}

#[derive(Default)]
struct Critical<'d> {
    slot: Option<Slot<'d>>,
}

impl<'d> Critical<'d> {
    fn vacant(&mut self, kind: stream::UniType, id: Id) -> Result<Vacant<'_, 'd>, conn::Error> {
        match &mut self.slot {
            slot @ None => Ok(Vacant { slot }),
            Some(slot) => Err(conn::Error::CriticalExists {
                kind,
                existing: slot.id.get(),
                attempted: id.get(),
            }),
        }
    }

    fn contains(&self, id: Id) -> bool {
        self.slot.as_ref().is_some_and(|slot| slot.id == id)
    }

    fn get_mut(&mut self, id: Id) -> Option<&mut Slot<'d>> {
        self.slot.as_mut().filter(|slot| slot.id == id)
    }

    fn recv(&self, id: Id) -> Option<&Recv<'d>> {
        self.slot
            .as_ref()
            .filter(|slot| slot.id == id)
            .map(|slot| &slot.recv)
    }

    fn recv_mut(&mut self, id: Id) -> Option<&mut Recv<'d>> {
        self.get_mut(id).map(|slot| &mut slot.recv)
    }
}

struct Slot<'d> {
    id: Id,
    recv: Recv<'d>,
}

struct Vacant<'a, 'd> {
    slot: &'a mut Option<Slot<'d>>,
}

impl<'d> Vacant<'_, 'd> {
    fn insert(self, id: Id, recv: Recv<'d>) {
        *self.slot = Some(Slot { id, recv });
    }
}

const _: () =
    assert!(core::mem::size_of::<Slot<'static>>() == core::mem::size_of::<(Id, Recv<'static>)>());
