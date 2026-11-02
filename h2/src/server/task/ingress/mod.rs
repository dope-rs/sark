use std::num::NonZero;

use o3::{
    depot,
    depot::{
        index::Initialized,
        map::direct,
        queue::{indexed::keyed, links},
    },
};
use sark_core::{
    body,
    body::{
        credit,
        stream::{Admission, Drive, Stream},
    },
    decode::{request, state},
};
use variant::{Borrowed, Projection, Variant};

use crate::{
    server::{
        state::exchange,
        task::{Built, ingress::entry::Entry},
    },
    stream,
};

type Occupied<'a, 'd, P, F, B, Q = ()> = direct::Occupied<'a, stream::Key, Entry<'d, P, F, B>, Q>;
type Vacancy<'a, 'd, P, F, B, Q = ()> = direct::Vacant<'a, stream::Key, Entry<'d, P, F, B>, Q>;
type Removed<S, T, Q> = (S, T, keyed::Link<stream::Key, Q>);

mod data;
pub(in crate::server::task) mod entry;
pub(super) mod errors;
pub(super) mod raw;
mod states;
mod variant;

pub(super) use data::{Data, Discard, Progress, Route, Schedule};
pub(in crate::server::task) use states::{Buffered, Receiving};

pub(super) struct Routes<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>> {
    entries: &'a direct::Table<stream::Key, Entry<'d, P, F, B>>,
    selected: keyed::Entry<'a, stream::Key, entry::Admission>,
    queue: &'a mut keyed::Fifo<stream::Key, entry::Admission>,
}

pub(super) struct Streams<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>> {
    entries: &'a direct::Table<stream::Key, Entry<'d, P, F, B>>,
    selected: keyed::Entry<'a, stream::Key, entry::Build>,
    queue: &'a mut keyed::Fifo<stream::Key, entry::Build>,
}

impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>> Routes<'_, 'd, P, F, B> {
    fn insert(
        vacant: Vacancy<'_, 'd, P, F, B>,
        queue: &mut keyed::Fifo<stream::Key, entry::Admission>,
        state: exchange::Routed<'d, P, F>,
    ) {
        use links::Index;
        let key = vacant.key();
        let link = keyed::Link::from(Index::from(key));
        let owner = Routes {
            entries: vacant.table(),
            selected: keyed::Entry::from_source(Borrowed {
                slot: &vacant,
                link: &link,
            }),
            queue,
        };
        // SAFETY: the new link is vacant; this exclusive admission root owns its neighbors.
        unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(owner)) }
            .push::<false>();
        vacant.insert(Entry::Routed { state, link });
    }

    fn remove<Q>(
        vacant: Vacancy<'_, 'd, P, F, B, Q>,
        link: &keyed::Link<stream::Key, entry::Admission>,
        queue: &mut keyed::Fifo<stream::Key, entry::Admission>,
    ) {
        let owner = Routes {
            entries: vacant.table(),
            selected: keyed::Entry::from_source(Borrowed {
                slot: &vacant,
                link,
            }),
            queue,
        };
        // SAFETY: every routed state is an admission member until this consuming removal.
        unsafe { keyed::Slot::from_source(depot::raw::Proof::new(owner)) }.remove();
    }
}

impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    Streams<'a, 'd, P, F, B>
{
    fn new(
        vacant: &'a Vacancy<'_, 'd, P, F, B>,
        link: &'a keyed::Link<stream::Key, entry::Build>,
        queue: &'a mut keyed::Fifo<stream::Key, entry::Build>,
    ) -> Result<keyed::Slot<Self, entry::Build>, keyed::Slot<Self, entry::Build, false>> {
        let owner = Self {
            entries: vacant.table(),
            selected: keyed::Entry::from_source(Borrowed { slot: vacant, link }),
            queue,
        };
        keyed::Slot::try_new(owner)
    }

    pub(in crate::server::task) fn insert(
        vacant: Vacancy<'a, 'd, P, F, B>,
        queue: &mut keyed::Fifo<stream::Key, entry::Build>,
        body: Stream<'d, B>,
        schedule: Schedule,
    ) -> Occupied<'a, 'd, P, F, B> {
        use links::Index;
        let key = vacant.key();
        let link = keyed::Link::from(Index::from(key));
        if let Schedule::Ready = schedule {
            let owner = Streams {
                entries: vacant.table(),
                selected: keyed::Entry::from_source(Borrowed {
                    slot: &vacant,
                    link: &link,
                }),
                queue,
            };
            // SAFETY: the new stream link is vacant in this original build root.
            unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(owner)) }
                .push::<false>();
        }
        vacant.occupy(Entry::Streaming { body, link })
    }
}

// SAFETY: Build permanently pairs the streaming FIFO with its direct table.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeySource<entry::Build> for &mut Build<'_, 'd, P, F, B>
{
    type Key = stream::Key;
    fn fifo(&self) -> &keyed::Fifo<stream::Key, entry::Build> {
        self.ready
    }
}

// SAFETY: the selected link and original table loan remain borrowed until this admission turn ends.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeyOwner<entry::Admission> for Routes<'_, 'd, P, F, B>
{
    type Key = stream::Key;
    type Fifo = keyed::Fifo<stream::Key, entry::Admission>;
    fn parts(&self) -> (stream::Key, &keyed::Link<stream::Key, entry::Admission>) {
        depot::raw::KeyBorrow::parts(self.selected)
    }
    fn fifo(&self) -> &keyed::Fifo<stream::Key, entry::Admission> {
        self.queue
    }
    fn entry<'a>(
        live: depot::raw::Proof<(&'a Self, links::Index), Initialized<entry::Admission>>,
    ) -> keyed::Entry<'a, stream::Key, entry::Admission> {
        let live = unsafe { live.map(|(owner, index)| (owner.entries, index)) };
        keyed::Entry::from_source(Projection(direct::Table::project(live)))
    }
}

// SAFETY: the selected streaming link and original table loan retain the exact build plane.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeyOwner<entry::Build> for Streams<'_, 'd, P, F, B>
{
    type Key = stream::Key;
    type Fifo = keyed::Fifo<stream::Key, entry::Build>;
    fn parts(&self) -> (stream::Key, &keyed::Link<stream::Key, entry::Build>) {
        depot::raw::KeyBorrow::parts(self.selected)
    }
    fn fifo(&self) -> &keyed::Fifo<stream::Key, entry::Build> {
        self.queue
    }
    fn entry<'a>(
        live: depot::raw::Proof<(&'a Self, links::Index), Initialized<entry::Build>>,
    ) -> keyed::Entry<'a, stream::Key, entry::Build> {
        let live = unsafe { live.map(|(owner, index)| (owner.entries, index)) };
        keyed::Entry::from_source(Projection(direct::Table::project(live)))
    }
}

pub(super) struct Turn<
    'a,
    'd,
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
    Q = (),
> {
    slot: Slot<'a, 'd, P, F, B, Q>,
    body: Stream<'d, B>,
}

struct Slot<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q = ()> {
    vacant: Vacancy<'a, 'd, P, F, B, Q>,
    link: keyed::Link<stream::Key, entry::Build>,
    queue: &'a mut keyed::Fifo<stream::Key, entry::Build>,
}

impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>> Turn<'a, 'd, P, F, B> {
    fn try_new(
        occupied: Occupied<'a, 'd, P, F, B>,
        queue: &'a mut keyed::Fifo<stream::Key, entry::Build>,
    ) -> Option<Self> {
        let (vacant, value) = occupied.take();
        let Entry::Streaming { body, link } = value else {
            vacant.insert(value);
            return None;
        };
        let slot = Slot {
            vacant,
            link,
            queue,
        };
        match keyed::Slot::try_new(slot) {
            Ok(member) => Some(Self {
                slot: member.release(),
                body,
            }),
            Err(vacant) => {
                let (slot, link) = vacant.release();
                slot.insert(Entry::Streaming { body, link });
                None
            }
        }
    }
}

impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q>
    Turn<'a, 'd, P, F, B, Q>
{
    fn drive<E: credit::Evidence>(
        self,
        credit: &mut credit::raw::Account,
        budget: NonZero<usize>,
        evidence: E,
    ) -> Built<'d, B> {
        let Self { slot, body } = self;
        let key = slot.vacant.key();
        credit.scope(|mut ledger| {
            // SAFETY: Turn retains the body selected from the incoming table that
            // its Build/Data root permanently pairs with this exact Book account.
            let body = unsafe { ledger.bind(body) };
            let (step, mut evidence) = body.try_drive(&mut ledger, evidence, budget);
            // SAFETY: the body transition retains the original slot, link, and FIFO membership.
            let member = unsafe { keyed::Slot::from_source(depot::raw::Proof::new(slot)) };
            match step {
                Drive::Pending(body) => {
                    // SAFETY: the pending builder returns to its original Book slot.
                    let body = unsafe { ledger.store(body) };
                    member.move_to_back().release().store(body);
                    body::BuildOutcome::Pending
                }
                Drive::NeedInput(body) => {
                    let (slot, link) = member.remove().release();
                    // SAFETY: the unscheduled builder returns to its original Book slot.
                    let body = unsafe { ledger.store(body) };
                    slot.insert(Entry::Streaming { body, link });
                    body::BuildOutcome::Pending
                }
                Drive::Ready(ready) => {
                    let (slot, _) = member.remove().release();
                    let key = slot.key();
                    match ready.try_admit(&mut ledger, evidence) {
                        Admission::Accepted(build) => body::BuildOutcome::Ready { key, build },
                        Admission::Rejected {
                            ready,
                            mut evidence,
                            error,
                        } => {
                            let release =
                                ready.discard(&mut ledger, &mut evidence).abandon(evidence);
                            body::BuildOutcome::Failed {
                                key,
                                error: errors::Build::rejected(error, release),
                            }
                        }
                    }
                }
                Drive::Failed {
                    stream: body,
                    error,
                } => {
                    // SAFETY: failure retains the original builder in its exact Book slot.
                    let body = unsafe { ledger.store(body) };
                    member.release().store(body);
                    body::BuildOutcome::Failed {
                        key,
                        error: errors::Build::Step(error),
                    }
                }
                Drive::Rejected {
                    stream: body,
                    error,
                } => {
                    let (slot, _) = member.remove().release();
                    let key = slot.key();
                    let release = body
                        .into_release()
                        .try_discard(&mut ledger, &mut evidence)
                        .abandon(evidence);
                    body::BuildOutcome::Failed {
                        key,
                        error: errors::Build::rejected(error, release),
                    }
                }
            }
        })
    }
}

impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q>
    Slot<'a, 'd, P, F, B, Q>
{
    fn store(self, body: Stream<'d, B>) {
        self.vacant.insert(Entry::Streaming {
            body,
            link: self.link,
        });
    }
}

// SAFETY: this capability retains the actual removed slot, link, and original FIFO loan.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q>
    depot::raw::KeyOwner<entry::Build> for Slot<'_, 'd, P, F, B, Q>
{
    type Key = stream::Key;
    type Fifo = keyed::Fifo<stream::Key, entry::Build>;
    fn parts(&self) -> (stream::Key, &keyed::Link<stream::Key, entry::Build>) {
        depot::raw::KeyBorrow::parts(Borrowed {
            slot: &self.vacant,
            link: &self.link,
        })
    }
    fn fifo(&self) -> &keyed::Fifo<stream::Key, entry::Build> {
        self.queue
    }
    fn entry<'a>(
        live: depot::raw::Proof<(&'a Self, links::Index), Initialized<entry::Build>>,
    ) -> keyed::Entry<'a, stream::Key, entry::Build> {
        let live = unsafe { live.map(|(owner, index)| (owner.vacant.table(), index)) };
        keyed::Entry::from_source(Projection(direct::Table::project(live)))
    }
}

// SAFETY: only a committed cursor restores the membership capability.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q>
    depot::raw::KeyFinish<true, entry::Build> for Slot<'_, 'd, P, F, B, Q>
{
    type Output = Self;
    unsafe fn finish(self) -> Self {
        self
    }
}

// SAFETY: detachment releases the actual slot and its now-vacant link.
unsafe impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>, Q>
    depot::raw::KeyFinish<false, entry::Build> for Slot<'a, 'd, P, F, B, Q>
{
    type Output = (
        Vacancy<'a, 'd, P, F, B, Q>,
        keyed::Link<stream::Key, entry::Build>,
    );
    unsafe fn finish(self) -> Self::Output {
        (self.vacant, self.link)
    }
}

pub(super) struct Build<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    entries: &'a mut direct::Table<stream::Key, Entry<'d, P, F, B>>,
    ready: &'a mut keyed::Fifo<stream::Key, entry::Build>,
    credit: &'a mut credit::raw::Account,
}

impl<'a, 'd, P, F, B> Build<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    pub(in crate::server::task) fn drive<E, Map>(
        &mut self,
        budget: NonZero<usize>,
        evidence: Map,
    ) -> Built<'d, B>
    where
        E: credit::Evidence,
        Map: FnOnce(stream::Key) -> E,
    {
        let Some(live) = keyed::Fifo::first(self) else {
            return body::BuildOutcome::Idle;
        };
        // SAFETY: the original build root retains all disjoint loans and its exact queue plane.
        let live = unsafe {
            live.map(|(build, index)| {
                (
                    (&mut *build.entries, (&mut *build.ready, &mut *build.credit)),
                    index,
                )
            })
        };
        let (occupied, (queue, credit)) = direct::Occupied::from_live(live);
        let key = occupied.key();
        let (vacant, body, link) = Variant::<Stream<'d, B>, entry::Build>::take(occupied);
        Turn {
            slot: Slot {
                vacant,
                link,
                queue,
            },
            body,
        }
        .drive(credit, budget, evidence(key))
    }

    pub(in crate::server::task) fn drive_for<E: credit::Evidence>(
        &mut self,
        stream: stream::Key,
        budget: NonZero<usize>,
        evidence: E,
    ) -> Built<'d, B> {
        let occupied = match self.entries.entry(stream) {
            Some(occupied) => occupied,
            None => return body::BuildOutcome::Idle,
        };
        // SAFETY: these are the exact entry, queue, and account retained together
        // by this Build owner from its original Book selection.
        unsafe { raw::drive_entry(occupied, self.ready, self.credit, budget, evidence) }
    }
}
