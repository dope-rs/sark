use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::{
    buffer::resident,
    depot,
    depot::{map::direct, queue::indexed::keyed},
};
use sark_core::{
    body::{self, admit::Retention, credit, input, stream},
    decode::{request, state},
    duplex,
    life::exchange::Id,
    manifold::stream::{Boundary, Payload as _},
};
use sark_http::{field::metadata::section::rules, payload::owned};

use crate::{
    conn::{Error, event},
    server::{
        self, payload,
        state::{exchange, exchange::Deferred as _},
        task::{
            book,
            ingress::{
                self, Build, Receiving, Routes, Streams, Variant,
                entry::{self, Entry},
                errors,
                variant::BufferedSlot,
            },
        },
    },
    stream::Key,
};

type Output<T, E> = Result<Progress<T>, E>;

pub(in crate::server::task) struct Data<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    pub(super) entries: &'a mut direct::Table<Key, Entry<'d, P, F, B>>,
    pub(super) ready: &'a mut keyed::Fifo<Key, entry::Build>,
    pub(super) admission: &'a mut keyed::Fifo<Key, entry::Admission>,
    pub(super) credit: &'a mut credit::raw::Account,
}

pub(in crate::server::task) enum Discard<'d, ID: Identity> {
    Missing(Id<conn::Id<'d, ID>, Key>),
    Removed(Id<conn::Id<'d, ID>, Key>),
    Failed {
        exchange: Id<conn::Id<'d, ID>, Key>,
        error: payload::Error,
    },
}

pub(in crate::server::task) enum Route {
    Missing,
    Finished,
}

#[derive(Clone, Copy)]
pub(in crate::server::task) enum Schedule {
    Idle,
    Ready,
}

impl<'a, 'd, P, F, B> Data<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    pub(in crate::server::task) fn from_book<G, X, D>(
        book: &'a mut book::Book<'d, P, F, G, B, X, D>,
    ) -> Self
    where
        G: rules::Policy,
        D: duplex::Session<'d>,
    {
        Self {
            entries: &mut book.incoming,
            ready: &mut book.build_ready,
            admission: &mut book.admission_ready,
            credit: &mut book.body_credit,
        }
    }

    pub(in crate::server::task) fn try_route<E: credit::Evidence>(
        &mut self,
        stream: Key,
        request: request::Guarded<F::State<state::Routed>>,
        evidence: E,
    ) -> Result<(), Error> {
        let state = exchange::Routed::new(request);
        let vacant = match self.entries.try_vacant(stream) {
            Ok(vacant) => vacant,
            Err(error) => {
                let release = self.credit.scope(|mut ledger| {
                    // SAFETY: the deferred route was created for this Book's
                    // admission and remains in its custody on failure.
                    unsafe { exchange::raw::Routed::new(state, &mut ledger) }.discard(evidence)
                });
                return Err(errors::reject(error, release));
            }
        };
        Routes::insert(vacant, self.admission, state);
        Ok(())
    }

    pub(in crate::server::task) fn take_route(
        &mut self,
    ) -> Option<(Key, exchange::Routed<'d, P, F>)> {
        let live = keyed::Fifo::first(self)?;
        // SAFETY: both loans come from the original admission root retained by the endpoint.
        let live = unsafe {
            live.map(|(data, index)| ((&mut *data.entries, &mut *data.admission), index))
        };
        let (occupied, queue) = direct::Occupied::from_live(live);
        let key = occupied.key();
        let (vacant, state, link) =
            Variant::<exchange::Routed<'d, P, F>, entry::Admission>::take(occupied);
        Routes::remove(vacant, &link, queue);
        Some((key, state))
    }

    pub(in crate::server::task) fn finish_route(
        &mut self,
        stream: Key,
    ) -> Result<Route, payload::Error> {
        let occupied = match self.entries.entry(stream) {
            Some(occupied) => occupied,
            None => return Ok(Route::Missing),
        };
        let (vacant, entry) = occupied.take();
        let (entry, result) = match entry {
            Entry::Routed { state, link } => {
                let (state, result) = match state.try_finish() {
                    Ok(state) => (state, Ok(Route::Finished)),
                    Err((error, state)) => (state, Err(error)),
                };
                (Entry::Routed { state, link }, result)
            }
            entry => (entry, Ok(Route::Missing)),
        };
        vacant.insert(entry);
        result
    }

    pub(in crate::server::task) fn build(self) -> Build<'a, 'd, P, F, B> {
        Build {
            entries: self.entries,
            ready: self.ready,
            credit: self.credit,
        }
    }

    pub(in crate::server::task) fn discard<ID: Identity, E: credit::Evidence>(
        &mut self,
        exchange: Id<conn::Id<'d, ID>, Key>,
        evidence: E,
    ) -> Discard<'d, ID> {
        let stream = exchange.stream();
        let Some(entry) = self.remove_any(stream) else {
            return Discard::Missing(exchange);
        };
        let result = self.credit.scope(|mut ledger| match entry {
            Entry::Routed { state, .. } => {
                // SAFETY: this route was removed from this exact Book.
                unsafe { exchange::raw::Routed::new(state, &mut ledger) }.discard(evidence)
            }
            Entry::Buffered(buffered) => {
                // SAFETY: this request was removed from this exact Book.
                unsafe { exchange::raw::Closing::new(buffered.request, &mut ledger) }
                    .discard::<Receiving<'d>, _>(evidence)
            }
            Entry::Streaming { body, .. } => {
                // SAFETY: this builder was removed from this exact Book.
                let body = unsafe { ledger.bind(body) };
                payload::discard(body.into_release(), &mut ledger, evidence)
            }
        });
        match result {
            Ok(()) => Discard::Removed(exchange),
            Err(error) => Discard::Failed { exchange, error },
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Schedule>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Schedule>() == core::mem::align_of::<bool>());
};

impl<'a, 'd, P, F, B> Data<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    pub(in crate::server::task) fn push<E: credit::Evidence>(
        self,
        stream: Key,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
        limits: server::Config,
        evidence: E,
    ) -> Output<BufferedSlot<'a, 'd, P, F, B>, errors::Push<B::Error>>
    where
        B: body::Push<'d>,
    {
        let occupied = match self.entries.entry(stream) {
            Some(occupied) => occupied,
            None => {
                data.discard();
                return Ok(Progress::Idle);
            }
        };
        let (vacant, entry) = occupied.take();
        match entry {
            Entry::Routed { mut state, link } => {
                let pushed = self.credit.scope(|mut ledger| {
                    // SAFETY: the route and account were selected from the
                    // same exclusively borrowed Book by this Data owner.
                    unsafe { exchange::raw::Route::new(&mut state, &mut ledger) }
                        .try_push(evidence, data, limits)
                });

                let (state, result) = match Self::settle(pushed) {
                    Ok(()) if matches!(boundary, Boundary::End) => match state.try_finish() {
                        Ok(state) => (state, Ok(Progress::Idle)),
                        Err((error, state)) => (state, Err(errors::Push::Payload(error))),
                    },
                    Ok(()) => (state, Ok(Progress::Idle)),
                    Err(error) => (state, Err(errors::Push::Payload(error))),
                };
                vacant.insert(Entry::Routed { state, link });
                result
            }
            Entry::Buffered(mut buffered) => {
                let pushed = self.credit.scope(|mut ledger| {
                    // SAFETY: the buffered request and account were selected
                    // from the same exclusively borrowed Book.
                    unsafe { ingress::raw::Buffering::new(&mut buffered, &mut ledger) }
                        .try_push(evidence, data, limits)
                });
                match pushed {
                    payload::Push::Accepted if matches!(boundary, Boundary::End) => {
                        Ok(Progress::Request(BufferedSlot::insert(vacant, buffered)))
                    }
                    pushed => {
                        vacant.insert(Entry::Buffered(buffered));
                        Self::settle(pushed)
                            .map(|()| Progress::Idle)
                            .map_err(errors::Push::Payload)
                    }
                }
            }
            Entry::Streaming { body, link } => {
                let schedule =
                    body.retention() == Retention::Retained || matches!(boundary, Boundary::End);
                let request = limits.request(body.max_body());
                self.credit.scope(|mut ledger| {
                    // SAFETY: this builder was selected from the incoming table
                    // permanently paired with this exact Book account.
                    let body = unsafe { ledger.bind(body) };
                    let len = data.len();
                    let chunks = data.count();
                    let pushed = body.try_push(
                        &mut ledger,
                        evidence,
                        input::Input::new(data, len, chunks),
                        boundary,
                        (request, limits.connection()),
                        |data| {
                            let (segments, _) = data.into_parts();
                            owned::Segments::from(segments)
                        },
                    );
                    let (body, result) = match pushed {
                        stream::Push::Accepted(body) => (body, Ok(())),
                        stream::Push::Discarded { build, input } => {
                            input.discard();
                            (build, Ok(()))
                        }
                        stream::Push::Refused {
                            build,
                            input,
                            error,
                        } => {
                            input.discard();
                            (build, Err(errors::Push::Input(error)))
                        }
                        stream::Push::Failed { build, error } => {
                            (build, Err(errors::Push::Input(error)))
                        }
                        stream::Push::Rejected { build, error } => {
                            (build, Err(errors::Push::Build(error)))
                        }
                    };
                    if result.is_ok()
                        && schedule
                        && let Err(vacant) = Streams::new(&vacant, &link, self.ready)
                    {
                        vacant.push::<false>();
                    }
                    // SAFETY: the successor returns to its original Book slot.
                    let body = unsafe { ledger.store(body) };
                    vacant.insert(Entry::Streaming { body, link });
                    result?;
                    Ok(Progress::Streaming)
                })
            }
        }
    }

    pub(in crate::server::task) fn finish<E: credit::Evidence>(
        self,
        stream: Key,
        evidence: E,
    ) -> Output<<P::Family as state::Family<P>>::State<Receiving<'d>>, payload::Error> {
        let occupied = match self.entries.entry(stream) {
            Some(occupied) => occupied,
            None => return Ok(Progress::Idle),
        };
        let (vacant, entry) = occupied.take();
        match entry {
            Entry::Buffered(buffered) => Ok(Progress::Request(buffered.request)),
            Entry::Streaming { body, link } => self.credit.scope(|mut ledger| {
                // SAFETY: selection retained this Book's exact builder and account.
                let body = unsafe { ledger.bind(body) };
                match body.try_finish(&mut ledger, evidence) {
                    body::Transfer::Accepted(body) => {
                        if let Err(vacant) = Streams::new(&vacant, &link, self.ready) {
                            vacant.push::<false>();
                        }
                        // SAFETY: the successor returns to the selected Book slot.
                        let body = unsafe { ledger.store(body) };
                        vacant.insert(Entry::Streaming { body, link });
                        Ok(Progress::Streaming)
                    }
                    body::Transfer::Rejected { build: body, error } => {
                        // SAFETY: rejection retains this same Book's original builder.
                        let body = unsafe { ledger.store(body) };
                        vacant.insert(Entry::Streaming { body, link });
                        Err(payload::Error::Body(error))
                    }
                }
            }),
            other => {
                vacant.insert(other);
                Ok(Progress::Idle)
            }
        }
    }
}

impl<'a, 'd, P, F, B> Data<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    fn settle(
        pushed: payload::Push<event::DataPayload<resident::Shared<'d>>>,
    ) -> Result<(), payload::Error> {
        match pushed {
            payload::Push::Accepted => Ok(()),
            payload::Push::Rejected { input, error } => {
                input.discard();
                Err(error)
            }
        }
    }

    fn remove_any(&mut self, stream: Key) -> Option<Entry<'d, P, F, B>> {
        let occupied = self.entries.entry(stream)?;
        let (vacant, entry) = occupied.take();
        match &entry {
            Entry::Routed { link, .. } => Routes::remove(vacant, link, self.admission),
            Entry::Streaming { link, .. } => {
                if let Ok(member) = Streams::new(&vacant, link, self.ready) {
                    member.remove();
                }
            }
            Entry::Buffered(_) => {}
        }
        Some(entry)
    }
}

pub(in crate::server::task) enum Progress<T> {
    Idle,
    Request(T),
    Streaming,
}

// SAFETY: Data permanently pairs the admission FIFO with its direct table.
unsafe impl<'d, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeySource<entry::Admission> for &mut Data<'_, 'd, P, F, B>
{
    type Key = Key;
    fn fifo(&self) -> &keyed::Fifo<Key, entry::Admission> {
        self.admission
    }
}
