use std::{cell, marker, mem, ops, ptr};

use dope::core::driver::route::Identity;
use o3::{depot, depot::arena::linked::fifo::ends};

use crate::client::receive::policies::Notify;

mod raw;

use dope::{core::driver::schedule::ready, manifold::client::conn};
use o3::{
    alloc::{self, BoxSliceExt as _},
    buffer::resident,
};
use sark_h2::{client, stream};

use crate::{
    client::{
        call::{bidi, request, response, unary},
        receive, session,
    },
    protocol::section,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::client::manifold) enum Key {
    Unary(receive::UnaryKey),
    Stream(receive::StreamKey),
    Call(stream::Key),
}

impl Key {
    fn index(self, offset: usize, claims: usize) -> usize {
        match self {
            Self::Unary(key) => key.slot().index(),
            Self::Stream(key) => key.slot(offset).index(),
            Self::Call(key) => claims + depot::raw::Key::index(key),
        }
    }
}

pub(in crate::client::manifold) enum Call<'d> {
    Unary(unary::Handle<'d, stream::Key>),
    Request(request::Stream<'d, stream::Key>),
    Response(response::Stream<'d, stream::Key>),
    Bidi(bidi::Stream<'d, stream::Key>),
    Send(bidi::Send<'d, stream::Key>),
    /// Two split tokens retain their original FIFO position and both reset/refills.
    ResetPair(bidi::Send<'d, stream::Key>),
}

impl<'d> Call<'d> {
    fn keys(&self) -> (Key, stream::Key) {
        match self {
            Self::Unary(call) => (Key::Unary(call.completion), call.stream),
            Self::Request(stream) => (
                Key::Unary(stream.response.completion),
                stream.response.stream,
            ),
            Self::Response(stream) => (Key::Stream(stream.completion), stream.stream),
            Self::Bidi(stream) => (
                Key::Stream(stream.response.completion),
                stream.response.stream,
            ),
            Self::Send(send) | Self::ResetPair(send) => (Key::Call(send.stream), send.stream),
        }
    }

    /// The primary owner check proved this response claim was retired before
    /// its position was record again. Only its stream reset remains live.
    fn fallback(self) -> Self {
        match self {
            Self::Response(response) => Self::Send(bidi::Send {
                brand: response.brand,
                stream: response.stream,
            }),
            call => call,
        }
    }

    pub(in crate::client::manifold) fn cancel<'scope>(
        self,
        session: &mut session::Session<'d, client::Plan<section::Plan>, resident::Shared<'d>>,
        done: &mut Notify<'scope, '_, '_>,
    ) {
        match self {
            Self::Unary(call) => session.cancel_unary(call, done),
            Self::Request(stream) => session.cancel_unary(stream.response, done),
            Self::Response(stream) => session.cancel_response(stream, done),
            Self::Bidi(stream) => session.cancel_bidi(stream, done),
            Self::Send(send) => session.cancel_send(send, done),
            Self::ResetPair(send) => {
                let other = bidi::Send {
                    brand: send.brand,
                    stream: send.stream,
                };
                session.cancel_send(send, done);
                session.cancel_send(other, done);
            }
        }
    }
}

/// The original affine call, admitted together with its cancellation positions.
/// Transitions keep its native keys; no separate index or allocation is stored.
#[repr(transparent)]
pub(in crate::client::manifold) struct Registered<T>(T);

impl<T> Registered<T> {
    pub(in crate::client::manifold) fn map<U>(self, change: impl FnOnce(T) -> U) -> Registered<U> {
        Registered(change(self.0))
    }

    pub(in crate::client::manifold) fn try_map<U, E>(
        self,
        change: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<Registered<U>, E> {
        change(self.0).map(Registered)
    }
}

impl<T> ops::Deref for Registered<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::DerefMut for Registered<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<'d> Registered<bidi::Stream<'d, stream::Key>> {
    pub(in crate::client::manifold) fn split(
        self,
    ) -> (
        Registered<bidi::Send<'d, stream::Key>>,
        Registered<response::Stream<'d, stream::Key>>,
    ) {
        let (send, response) = self.0.split();
        (Registered(send), Registered(response))
    }
}

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Parked,
    Queued,
}

/// The native empty-front result, consumed before another queue operation.
pub(in crate::client::manifold) struct Empty<'a, 'd, ID: Identity>(&'a Queue<'d, ID>);

impl<'d, ID: Identity> Empty<'_, 'd, ID> {
    pub(in crate::client::manifold) fn register<T>(
        self,
        conn: conn::Id<'d, ID>,
        key: Key,
        stream: stream::Key,
        value: T,
    ) -> Registered<T> {
        use raw::Source as _;

        for key in [key, Key::Call(stream)] {
            // SAFETY: Access supplies this queue's final empty-front result and
            // a native call admitted by the State that sized its fixed region.
            let entry = unsafe { self.0.entry(key) };
            unsafe { entry.register(conn, key) };
        }
        Registered(value)
    }
}

pub(in crate::client::manifold) struct Queue<'d, ID: Identity> {
    entries: Box<[raw::Entry<'d, ID>]>,
    offset: usize,
    claims: usize,
    ends: ends::Cell<Option<ptr::NonNull<raw::Entry<'d, ID>>>>,
    ready: cell::Cell<Option<ready::Target<'d>>>,
}

impl<'d, ID: Identity> Queue<'d, ID> {
    pub(in crate::client::manifold) fn try_new(
        claims: usize,
        calls: usize,
        offset: usize,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            entries: Box::try_init(
                claims.checked_add(calls).ok_or(alloc::Error::Overflow)?,
                raw::Entry::new,
            )?,
            offset,
            claims,
            ends: ends::Cell::empty(),
            ready: cell::Cell::new(None),
        })
    }

    pub(in crate::client::manifold) fn ready(&self, ready: ready::Target<'d>) {
        self.ready.set(Some(ready));
    }

    /// Appends the exact initialized payload whose detached position is retained
    /// by the caller's native phase proof.
    fn enqueue(&self, entry: &raw::Entry<'d, ID>, call: Call<'d>) {
        // SAFETY: the affine primary token or the checked fallback Parked phase
        // owns this detached registered position. Initialization precedes publish.
        unsafe { entry.enqueue(call) };
        let index = ptr::NonNull::from(entry);
        let publish = |previous: Option<ptr::NonNull<raw::Entry<'d, ID>>>| {
            if let Some(previous) = previous {
                // SAFETY: this queue's unchanged tail retains an initialized
                // queued entry in the same fixed allocation.
                unsafe { previous.as_ref() }.next.set(Some(index));
            }
        };
        // SAFETY: this exact queued payload is detached, has no successor, and
        // publish changes only the existing tail's link without calling out.
        ends::Cell::push_back(unsafe { depot::raw::Proof::new((&self.ends, index, publish)) });
    }

    pub(in crate::client::manifold) fn push(
        &self,
        conn: conn::Id<'d, ID>,
        record: Registered<Call<'d>>,
    ) {
        use raw::Source as _;

        let call = record.0;
        let (primary, stream) = call.keys();
        // SAFETY: Registered preserves the native keys from this channel's
        // admitted State and no queue loan escapes before the final wake.
        let entry = unsafe { self.entry(primary) };
        if !matches!(primary, Key::Call(_))
            && entry.owner().map(|(owner, _)| owner) == Some((conn, primary))
        {
            self.enqueue(entry, call);
        } else {
            let key = Key::Call(stream);
            // SAFETY: the same native call retains this stream position in the
            // same immutable allocation, including after its claim is retired.
            let entry = unsafe { self.entry(key) };
            let Some((owner, phase)) = entry.owner() else {
                return;
            };
            if owner != (conn, key) {
                return;
            }
            match phase {
                Phase::Parked => self.enqueue(entry, call.fallback()),
                Phase::Queued => {
                    // SAFETY: actual owner/generation and Queued checks retain
                    // the other split token in this same stream position.
                    unsafe {
                        entry.pair(bidi::Send {
                            brand: marker::PhantomData,
                            stream,
                        })
                    };
                }
            }
        }
        if let Some(ready) = self.ready.get() {
            ready.wake();
        }
    }

    pub(in crate::client::manifold) fn pop(
        &self,
    ) -> Result<(conn::Id<'d, ID>, Call<'d>), Empty<'_, 'd, ID>> {
        let take = |front: ptr::NonNull<raw::Entry<'d, ID>>, ()| {
            // SAFETY: the native head was published only after owner and call
            // initialization; links retain the same stable Box allocation.
            let entry = unsafe { front.as_ref() };
            let value = unsafe { entry.take() };
            (entry.next.take(), value)
        };
        // SAFETY: the callback consumes this queue's initialized front and
        // preserves its exact successor; no application callback runs here.
        match ends::Cell::pop_front(unsafe { depot::raw::Proof::new((&self.ends, (), take)) }) {
            Some(call) => Ok(call),
            None => Err(Empty(self)),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Registered<Call<'static>>>() == mem::size_of::<Call<'static>>());
    assert!(mem::size_of::<Call<'static>>() <= 32);
    assert!(!mem::needs_drop::<Call<'static>>());
    assert!(mem::size_of::<raw::Entry<'static, dope::core::driver::route::KeyTag<0>>>() <= 80);
};

// SAFETY: native registrations and affine Registered transitions retain keys
// from this channel's State. Shared sizes this fixed Box from that same State's
// immutable limits; no callback runs during a queue mutation.
unsafe impl<'d, ID: Identity> raw::Source<'d, ID> for Queue<'d, ID> {
    unsafe fn entry(&self, key: Key) -> &raw::Entry<'d, ID> {
        unsafe {
            self.entries
                .get_unchecked(key.index(self.offset, self.claims))
        }
    }
}
