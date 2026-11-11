use dope::net::link::egress::{self, data};
use o3::buffer::resident::bound;

use crate::{
    pump,
    server::payload,
    wire::{output, response},
};

/// One task with exactly the authority needed by its HTTP/1 phase.
#[repr(u8)]
pub enum Running<T, E = T> {
    Future(response::Context, T),
    Head(response::Context, T),
    Body(T),
    Closing(E),
}

/// One deferred wire frame retaining its validated geometry and transport identity.
#[repr(transparent)]
pub struct Stream<'d, K, B: bound::Bound = bound::Unbounded> {
    body: egress::NonEmpty<data::Bound<payload::Stream<'d, K>, B>>,
}

impl<'d, K, B: bound::Bound> Stream<'d, K, B> {
    pub const fn new(body: egress::NonEmpty<data::Bound<payload::Stream<'d, K>, B>>) -> Self {
        Self { body }
    }

    pub fn owner(&self) -> output::Owner {
        output::Owner {
            len: self.body.geometry().wire().widen().get(),
            charge: data::raw::Payload::charge(self.body.as_inner()),
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        egress::NonEmpty<data::Bound<payload::Stream<'d, K>, B>>,
        payload::End,
    ) {
        let end = self.body.as_inner().as_inner().end();
        (self.body, end)
    }

    pub fn release(self) -> K {
        self.body.into_inner().into_inner().release()
    }
}

/// Per-connection executor overlay retained by the H1 session.
pub struct Runtime<'d, T, S = (), K = (), B: bound::Bound = bound::Unbounded, E = T> {
    running: Option<Running<T, E>>,
    stream: Option<Stream<'d, K, B>>,
    session: S,
}

impl<T, S: Default, K, B: bound::Bound, E> Default for Runtime<'_, T, S, K, B, E> {
    fn default() -> Self {
        Self {
            running: None,
            stream: None,
            session: S::default(),
        }
    }
}

impl<'d, T, S, K, B: bound::Bound, E> Runtime<'d, T, S, K, B, E> {
    pub fn take(&mut self) -> Option<Running<T, E>> {
        self.running.take()
    }

    pub fn insert(&mut self, run: Running<T, E>) {
        self.running = Some(run);
    }

    pub fn take_stream(&mut self) -> Option<Stream<'d, K, B>> {
        self.stream.take()
    }

    pub fn try_stage(&mut self, stream: Stream<'d, K, B>) -> Result<(), Stream<'d, K, B>> {
        if self.stream.is_some() {
            Err(stream)
        } else {
            self.stream = Some(stream);
            Ok(())
        }
    }

    pub const fn session(&self) -> &S {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut S {
        &mut self.session
    }

    #[doc(hidden)]
    pub const fn cleared(&self) -> Option<usize> {
        if self.running.is_none() {
            Some(0)
        } else {
            None
        }
    }

    #[doc(hidden)]
    pub const fn owners(&self) -> usize {
        if self.running.is_some() { 1 } else { 0 }
    }

    #[doc(hidden)]
    pub fn staged_bytes(&self) -> usize {
        self.stream
            .as_ref()
            .map_or(0, |stream| stream.owner().charge.resident_bytes)
    }
}

impl<T, S, K, B: bound::Bound, E> pump::Tasks for Runtime<'_, T, S, K, B, E> {
    fn activity(&self) -> pump::Activity {
        match &self.running {
            None => pump::Activity::Idle,
            Some(Running::Future(..) | Running::Head(..)) => pump::Activity::Task,
            Some(Running::Body(_) | Running::Closing(_)) => pump::Activity::Stream,
        }
    }
}

const _: () = {
    use core::mem;

    use sark_core::life::sequence::observed::Tag;

    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<payload::End>() == mem::size_of::<bool>());
    assert!(mem::align_of::<payload::End>() == mem::align_of::<bool>());
    assert!(mem::size_of::<Running<usize>>() <= 2 * word);
    assert!(
        mem::size_of::<Stream<'static, ()>>()
            == mem::size_of::<egress::NonEmpty<payload::Stream<'static, ()>>>()
    );
    assert!(mem::size_of::<Option<Stream<'static, ()>>>() == mem::size_of::<Stream<'static, ()>>());
    assert!(
        mem::size_of::<Runtime<'static, usize>>()
            == mem::size_of::<Option<Stream<'static, ()>>>()
                + mem::size_of::<Option<Running<usize>>>()
    );
    assert!(
        mem::size_of::<Runtime<'static, usize, ()>>() == mem::size_of::<Runtime<'static, usize>>()
    );
    assert!(
        mem::size_of::<Runtime<'static, usize, (), Tag>>()
            <= mem::size_of::<Runtime<'static, usize>>() + mem::size_of::<Tag>() + word
    );
};
