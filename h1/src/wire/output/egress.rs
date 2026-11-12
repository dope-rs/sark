use o3::buffer::write::Slice;
use std::{convert::Infallible, mem};

use o3::buffer::storage;
use sark_core::route::guard;
use sark_http::{
    h1::head::request::Coding,
    owner::{evidence, handoff, handoff::stage},
    payload::{compress::Compression, response::dispatch},
    request::Id,
};
use sink::{Mode, Sink, written::Written};

use crate::{
    route,
    wire::{Failure, InputError, cache, head, output, output::sink, response},
};

/// Writes response decisions into the caller-provided egress region.
pub struct Egress<'a, I = ()> {
    write: Slice<'a, &'a mut usize>,
    date: &'a [u8; 29],
    input: I,
    response: response::Context,
}

impl<'a> Egress<'a> {
    pub fn new(
        write: Slice<'a, &'a mut usize>,
        date: &'a [u8; 29],
        response: response::Context,
    ) -> Self {
        Self {
            write,
            date,
            input: (),
            response,
        }
    }

    pub(crate) fn with_context(mut self, response: response::Context) -> Self {
        self.response = response;
        self
    }

    pub fn cached<'d>(
        &mut self,
        cache: &mut cache::Cache<'_>,
        policy: route::Cache,
    ) -> Option<output::Outcome<'d>> {
        match policy {
            route::Cache::Bypass => None,
            route::Cache::Store => {
                match cache.try_write(self.write.reborrow(), self.date, self.response) {
                    Ok(cached) => cached.map(Self::from_cache),
                    Err(error) => Some(output::Outcome::Failed(Failure::Output(error.into()))),
                }
            }
        }
    }

    pub fn route<'req, 'd, R, O: stage::Body<'req, 'd>>(
        self,
        reply: R,
        slot: handoff::Slot<'req, 'd, (), O>,
        cache: cache::Cache<'_>,
        gzip: &mut impl Compression,
        coding: Coding,
        policy: route::Policy,
    ) -> output::Outcome<'d, O::Deferred>
    where
        'd: 'req,
        R: dispatch::Emit<'req, 'd, Kind = dispatch::Sync>,
    {
        let mode = match policy.cache {
            route::Cache::Store => Mode::Cache(cache),
            route::Cache::Bypass => match coding {
                Coding::Plain => Mode::Plain,
                Coding::Gzip => Mode::Compress(gzip),
            },
        };
        let sink = Sink::new(self.write, self.date, self.response, mode, slot).with_policy(policy);
        Self::outcome(reply.emit(sink), head::Connection::KeepAlive)
    }

    pub fn plain<'req, 'd, S>(self, reply: S, conn: head::Connection) -> output::Outcome<'d>
    where
        'd: 'req,
        S: dispatch::Emit<'req, 'd, Kind = dispatch::Sync>,
    {
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(conn),
            Mode::<()>::Plain,
            handoff::Slot::ready(None),
        );
        Self::outcome(reply.emit(sink), conn)
    }

    pub fn plain_task<'req, 'd, S>(
        self,
        reply: S,
        id: Id<'d>,
        conn: head::Connection,
        policy: route::Policy,
    ) -> (output::Outcome<'d>, Option<S::Stream>)
    where
        'd: 'req,
        S: dispatch::Emit<'req, 'd>,
    {
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(conn),
            Mode::<()>::Plain,
            handoff::Slot::ready(Some(id)),
        )
        .with_policy(policy);
        match reply.emit(sink) {
            Ok(written) => <S::Kind as dispatch::Dispatch>::fold::<
                Sink<'_, 'req, 'd, (), Infallible, ()>,
                S::Stream,
                _,
            >(
                written,
                |written| (Self::outcome(Ok(written), conn), None),
                |stream| (output::Outcome::Send { connection: conn }, stream),
            ),
            Err(error) => (output::Outcome::Failed(Failure::Output(error)), None),
        }
    }

    /// Writes and transfers one streaming response without widening its shape.
    pub(crate) fn stream<'req, 'd, S, A>(
        self,
        reply: S,
        policy: route::Policy,
        admit: output::Admit<A>,
    ) -> Result<Option<(A, S::Stream)>, Failure>
    where
        'd: 'req,
        S: dispatch::Emit<'req, 'd, Kind = dispatch::Polled>,
    {
        let sink = Sink::new(
            self.write,
            self.date,
            self.response,
            Mode::<()>::Plain,
            handoff::Slot::ready(None),
        )
        .with_policy(policy)
        .with_admit(admit);
        reply.emit(sink).map_err(Failure::Output)
    }

    fn outcome<'input, 'd, P, H: Into<crate::Input<'input>>>(
        sent: Result<Written<'d, P, H>, output::Error>,
        conn: head::Connection,
    ) -> output::Outcome<'d, P> {
        let sent = match sent {
            Ok(sent) => sent,
            Err(error) => return output::Outcome::Failed(Failure::Output(error)),
        };
        match sent {
            Written::Inline { copy } => output::Outcome::SendCopied {
                copy,
                connection: conn,
            },
            Written::Compressed { body, read } => output::Outcome::SendCompressed {
                body,
                read,
                connection: conn,
            },
            Written::Body { body } => Self::body_outcome(body, conn),
            Written::Head { input } => Self::head_outcome(input.into(), conn),
            Written::RequestBody { body } => {
                output::Outcome::SendRequestBody(output::Pending::new(body, conn))
            }
        }
    }

    fn body_outcome<'d, P>(
        ready: handoff::Ready<'d>,
        conn: head::Connection,
    ) -> output::Outcome<'d, P> {
        match ready {
            handoff::Ready::Empty => output::Outcome::Send { connection: conn },
            handoff::Ready::Owned(mut body) => {
                let body = match storage::Shared::try_from(&mut body) {
                    Ok(body) => body,
                    Err(error) => {
                        return output::Outcome::Failed(Failure::Output(
                            output::Error::Allocation(error),
                        ));
                    }
                };
                output::Outcome::SendShared {
                    body,
                    connection: conn,
                }
            }
            handoff::Ready::Copied(body) => {
                let (body, copy) = body.into_parts();
                output::Outcome::SendOwnedCopy {
                    body,
                    cause: copy.cause,
                    connection: conn,
                }
            }
            handoff::Ready::Retained(body) => output::Outcome::SendShared {
                body,
                connection: conn,
            },
            handoff::Ready::Segmented(body) => output::Outcome::SendSegmented {
                body,
                connection: conn,
            },
            handoff::Ready::Linked(body) => output::Outcome::SendLinked {
                body,
                connection: conn,
            },
            handoff::Ready::Fixed(body) => output::Outcome::SendStatic {
                body,
                connection: conn,
            },
        }
    }

    fn head_outcome<'d, P>(
        input: crate::Input<'_>,
        conn: head::Connection,
    ) -> output::Outcome<'d, P> {
        match input {
            crate::Input::Borrowed(bytes) => match storage::Shared::try_copy(bytes) {
                Ok(body) => output::Outcome::SendOwnedCopy {
                    body,
                    cause: evidence::Cause::Borrowed,
                    connection: conn,
                },
                Err(error) => output::Outcome::Failed(Failure::Input(InputError::Storage(error))),
            },
            crate::Input::Buffered(view) => match view.try_retain() {
                Ok(body) => output::Outcome::SendShared {
                    body,
                    connection: conn,
                },
                Err(error) => output::Outcome::Failed(Failure::Input(InputError::Share(error))),
            },
        }
    }

    fn from_cache<'d>(cached: cache::Cached) -> output::Outcome<'d> {
        match cached {
            cache::Cached::Fixed { body } => output::Outcome::SendCopied {
                copy: evidence::Copy {
                    read: body,
                    written: body,
                    cause: evidence::Cause::Cached,
                },
                connection: head::Connection::KeepAlive,
            },
            cache::Cached::Shared { body } => output::Outcome::SendShared {
                body,
                connection: head::Connection::KeepAlive,
            },
        }
    }
}

impl<'a, 'req> Egress<'a, crate::Input<'req>> {
    pub fn bind(
        write: Slice<'a, &'a mut usize>,
        date: &'a [u8; 29],
        input: crate::Input<'req>,
        response: response::Context,
    ) -> Self {
        Self {
            write,
            date,
            input,
            response,
        }
    }

    pub fn into_parts(self) -> (Slice<'a, &'a mut usize>, &'a [u8; 29]) {
        (self.write, self.date)
    }
}

impl<'input, 'req, 'd: 'req> guard::Reject<'req, 'd> for Egress<'_, crate::Input<'input>> {
    type Break = output::Outcome<'d>;

    fn try_reject<R>(self, response: R) -> Self::Break
    where
        R: dispatch::Immediate<'req, 'd>,
    {
        let slot = handoff::Slot::ready(None).with_head(self.input);
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(head::Connection::Close),
            Mode::<()>::Plain,
            slot,
        );
        Egress::outcome(response.emit(sink), head::Connection::Close)
    }
}

const _: () = {
    assert!(mem::size_of::<Egress<'_>>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Egress<'_, crate::Input<'_>>>() <= 8 * mem::size_of::<usize>());
};
