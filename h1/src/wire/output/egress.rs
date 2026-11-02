use std::mem;

use o3::buffer::storage;
use sark_core::route::guard;
use sark_http::{
    h1::head::request::Coding,
    owner::{evidence, handoff, handoff::stage},
    payload::{compress::Compression, response::dispatch},
    request::Id,
};
use sink::{Mode, Sink, Stream, written::Written};

use crate::{
    route,
    wire::{Failure, InputError, cache, head, output, output::sink, response},
};

/// Writes response decisions into the caller-provided egress region.
pub struct Egress<'a, I = ()> {
    write: &'a mut [u8],
    date: &'a [u8; 29],
    input: I,
    response: response::Context,
}

impl<'a> Egress<'a> {
    pub fn new(write: &'a mut [u8], date: &'a [u8; 29], response: response::Context) -> Self {
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
            route::Cache::Store => match cache.try_write(self.write, self.date, self.response) {
                Ok(cached) => cached.map(Self::from_cache),
                Err(error) => Some(output::Outcome::Failed(Failure::Output(error.into()))),
            },
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
    ) -> output::Outcome<'d>
    where
        'd: 'req,
        S: dispatch::Complete<'req, 'd>,
    {
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(conn),
            Mode::<()>::Plain,
            handoff::Slot::ready(Some(id)),
        )
        .with_policy(policy);
        Self::outcome(reply.complete(sink), conn)
    }

    /// Writes and transfers one streaming response without widening its shape.
    pub(crate) fn stream<'req, 'd, S, A>(
        self,
        reply: S,
        policy: route::Policy,
        admit: output::Admit<A>,
    ) -> Result<Stream<(A, S::Stream)>, Failure>
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
            Written::Inline {
                written: used,
                body_bytes: bytes,
                cause,
            } => output::Outcome::SendCopied {
                written: used,
                body_bytes: bytes,
                cause,
                connection: conn,
            },
            Written::Compressed { head, body, read } => output::Outcome::SendCompressed {
                hdr_written: head,
                body,
                read,
                connection: conn,
            },
            Written::Body { head, body } => Self::body_outcome(head, body, conn),
            Written::Head { written, input } => Self::head_outcome(written, input.into(), conn),
            Written::RequestBody { head, body } => {
                output::Outcome::SendRequestBody(output::Pending::new(head, body, conn))
            }
        }
    }

    fn body_outcome<'d, P>(
        head: usize,
        ready: handoff::Ready<'d>,
        conn: head::Connection,
    ) -> output::Outcome<'d, P> {
        match ready {
            handoff::Ready::Empty => output::Outcome::Send {
                written: head,
                connection: conn,
            },
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
                    hdr_written: head,
                    body,
                    connection: conn,
                }
            }
            handoff::Ready::Copied(body) => {
                let (body, copy) = body.into_parts();
                output::Outcome::SendOwnedCopy {
                    hdr_written: head,
                    body,
                    cause: copy.cause,
                    connection: conn,
                }
            }
            handoff::Ready::Retained(body) => output::Outcome::SendShared {
                hdr_written: head,
                body,
                connection: conn,
            },
            handoff::Ready::Segmented(body) => output::Outcome::SendSegmented {
                hdr_written: head,
                body,
                connection: conn,
            },
            handoff::Ready::Linked(body) => output::Outcome::SendLinked {
                hdr_written: head,
                body,
                connection: conn,
            },
            handoff::Ready::Fixed(body) => output::Outcome::SendStatic {
                hdr_written: head,
                body,
                connection: conn,
            },
        }
    }

    fn head_outcome<'d, P>(
        written: usize,
        input: crate::Input<'_>,
        conn: head::Connection,
    ) -> output::Outcome<'d, P> {
        match input {
            crate::Input::Borrowed(bytes) => match storage::Shared::try_copy(bytes) {
                Ok(body) => output::Outcome::SendOwnedCopy {
                    hdr_written: written,
                    body,
                    cause: evidence::Cause::Borrowed,
                    connection: conn,
                },
                Err(error) => output::Outcome::Failed(Failure::Input(InputError::Storage(error))),
            },
            crate::Input::Buffered(view) => match view.try_retain() {
                Ok(body) => output::Outcome::SendShared {
                    hdr_written: written,
                    body,
                    connection: conn,
                },
                Err(error) => output::Outcome::Failed(Failure::Input(InputError::Share(error))),
            },
        }
    }

    fn from_cache<'d>(cached: cache::Cached) -> output::Outcome<'d> {
        match cached {
            cache::Cached::Fixed { len, body } => output::Outcome::SendCopied {
                written: len,
                body_bytes: body,
                cause: evidence::Cause::Cached,
                connection: head::Connection::KeepAlive,
            },
            cache::Cached::Shared { head, body } => output::Outcome::SendShared {
                hdr_written: head,
                body,
                connection: head::Connection::KeepAlive,
            },
        }
    }
}

impl<'a, 'req> Egress<'a, crate::Input<'req>> {
    pub fn bind(
        write: &'a mut [u8],
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

    pub fn into_parts(self) -> (&'a mut [u8], &'a [u8; 29]) {
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
    assert!(mem::size_of::<Egress<'_>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Egress<'_, crate::Input<'_>>>() <= 7 * mem::size_of::<usize>());
};
