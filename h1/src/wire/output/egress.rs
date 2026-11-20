use o3::buffer::write::Slice;
use std::{convert::Infallible, mem, ops::ControlFlow};

use o3::buffer::storage;
use sark_core::{manifold::task, route::guard};
use sark_http::{
    owner::{evidence, handoff, handoff::stage},
    payload::{
        compress::Compression,
        response::{dispatch, dispatch::Emit},
    },
};
use sink::{Mode, Sink, written::Written};

use crate::{
    route,
    wire::{Failure, InputError, head, output, output::sink, response},
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

    pub fn try_route<'req, 'd: 'req, R>(
        self,
        reply: R,
        gzip: &mut impl Compression,
        policy: route::Policy,
    ) -> Result<output::Complete<'d>, Failure>
    where
        R: dispatch::Emit<'req, 'd, Kind = dispatch::Sync>,
    {
        let ControlFlow::Continue(ready) =
            self.try_stage(reply, handoff::Slot::ready(None), gzip, policy)?;
        Ok(ready)
    }

    pub(crate) fn try_stage<'req, 'd: 'req, R, O: stage::Body<'req, 'd>>(
        self,
        reply: R,
        slot: handoff::Slot<'req, 'd, (), O>,
        gzip: &mut impl Compression,
        policy: route::Policy,
    ) -> Result<ControlFlow<O::Deferred, output::Complete<'d>>, Failure>
    where
        R: dispatch::Emit<'req, 'd, Kind = dispatch::Sync>,
    {
        let mode = Mode::new(policy.mode, self.response.coding(), gzip);
        let sink = Sink::new(self.write, self.date, self.response, mode, slot).with_policy(policy);
        Self::try_finish(reply.emit(sink).map_err(Failure::Output)?)
    }

    pub fn try_task<'d, R: sark_core::route::Task<'d, A> + route::PolicyOf, A>(
        self,
        output: task::response::Output<'d, R, A>,
        conn: head::Connection,
    ) -> Result<
        (
            output::Complete<'d>,
            Option<task::response::Stream<'d, R, A>>,
        ),
        Failure,
    > {
        let (id, response) = output.into_parts();
        let reply = match response {
            task::response::Outcome::Response(reply) => reply,
            task::response::Outcome::Rejected(error) => return Err(Failure::Route(error)),
        };
        let mut gzip = ();
        let mode = match R::POLICY.mode {
            route::Mode::RequiredGzip => {
                Mode::new(R::POLICY.mode, self.response.coding(), &mut gzip)
            }
            route::Mode::Plain | route::Mode::Gzip => Mode::Plain,
        };
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(conn),
            mode,
            handoff::Slot::ready(Some(id)),
        )
        .with_policy(R::POLICY);
        let written = reply.emit(sink).map_err(Failure::Output)?;
        <<R::Output as Emit<'d, 'd>>::Kind as dispatch::Dispatch>::fold::<
            Sink<'_, 'd, 'd, (), Infallible, ()>,
            task::response::Stream<'d, R, A>,
            _,
        >(
            written,
            |written| Ok((Self::try_ready(written)?, None)),
            |stream| Ok((output::Complete::Inline, stream)),
        )
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
        let mut gzip = ();
        let mode = match policy.mode {
            route::Mode::RequiredGzip => Mode::new(policy.mode, self.response.coding(), &mut gzip),
            route::Mode::Plain | route::Mode::Gzip => Mode::Plain,
        };
        let sink = Sink::new(
            self.write,
            self.date,
            self.response,
            mode,
            handoff::Slot::ready(None),
        )
        .with_policy(policy)
        .with_admit(admit);
        reply.emit(sink).map_err(Failure::Output)
    }

    fn try_ready<'input, 'd, H: Into<crate::Input<'input>>>(
        written: Written<'d, Infallible, H>,
    ) -> Result<output::Complete<'d>, Failure> {
        let ControlFlow::Continue(ready) = Self::try_finish(written)?;
        Ok(ready)
    }

    fn try_finish<'input, 'd, P, H: Into<crate::Input<'input>>>(
        written: Written<'d, P, H>,
    ) -> Result<ControlFlow<P, output::Complete<'d>>, Failure> {
        use output::Complete;
        let ready = match written {
            Written::Inline { copy } => Complete::Copy { copy },
            Written::Compressed { body, read } => Complete::Compressed { body, read },
            Written::RequestBody { body } => return Ok(ControlFlow::Break(body)),
            Written::Head { input } => match input.into() {
                crate::Input::Borrowed(bytes) => Complete::Owned {
                    body: storage::Shared::try_copy(bytes)
                        .map_err(|error| Failure::Input(InputError::Storage(error)))?,
                    cause: evidence::Cause::Borrowed,
                },
                crate::Input::Buffered(view) => Complete::Shared {
                    body: view
                        .try_retain()
                        .map_err(|error| Failure::Input(InputError::Share(error)))?,
                },
            },
            Written::Body { body } => match body {
                handoff::Ready::Empty => Complete::Inline,
                handoff::Ready::Owned(mut body) => Complete::Shared {
                    body: storage::Shared::try_from(&mut body)
                        .map_err(|error| Failure::Output(output::Error::Allocation(error)))?,
                },
                handoff::Ready::Copied(body) => {
                    let (body, copy) = body.into_parts();
                    Complete::Owned {
                        body,
                        cause: copy.cause,
                    }
                }
                handoff::Ready::Retained(body) => Complete::Shared { body },
                handoff::Ready::Segmented(body) => Complete::Segments { body },
                handoff::Ready::Linked(body) => Complete::Linked { body },
                handoff::Ready::Fixed(body) => Complete::Static { body },
            },
        };
        Ok(ControlFlow::Continue(ready))
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
}

impl<'input, 'req, 'd: 'req> guard::Reject<'req, 'd> for Egress<'_, crate::Input<'input>> {
    type Break = Result<output::Complete<'d>, Failure>;

    fn try_reject<R>(self, response: R) -> Self::Break
    where
        R: dispatch::Complete<'req, 'd>,
    {
        let slot = handoff::Slot::ready(None).with_head(self.input);
        let sink = Sink::new(
            self.write,
            self.date,
            self.response.with_connection(head::Connection::Close),
            Mode::<()>::Plain,
            slot,
        );
        Egress::try_ready(response.complete(sink).map_err(Failure::Output)?)
    }
}

const _: () = {
    assert!(mem::size_of::<Egress<'_>>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Egress<'_, crate::Input<'_>>>() <= 8 * mem::size_of::<usize>());
};
