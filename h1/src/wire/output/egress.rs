use o3::buffer::write::Slice;
use std::{convert::Infallible, mem, ops::ControlFlow};

use sark_core::{manifold::task, route::guard};
use sark_http::{
    owner::{handoff, handoff::stage},
    payload::{
        compress::Compression,
        response::{dispatch, dispatch::Emit},
    },
};
use sink::{Mode, Sink};

use crate::{
    route,
    wire::{Failure, head, output, output::sink, response},
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
        let sink = Sink::new(
            self.write,
            self.date,
            self.response,
            policy,
            mode,
            slot,
            None::<Infallible>,
        );
        reply.emit(sink)
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
            R::POLICY,
            mode,
            handoff::Slot::ready(Some(id)),
            Some(()),
        );
        let written = reply.emit(sink)?;
        <<R::Output as Emit<'d, 'd>>::Kind as dispatch::Dispatch>::fold::<
            Sink<'_, 'd, 'd, (), Infallible, (), ()>,
            task::response::Stream<'d, R, A>,
            _,
        >(
            written,
            |ControlFlow::Continue(ready)| Ok((ready, None)),
            |stream| Ok((output::Complete::Inline, stream.map(|((), stream)| stream))),
        )
    }

    /// Writes and transfers one streaming response without widening its shape.
    pub(crate) fn stream<'req, 'd, S, A>(
        self,
        reply: S,
        policy: route::Policy,
        admit: Option<A>,
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
            policy,
            mode,
            handoff::Slot::ready(None),
            admit,
        );
        reply.emit(sink)
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
            route::Policy::new(
                route::Mode::Plain,
                sark_http::h1::head::response::Date::Include,
                sark_http::h1::head::response::Server::Include,
            ),
            Mode::<()>::Plain,
            slot,
            None::<Infallible>,
        );
        let ControlFlow::Continue(ready) = response.complete(sink)?;
        Ok(ready)
    }
}

const _: () = {
    assert!(mem::size_of::<Egress<'_>>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Egress<'_, crate::Input<'_>>>() <= 8 * mem::size_of::<usize>());
};
