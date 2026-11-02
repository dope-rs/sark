use std::{convert::Infallible, pin::Pin};

use o3::buffer::Complete as _;
use sark_h1::{Input, session, session::Dispatch, switch, wire::parsing};
use sark_http::payload::{fixed, owned};

pub struct Pending;

impl<'d> Dispatch<'d, (), Infallible> for Pending {
    type Ready = Infallible;
    type Head = ();

    fn try_consume(
        self: Pin<&mut Self>,
        permit: session::Permit,
        _: Input<'_>,
        _: &mut [u8],
        _: &mut session::Conn<'d, (), Infallible, switch::Disabled, Infallible, ()>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, (), parsing::Rejected<'d>> {
        session::RouteOutcome::AwaitHead {
            permit,
            pending: (),
        }
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, (), Infallible, switch::Disabled, Infallible, ()>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, (), parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn resume_head(
        self: Pin<&mut Self>,
        _: (),
        permit: session::Permit,
        input: Input<'_>,
        write: &mut [u8],
        conn: &mut session::Conn<'d, (), Infallible, switch::Disabled, Infallible, ()>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, (), parsing::Rejected<'d>> {
        self.try_consume(permit, input, write, conn)
    }

    fn body_data(
        self: Pin<&mut Self>,
        pending: Infallible,
        _: owned::Segments<'d>,
        _: &mut (),
    ) -> Result<Infallible, fixed::Body<'d>> {
        match pending {}
    }

    fn has_body_work(_: &Infallible) -> bool {
        false
    }

    fn resume_pending(
        self: Pin<&mut Self>,
        pending: Infallible,
        _: &mut (),
    ) -> Result<Infallible, fixed::Body<'d>> {
        Ok(pending)
    }

    fn finish_body(
        self: Pin<&mut Self>,
        pending: Infallible,
        _: session::Complete,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match ready {}
    }
}
