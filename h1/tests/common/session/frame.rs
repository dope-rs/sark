use std::{convert::Infallible, pin::Pin};

use o3::buffer::{Complete as _, storage::range};
use sark_h1::{
    Input, session,
    session::{Buffered, Conn, Consumption, Dispatch, Outcome, Permit},
    wire::{Failure, InputError, head, output, parsing},
};
use sark_http::payload::{fixed, owned};

#[derive(Default)]
pub struct Frame<'d> {
    pub next: Option<Outcome<'d>>,
    pub calls: usize,
    pub reported: Vec<Option<range::Error>>,
}

impl<'d> Dispatch<'d, (), Infallible> for Frame<'d> {
    type Ready = Infallible;
    type Head = Infallible;

    fn failure(self: Pin<&mut Self>, cause: &Failure) -> fixed::Body<'d> {
        self.get_mut().reported.push(match cause {
            Failure::Input(InputError::Range(error)) => Some(*error),
            _ => None,
        });
        cause.body()
    }

    fn resume_head(
        self: Pin<&mut Self>,
        pending: Self::Head,
        _: Permit,
        _: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: Permit,
        source: parsing::Source,
        write: o3::buffer::write::Slice<'_, &mut usize>,
        conn: &mut Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn try_consume(
        self: Pin<&mut Self>,
        permit: Permit,
        input: Input<'_>,
        mut write: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        let this = self.get_mut();
        this.calls += 1;
        if this.calls > 1
            && let Some(next) = this.next.take()
        {
            return next.into();
        }
        o3::buffer::write::Sink::write_byte(&mut write, input.as_slice()[0]).expect("inline frame");
        Outcome::Complete {
            permit,
            consumption: Consumption::Buffered(Buffered::new(1)),
            response: output::Complete::Inline,
            connection: head::Connection::KeepAlive,
        }
        .into()
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
        _: Permit,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match ready {}
    }
}
