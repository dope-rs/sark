use std::{cell::Cell, convert::Infallible, pin::Pin};

use o3::buffer::{Complete as _, storage::range};
use sark_h1::{
    Input, pump, session, switch,
    wire::{Failure, InputError, head, parsing},
};
use sark_http::{
    h1::codec::canned,
    payload::{fixed, owned},
};

pub struct Reject<'a> {
    pub calls: &'a Cell<usize>,
    pub input: Option<&'a [u8]>,
    pub cause: Option<Failure>,
    pub reported: Vec<Option<range::Error>>,
}

impl<'a> Reject<'a> {
    pub fn new(calls: &'a Cell<usize>, cause: Option<Failure>) -> Self {
        Self {
            calls,
            input: None,
            cause,
            reported: Vec::new(),
        }
    }

    pub fn close<'d, T: switch::Engine>(
        &mut self,
        conn: &mut session::Conn<'d, (), Infallible, T>,
    ) -> pump::Cycle<'d, T::Staged> {
        pump::Ingress::drive(
            conn,
            Pin::new(self),
            b"rejected request",
            o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
            head::Connection::KeepAlive,
        )
    }
}

impl<'d, T: switch::Engine> session::Dispatch<'d, (), Infallible, T> for Reject<'_> {
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
        _: session::Permit,
        _: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut session::Conn<'d, (), Infallible, T>,
    ) -> session::RouteOutcome<'d, Infallible, T::Raw, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: o3::buffer::write::Slice<'_, &mut usize>,
        conn: &mut session::Conn<'d, (), Infallible, T>,
    ) -> session::RouteOutcome<'d, Infallible, T::Raw, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn try_consume(
        self: Pin<&mut Self>,
        _: session::Permit,
        input: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut session::Conn<'d, (), Infallible, T>,
    ) -> session::RouteOutcome<'d, Infallible, T::Raw, Self::Head, parsing::Rejected<'d>> {
        let this = self.get_mut();
        if let Some(expected) = this.input {
            assert_eq!(input.as_slice(), expected);
        }
        this.calls.set(this.calls.get() + 1);
        match this.cause.take() {
            Some(cause) => session::Outcome::Failed(cause).into(),
            None => session::Outcome::Close(canned::BAD_REQUEST).into(),
        }
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
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, T::Raw> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, T::Raw> {
        match ready {}
    }
}
