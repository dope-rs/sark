use std::{convert::Infallible, pin::Pin};

use o3::buffer::storage::range;
use sark_h1::{
    Input, pump, session,
    session::{Buffered, Conn, Consumption, Dispatch, Outcome, Permit},
    switch,
    wire::{Failure, InputError, head, output, parsing},
};
use sark_http::{
    h1::codec::canned,
    payload::{fixed, owned},
};

#[derive(Default)]
pub struct Frame<'d> {
    pub next: Option<Outcome<'d>>,
    pub after: usize,
    pub input: Option<&'d [u8]>,
    pub calls: usize,
    pub reported: Option<Option<range::Error>>,
}

impl<'d> Frame<'d> {
    pub fn reject() -> Self {
        Self {
            next: Some(Outcome::Close(canned::BAD_REQUEST)),
            ..Self::default()
        }
    }

    pub fn drive<T: switch::Engine<Raw = Infallible>>(
        &mut self,
        conn: &mut Conn<'d, (), Infallible, T>,
        input: &[u8],
    ) -> pump::Cycle<'d, T::Staged> {
        conn.drive(
            Pin::new(self),
            input,
            o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
        )
    }
}

impl<'d, T: switch::Engine<Raw = Infallible>> Dispatch<'d, (), Infallible, T> for Frame<'d> {
    type Ready = Infallible;
    type Head = Infallible;

    fn failure(self: Pin<&mut Self>, cause: &Failure) -> fixed::Body<'d> {
        let range = match cause {
            Failure::Input(InputError::Range(error)) => Some(*error),
            _ => None,
        };
        assert!(self.get_mut().reported.replace(range).is_none());
        cause.body()
    }

    fn resume_head(
        self: Pin<&mut Self>,
        pending: Self::Head,
        _: Permit,
        _: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible, T>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn try_consume(
        self: Pin<&mut Self>,
        permit: Permit,
        input: Input<'_>,
        mut write: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible, T>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        let this = self.get_mut();
        this.calls += 1;
        if let Some(expected) = this.input {
            assert_eq!(input.as_slice(), expected);
        }
        if this.calls > this.after
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
