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

pub struct Frame<'d, Raw = Infallible, Head = Infallible> {
    pub next: Option<Outcome<'d, Raw>>,
    pub head: Option<Head>,
    pub after: usize,
    pub input: Option<&'d [u8]>,
    pub calls: usize,
    pub reported: Option<Option<range::Error>>,
}

impl<Raw, Head> Default for Frame<'_, Raw, Head> {
    fn default() -> Self {
        Self {
            next: None,
            head: None,
            after: 0,
            input: None,
            calls: 0,
            reported: None,
        }
    }
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

impl<'d, Raw: Unpin, Head: Unpin, T: switch::Engine<Raw = Raw>> Dispatch<'d, (), Infallible, T>
    for Frame<'d, Raw, Head>
{
    type Ready = Infallible;
    type Head = Head;

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
        permit: Permit,
        _: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible, T, Infallible, Head>,
    ) -> session::RouteOutcome<'d, Infallible, Raw, Head, parsing::Rejected<'d>> {
        session::RouteOutcome::AwaitHead { permit, pending }
    }

    fn try_consume(
        self: Pin<&mut Self>,
        permit: Permit,
        input: Input<'_>,
        mut write: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Conn<'d, (), Infallible, T, Infallible, Head>,
    ) -> session::RouteOutcome<'d, Infallible, Raw, Head, parsing::Rejected<'d>> {
        let this = self.get_mut();
        this.calls += 1;
        if let Some(expected) = this.input {
            assert_eq!(input.as_slice(), expected);
        }
        if let Some(pending) = this.head.take() {
            return session::RouteOutcome::AwaitHead { permit, pending };
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
    ) -> session::BodyOutcome<'d, Self::Ready, Raw> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Raw> {
        match ready {}
    }
}
