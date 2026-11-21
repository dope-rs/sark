use std::{convert::Infallible, pin::Pin};

use o3::buffer::storage::range;
use sark_h1::{
    Input, pump, session,
    session::{Conn, Consumption, Dispatch, Outcome},
    switch,
    wire::{Failure, InputError, head, output},
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
        pending: Self::Head,
        turn: &mut session::Turn<'_, '_, 'd, (), Infallible, T, Self>,
    ) -> session::Flow {
        turn.await_head(pending)
    }

    fn try_consume(
        input: Input<'_>,
        turn: &mut session::Turn<'_, '_, 'd, (), Infallible, T, Self>,
        prior: usize,
    ) -> session::Flow {
        let this = turn.router.as_mut().get_mut();
        this.calls += 1;
        if let Some(expected) = this.input {
            assert_eq!(input.as_slice(), expected);
        }
        if let Some(pending) = this.head.take() {
            return turn.await_head(pending);
        }
        if this.calls > this.after
            && let Some(next) = this.next.take()
        {
            return turn.apply(next, prior);
        }
        o3::buffer::write::Sink::write_byte(&mut turn.output, input.as_slice()[0])
            .expect("inline frame");
        turn.complete(
            Consumption::Buffered(1),
            output::Complete::Inline,
            head::Connection::KeepAlive,
            prior,
        )
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
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Raw> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Raw> {
        match ready {}
    }
}
