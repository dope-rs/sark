use o3::buffer::write::Slice;
use std::{cell::Cell, collections::VecDeque};

use dope_test::harness::observe::VecCellExt as _;
use o3::{
    buffer::{resident, storage, write::Sink as _},
    mem::Rejected,
};
use sark_core::duplex::{self, admit, input};
use sark_http::payload::owned;

pub struct Buf(pub VecDeque<storage::Shared>);

pub trait Admit<'d>: duplex::Session<'d> + Sized {
    fn admit<'turn>(
        emission: admit::Emission<'turn, 'd, Self>,
    ) -> admit::Admission<'turn, 'd, Self, (), ()> {
        match emission.into_parts().0 {
            admit::Payload::Vacant(permit) => permit.commit(()),
            admit::Payload::Present { output, permit } => permit.returned((), output),
        }
    }
}

impl<'d, D: duplex::Session<'d>> Admit<'d> for D {}

impl<'d> input::Buf<'d> for Buf {
    type Input = &'d [u8];
    type Error = storage::AllocError;
    type Receipt = usize;

    fn state(&self) -> input::Buffer {
        if self.0.is_empty() {
            input::Buffer::Empty
        } else {
            input::Buffer::Queued
        }
    }

    fn push(&mut self, input: Self::Input) -> Result<(), Rejected<Self::Input, Self::Error>> {
        if !input.is_empty() {
            let owned =
                storage::Shared::try_copy(input).map_err(|error| Rejected::new(input, error))?;
            self.0.push_back(owned);
        }
        Ok(())
    }

    fn take(&mut self) -> input::Take<'d, Self::Receipt> {
        let Some(input) = self.0.pop_front() else {
            return input::Take::Empty;
        };
        let receipt = input.len();
        input::Take::Item {
            input: input.into(),
            receipt,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    First,
    Second,
    Bad,
    Activate,
    Finished,
}

pub struct Session<'a>(pub &'a Cell<Vec<Event>>);
pub struct Idle<'a>(pub &'a Cell<Vec<Event>>);
pub struct Terminal;

pub struct Ending<'a>(pub &'a Cell<Vec<Event>>);

impl<'d> duplex::Session<'d> for Ending<'_> {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::idle()
    }

    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        duplex::Action::idle().activate()
    }

    fn finish(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.0.push(Event::Finished);
        duplex::Action::send_end(resident::Shared::from(b"end".as_slice()))
    }
}

impl<'d> duplex::Session<'d> for Session<'_> {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        _: Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        let event = match input.contiguous() {
            Some(b"first") => Event::First,
            Some(b"second") => Event::Second,
            _ => Event::Bad,
        };
        self.0.push(event);
        duplex::Action::idle()
    }
    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.0.push(Event::Activate);
        duplex::Action::idle().activate()
    }
    fn finish(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.0.push(Event::Finished);
        duplex::Action::close()
    }
}

impl<'d> duplex::Session<'d> for Terminal {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::close()
    }
    fn finish(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        duplex::Action::end()
    }
    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        duplex::Action::close()
    }
}

impl<'d> duplex::Session<'d> for Idle<'_> {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        self.0.push(Event::Bad);
        duplex::Action::idle()
    }

    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.0.push(Event::Activate);
        duplex::Action::idle()
    }

    fn finish(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.0.push(Event::Finished);
        duplex::Action::idle()
    }
}

pub struct Prefix<'a> {
    pub calls: &'a Cell<usize>,
    pub written: usize,
    pub body: bool,
}

impl<'d> duplex::Session<'d> for Prefix<'_> {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        output: Slice<'_, &mut usize>,
        scratch: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        self.finish(output, scratch)
    }

    fn finish(&mut self, mut output: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        self.calls.set(self.calls.get() + 1);
        output
            .write_slice(&b"p"[..self.written])
            .expect("prefix capacity");
        if self.body {
            duplex::Action::send_end(resident::Shared::from(b"end".as_slice()))
        } else {
            duplex::Action::end()
        }
    }

    fn activate(&mut self, _: Slice<'_, &mut usize>, _: &mut Vec<u8>) -> duplex::Action<'d> {
        duplex::Action::end()
    }
}
