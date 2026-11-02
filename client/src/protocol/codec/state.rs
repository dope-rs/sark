use std::num;

use dope::manifold::client::session::{codec, response::Outcome};
use o3::buffer::{self, hold, resident};

use crate::protocol::{
    Error,
    codec::{
        chunks,
        output::{self, Completion, Output, Parsed, Preview},
        retain,
    },
    response,
};

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Content {
    Allowed,
    Forbidden,
}

pub(super) enum Framing<'d> {
    Head,
    Sized { remaining: num::NonZero<usize> },
    Chunked(chunks::Chunks<'d>),
    UntilEof { received: usize, content: Content },
    Failed,
}

const _: () = assert!(size_of::<Content>() == size_of::<bool>());
const _: () = assert!(align_of::<Content>() == align_of::<bool>());

pub struct State<'d> {
    pub(super) framing: Framing<'d>,
    pub(super) informational: usize,
    pub(super) budget: resident::Budget<'d>,
}

impl<'d> State<'d> {
    pub(super) fn new(budget: resident::Budget<'d>) -> Self {
        Self {
            framing: Framing::Head,
            informational: 0,
            budget,
        }
    }

    pub(super) fn finish<'id, 'input, R, T: Outcome, F>(
        &mut self,
        buf: codec::Input<'id, 'input, 'd, R>,
        limit: usize,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Finish<T>, T>
    where
        R: buffer::Cursor + hold::raw::Retain<'d>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
        'd: 'input,
    {
        let plan = (|| {
            let (received, content) = match &self.framing {
                Framing::Failed => return Ok(None),
                Framing::UntilEof { received, content } => (*received, *content),
                Framing::Sized { .. } => return Err(Error::IncompleteBody),
                Framing::Chunked(_) => return Err(Error::IncompleteChunked),
                Framing::Head if buf.is_empty() => return Ok(None),
                Framing::Head => return Err(Error::IncompleteHead),
            };
            if let Content::Forbidden = content
                && !buf.is_empty()
            {
                return Err(Error::ForbiddenContent);
            }
            if received
                .checked_add(buf.len())
                .is_none_or(|total| total > limit)
            {
                return Err(Error::BodyLimit);
            }
            let bytes = if buf.is_empty() {
                None
            } else {
                Some(buf.try_plan(0..buf.len()).map_err(Error::Range)?)
            };
            Ok(Some(bytes))
        })();
        let bytes = match plan {
            Ok(None) => return Ok(codec::Finish::Empty),
            Ok(Some(bytes)) => bytes,
            Err(error) => return Err(output::rejected(error, self, admit)),
        };
        let emit = match admit(Ok(Preview::body(
            bytes.as_ref().map_or(&[], |bytes| bytes.as_slice()),
            Completion::Complete,
        ))) {
            codec::Admission::RetryInput => return Ok(codec::Finish::Item(T::retry_input())),
            codec::Admission::Ready(emit) => emit,
        };
        let output = match bytes {
            None => Output::silent(Completion::Complete),
            Some(bytes) => {
                let bytes = match bytes.bind(retain::RESPONSE).try_into() {
                    Ok(bytes) => bytes,
                    Err(error) => return Err(T::accept(emit(Err(Error::Retention(error)), self))),
                };
                Output::event(
                    response::Event::Data(response::Data::from_owner(bytes)),
                    Completion::Complete,
                )
            }
        };
        self.framing = Framing::Head;
        Ok(codec::Finish::Item(T::accept(emit(
            Ok(output.into()),
            self,
        ))))
    }
}
