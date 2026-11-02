//! Accepted backing-storage contracts for parsed JSON.

use std::ops;

use o3::{
    alloc::VecExt as _,
    buffer::{storage, write::Sink as _},
};

pub(crate) mod outputs;
pub(crate) mod string;

use crate::bytes::seals;

use crate::{Bytes, Error, Plain, Raw, Text, Token, input, parsing::Depth};

#[doc(hidden)]
pub trait Source: Copy + seals::Source {
    type Frame: crate::Backing;
    type Slice: crate::Backing;

    fn input(&self) -> &[u8];
    fn get_frame(self, range: ops::Range<usize>) -> crate::Result<Self::Frame>;
    fn get_slice(self, range: ops::Range<usize>) -> crate::Result<Self::Slice>;
    fn decode_frame(span: input::StringSpan<'_>) -> crate::Result<Self::Frame>;

    fn frame(self, idx: &mut usize) -> crate::Result<Text<Self::Frame>> {
        self.frame_depth(idx, Depth::ROOT)
    }

    #[doc(hidden)]
    fn frame_depth(self, idx: &mut usize, depth: Depth) -> crate::Result<Text<Self::Frame>> {
        let input = self.input();
        let start = *idx;
        if start >= input.len() {
            return Err(Error::Invalid);
        }
        if input[start] == b'"' {
            let span = input::Input::string_span(input, idx)?;
            if span.escaped {
                Self::decode_frame(span).map(Text::admitted)
            } else {
                self.get_frame(span.start..span.end).map(Text::admitted)
            }
        } else {
            input::Input::skip_depth(input, idx, depth)?;
            self.get_frame(start..*idx).map(Text::admitted)
        }
    }

    fn frame_plain(self, idx: &mut usize) -> crate::Result<Plain<Self::Slice>> {
        let span = input::Input::string_span(self.input(), idx)?;
        if span.escaped {
            return Err(Error::Invalid);
        }
        self.get_slice(span.start..span.end).map(Plain::admitted)
    }

    fn inline_plain<const N: usize>(self, idx: &mut usize) -> crate::Result<Plain<Token<N>>> {
        let input = self.input();
        let content = input::inline_plain(input, idx, inline_capacity::<N>())?;
        Token::from_slice(content.bytes()).map(Plain::admitted)
    }

    fn frame_raw(self, idx: &mut usize) -> crate::Result<Raw<Self::Slice>> {
        self.raw_depth(idx, Depth::ROOT)
    }

    #[doc(hidden)]
    fn raw_depth(self, idx: &mut usize, depth: Depth) -> crate::Result<Raw<Self::Slice>> {
        let input = self.input();
        input::Input::ws(input, idx);
        let start = *idx;
        input::Input::skip_depth(input, idx, depth)?;
        self.get_slice(start..*idx).map(Raw::admitted)
    }

    fn inline_raw<const N: usize>(self, idx: &mut usize) -> crate::Result<Raw<Token<N>>> {
        self.inline_depth(idx, Depth::ROOT)
    }

    #[doc(hidden)]
    fn inline_depth<const N: usize>(
        self,
        idx: &mut usize,
        depth: Depth,
    ) -> crate::Result<Raw<Token<N>>> {
        let input = self.input();
        let range = input::inline_depth(input, idx, inline_capacity::<N>(), depth)?;
        Token::from_slice(&input[range]).map(Raw::admitted)
    }
}

impl<'req> Source for &'req [u8] {
    type Frame = Bytes<'req>;
    type Slice = &'req [u8];

    fn input(&self) -> &[u8] {
        self
    }

    fn get_frame(self, range: ops::Range<usize>) -> crate::Result<Self::Frame> {
        self.get_slice(range).map(Bytes::borrowed)
    }

    fn get_slice(self, range: ops::Range<usize>) -> crate::Result<Self::Slice> {
        self.get(range).ok_or(Error::Invalid)
    }

    fn decode_frame(span: input::StringSpan<'_>) -> crate::Result<Self::Frame> {
        let mut decoded = Vec::try_new(span.decoded_len).map_err(Error::Allocation)?;
        decoded.extend(outputs::Decoder::new(span.content));
        Ok(Bytes::owned(decoded))
    }
}

impl Source for &storage::Shared {
    type Frame = storage::Shared;
    type Slice = storage::Shared;

    fn input(&self) -> &[u8] {
        self.as_slice()
    }

    fn get_frame(self, range: ops::Range<usize>) -> crate::Result<Self::Frame> {
        self.try_get(range)
            .map_err(Error::Retain)?
            .ok_or(Error::Invalid)
    }

    fn get_slice(self, range: ops::Range<usize>) -> crate::Result<Self::Slice> {
        self.get_frame(range)
    }

    fn decode_frame(span: input::StringSpan<'_>) -> crate::Result<Self::Frame> {
        let decoded = storage::Exact::try_build(span.decoded_len, |writer| {
            for byte in outputs::Decoder::new(span.content) {
                writer.write_byte(byte)?;
            }
            Ok(())
        })
        .map_err(|error| match error {
            storage::BuildError::Capacity(error) | storage::BuildError::Build(error) => {
                Error::from(storage::AllocError::Capacity(error))
            }
            storage::BuildError::Exhausted(error) => {
                Error::from(storage::AllocError::Exhausted(error))
            }
            storage::BuildError::Length(error) => Error::OutputLength(error),
        })?;
        Ok(decoded.into_shared())
    }
}

fn inline_capacity<const N: usize>() -> u8 {
    const { assert!(N <= u8::MAX as usize) }
    N as u8
}
