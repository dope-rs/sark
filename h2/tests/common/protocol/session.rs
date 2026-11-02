use sark_core::manifold::stream::Boundary;
use sark_h2::{
    frame::{
        self,
        layout::{Data, Header, Headers},
        numbers::{ErrorCode, Flags, Length, ParseError, Type},
        payload::{Continuation, Payload, PushPromise},
    },
    stream::Id,
};
use sark_http::field::metadata::section::rules::Push;

use crate::common::protocol::rules::Build;

pub const OK_RESPONSE: &[u8] = &[0x88];

pub const SELF_DEPENDENT_HEADERS: &[u8] = &[
    0, 0, 11, 1, 0x24, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0x82, 0x86, 0x84, 1, 1, b'x',
];

#[derive(Clone, Copy)]
pub struct Authority;

impl Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = Some(&[b"http"]);
    const AUTHORITIES: Option<&[&[u8]]> = Some(&[b"x", b"example.com"]);
}

pub struct Frame<'a> {
    id: u32,
    bytes: &'a [u8],
}

impl<'a> Frame<'a> {
    pub const fn new(id: u32, bytes: &'a [u8]) -> Self {
        Self { id, bytes }
    }
    pub fn try_headers(self, boundary: Boundary, chain: frame::Block) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        Headers::new(
            Id::new(self.id).ok_or(Build::Stream(self.id))?,
            boundary,
            chain,
            None,
            self.bytes,
        )
        .map_err(Build::Parse)?
        .encode(&mut out)
        .map_err(Build::Encode)?;
        Ok(out)
    }
    pub fn try_continue(self, chain: frame::Block) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        let stream = Id::new(self.id).ok_or(Build::Stream(self.id))?;
        Continuation::new(stream, chain, self.bytes)
            .ok_or(Build::Continuation {
                stream,
                bytes: self.bytes.len(),
            })?
            .encode(&mut out)
            .map_err(Build::Encode)?;
        Ok(out)
    }
    pub fn try_data(self, boundary: Boundary) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        let stream = Id::new(self.id).ok_or(Build::Stream(self.id))?;
        Data::new(stream, boundary, self.bytes)
            .ok_or(Build::Data {
                stream,
                bytes: self.bytes.len(),
            })?
            .encode(&mut out)
            .map_err(Build::Encode)?;
        Ok(out)
    }
    pub fn try_padded(self, boundary: Boundary, padding: u8) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        let flags = match boundary {
            Boundary::More => Flags::PADDED,
            Boundary::End => Flags::PADDED | Flags::END_STREAM,
        };
        let length = self
            .bytes
            .len()
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(padding as usize))
            .ok_or(Build::Length {
                bytes: self.bytes.len(),
                padding,
            })?;
        out.extend_from_slice(
            &Header {
                length: Length::from_usize(length).ok_or(Build::Length {
                    bytes: self.bytes.len(),
                    padding,
                })?,
                kind: Type::Data,
                flags: Flags(flags),
                stream_id: Id::new(self.id).ok_or(Build::Stream(self.id))?,
            }
            .wire_bytes(),
        );
        out.push(padding);
        out.extend_from_slice(self.bytes);
        out.resize(out.len() + padding as usize, 0);
        Ok(out)
    }
    pub fn try_push(self, promised: u32, chain: frame::Block) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        let stream = Id::new(self.id).ok_or(Build::Stream(self.id))?;
        let promised = Id::new(promised).ok_or(Build::Stream(promised))?;
        PushPromise::new(stream, promised, chain, self.bytes)
            .ok_or(Build::Push {
                stream,
                promised,
                bytes: self.bytes.len(),
            })?
            .encode(&mut out)
            .map_err(Build::Encode)?;
        Ok(out)
    }
}

pub struct Wire<'a> {
    bytes: &'a [u8],
}

impl<'a> Wire<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub fn try_reset(self) -> Result<Option<(Id, ErrorCode)>, ParseError> {
        let mut out = self.bytes;
        while !out.is_empty() {
            let (frame, consumed) = Payload::parse(out)?;
            if let Payload::RstStream(reset) = frame {
                return Ok(Some((reset.stream_id(), reset.error())));
            }
            out = &out[consumed..];
        }
        Ok(None)
    }
    pub fn try_window(self) -> Result<Option<u32>, ParseError> {
        let mut out = self.bytes;
        while !out.is_empty() {
            let (frame, consumed) = Payload::parse(out)?;
            if let Payload::WindowUpdate(update) = frame
                && update.stream_id == Id::CONNECTION
            {
                return Ok(Some(update.increment.get()));
            }
            out = &out[consumed..];
        }
        Ok(None)
    }
}
