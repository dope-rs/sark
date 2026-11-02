use std::{
    cell::Cell,
    io::{self, Read},
    net,
    net::TcpStream,
};

use dope::core::driver::route::KeyTag;
use o3::alloc;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    frame,
    frame::{
        Block,
        layout::{Header, Headers},
        numbers,
        payload::Continuation,
    },
    hpack::encode::{self, Encoder},
    server::{
        self, runtime,
        service::{Handler, Request},
    },
    stream::Id,
};
use sark_http::{
    field::metadata::map::record::Entry,
    o3::buffer::resident::bound,
    payload::response::{fixed, headers, status::Code},
    request::body::Ref,
};

use crate::common::{
    body::{Configure, LIMITS},
    headers::TEXT,
};

pub struct Echo<'a>(pub &'a Cell<usize>);

impl Handler for Echo<'_> {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = fixed::Direct<[headers::Entry<'req>; 1], Ref<'req, 'd>>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        self.0.set(self.0.get() + 1);
        fixed::Direct::new(Code::OK, [TEXT], request.body())
    }
}

impl Echo<'_> {
    pub fn try_plan(
        bind: net::SocketAddr,
    ) -> Result<runtime::Plan<KeyTag<0>>, server::ConfigError> {
        server::Config {
            max_request_body_bytes: 7,
            max_connection_body_bytes: 7,
            ..LIMITS
        }
        .try_plan(bind)
    }
}

#[derive(Debug)]
pub enum Error {
    Continuation,
    Encode(alloc::Error),
    Hpack(encode::Error),
    Io(io::Error),
    Parse(numbers::ParseError),
    Closed,
}

pub trait Blocks {
    fn try_request(&mut self) -> Result<Vec<u8>, encode::Error>;
    fn try_invalid(&mut self, id: Id) -> Result<(Vec<u8>, usize), Error>;
}

impl Blocks for Encoder {
    fn try_request(&mut self) -> Result<Vec<u8>, encode::Error> {
        let mut block = Vec::new();
        self.try_encode(
            [
                Entry::new(b":method", b"GET"),
                Entry::new(b":scheme", b"http"),
                Entry::new(b":path", b"/"),
            ],
            &mut block,
        )?;
        Ok(block)
    }

    fn try_invalid(&mut self, id: Id) -> Result<(Vec<u8>, usize), Error> {
        let mut block = Vec::new();
        self.try_encode([Entry::new(b"x-discard-sync", b"retained")], &mut block)
            .map_err(Error::Hpack)?;
        let split = block.len() / 2;
        let mut wire = Vec::new();
        Headers::new(id, Boundary::More, Block::More, None, &block[..split])
            .map_err(Error::Parse)?
            .encode(&mut wire)
            .map_err(Error::Encode)?;
        Continuation::new(id, Block::End, &block[split..])
            .ok_or(Error::Continuation)?
            .encode(&mut wire)
            .map_err(Error::Encode)?;
        Ok((wire, block.len()))
    }
}

pub trait ReadFrames {
    fn try_frames(
        &mut self,
        pending: &mut Vec<u8>,
        visit: impl FnMut(Header, &[u8]) -> bool,
    ) -> Result<(), Error>;
}

impl ReadFrames for TcpStream {
    fn try_frames(
        &mut self,
        pending: &mut Vec<u8>,
        mut visit: impl FnMut(Header, &[u8]) -> bool,
    ) -> Result<(), Error> {
        let mut input = [0; 16 * 1024];
        loop {
            while pending.len() >= frame::HEADER_LEN {
                let header = Header::parse(pending).map_err(Error::Parse)?;
                let total = frame::HEADER_LEN + header.length.into_usize();
                if pending.len() < total {
                    break;
                }
                let stop = visit(header, &pending[frame::HEADER_LEN..total]);
                pending.copy_within(total.., 0);
                pending.truncate(pending.len() - total);
                if stop {
                    return Ok(());
                }
            }
            let read = self.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed);
            }
            pending.extend_from_slice(&input[..read]);
        }
    }
}
