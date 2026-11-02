use std::num::NonZero;

use o3::alloc;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        self, CLIENT_PREFACE, DataOutcome, config,
        config::{Config, capacity},
        machine::Connection,
    },
    frame::{
        self, Block,
        layout::{Header, Headers},
        numbers::{ParseError, Type},
        payload::Continuation,
    },
    hpack::encode::{self, Encoder},
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::{headers::entry, hpack, retain::Error};

pub const POST: [field::Entry<'static>; 4] = [
    field::literal!(entry(b":method", b"POST")),
    field::literal!(entry(b":scheme", b"http")),
    field::literal!(entry(b":path", b"/")),
    field::literal!(entry(b":authority", b"localhost")),
];

pub fn try_start(client: &mut Connection<ClientRole>) -> Result<Id, conn::Error> {
    client.streams().headers().request(POST, Boundary::More)
}

type Pair = (Connection<ClientRole>, Connection<ServerRole>, Id);

pub struct Upload {
    config: Config,
}

impl Upload {
    pub const fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn with_payload(plan: capacity::Payload) -> Self {
        Self::new(Config {
            payload_capacity: plan,
            ..Config::default()
        })
    }

    pub fn try_connect(self) -> Result<Pair, Build> {
        let mut client = Connection::<ClientRole>::try_new().map_err(Build::Config)?;
        let id = try_start(&mut client).map_err(Build::Connection)?;
        let mut server =
            Connection::<ServerRole>::with_config(self.config).map_err(Build::Config)?;
        let outbound = client.output().first();
        let settings = Header::parse(&outbound[CLIENT_PREFACE.len()..]).map_err(Build::Parse)?;
        let head_end = CLIENT_PREFACE.len() + frame::HEADER_LEN + settings.length.into_usize();
        server
            .ingress()
            .ingest(&outbound[..head_end])
            .map_err(Build::Connection)?;
        while server.ingress().poll_event().is_some() {}
        server.transfer().drain(usize::MAX);
        server
            .ingress()
            .ingest(&outbound[head_end..])
            .map_err(Build::Connection)?;
        while server.ingress().poll_event().is_some() {}
        server.transfer().drain(usize::MAX);
        client.transfer().drain(usize::MAX);
        Ok((client, server, id))
    }
}

pub struct Wire<'a> {
    bytes: &'a [u8],
}

impl<'a> Wire<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn try_count(self, kind: Type) -> Result<usize, ParseError> {
        let mut bytes = self.bytes;
        let mut count = 0;
        while !bytes.is_empty() {
            let header = Header::parse(bytes)?;
            count += usize::from(header.kind == kind);
            let required = frame::HEADER_LEN + header.length.into_usize();
            bytes = bytes.get(required..).ok_or(ParseError::Need {
                actual: bytes.len(),
                required,
            })?;
        }
        Ok(count)
    }
}

pub struct Fields<'a> {
    id: Id,
    headers: &'a [Entry<'a>],
}

impl<'a> Fields<'a> {
    pub const fn new(id: Id, headers: &'a [Entry<'a>]) -> Self {
        Self { id, headers }
    }

    pub fn try_encode(self, encoder: &mut Encoder) -> Result<Vec<u8>, Build> {
        self.try_headers(encoder, Boundary::End)
    }

    pub fn try_headers(self, encoder: &mut Encoder, boundary: Boundary) -> Result<Vec<u8>, Build> {
        let block = hpack::Fields::new(self.headers)
            .try_with(encoder)
            .map_err(Build::Table)?;
        let mut frame = Vec::new();
        Headers::new(self.id, boundary, Block::End, None, &block)
            .map_err(Build::Parse)?
            .encode(&mut frame)
            .map_err(Build::Encode)?;
        Ok(frame)
    }
}

pub struct Fragments<'a> {
    id: Id,
    boundary: Boundary,
    block: &'a [u8],
}

impl<'a> Fragments<'a> {
    pub const fn new(id: Id, boundary: Boundary, block: &'a [u8]) -> Self {
        Self {
            id,
            boundary,
            block,
        }
    }

    pub fn try_split(self, chunk: NonZero<usize>) -> Result<Vec<Vec<u8>>, Build> {
        let Self {
            id,
            boundary,
            block,
        } = self;
        if block.is_empty() {
            return Err(Build::Empty);
        }
        let chunk = chunk.get();
        let end = chunk.min(block.len());
        let mut frames = Vec::new();
        let mut first = Vec::new();
        let chain = if end == block.len() {
            Block::End
        } else {
            Block::More
        };
        Headers::new(id, boundary, chain, None, &block[..end])
            .map_err(Build::Parse)?
            .encode(&mut first)
            .map_err(Build::Encode)?;
        frames.push(first);
        let mut start = end;
        while start < block.len() {
            let end = start + chunk.min(block.len() - start);
            let mut next = Vec::new();
            let chain = if end == block.len() {
                Block::End
            } else {
                Block::More
            };
            Continuation::new(id, chain, &block[start..end])
                .ok_or(Build::Continuation {
                    id,
                    bytes: end - start,
                })?
                .encode(&mut next)
                .map_err(Build::Encode)?;
            frames.push(next);
            start = end;
        }
        Ok(frames)
    }
}

#[derive(Debug)]
pub enum Build {
    Config(config::Error),
    Connection(conn::Error),
    Parse(ParseError),
    Table(encode::Error),
    Encode(alloc::Error),
    Empty,
    Continuation { id: Id, bytes: usize },
}

pub const RESPONSE_OK: [field::Entry<'static>; 1] = [field::literal!(entry(b":status", b"200"))];
pub const RETAINED_BODY: [u8; 4096] = [b'x'; 4096];

pub fn try_wire() -> Result<(Vec<u8>, usize), Error> {
    let mut client = Connection::<ClientRole>::try_new().map_err(Error::Config)?;
    let stream = try_start(&mut client).map_err(Error::Connection)?;
    let sent = client
        .streams()
        .try_send(stream, b"retained-payload", Boundary::End)
        .map_err(Error::Connection)?;
    if !matches!(sent, DataOutcome::Sent(written) if written.get() == b"retained-payload".len()) {
        return Err(Error::Data(sent));
    }
    let wire = client.output().first().to_vec();
    let offset = wire
        .windows(b"retained-payload".len())
        .position(|bytes| bytes == b"retained-payload")
        .ok_or(Error::Payload)?;
    Ok((wire, offset))
}
