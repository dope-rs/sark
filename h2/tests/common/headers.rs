use std::{cell::Cell, slice};

use o3::{
    alloc,
    buffer::{pool, storage, storage::Shared},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{Error, event::Event, machine::Connection},
    frame::{
        Block,
        layout::Headers,
        payload::{Continuation, PushPromise},
    },
    hpack::{
        capacity,
        encode::{self, Encoder},
    },
    stream::Id,
};
use sark_http::{
    field,
    field::{
        base64,
        metadata::{
            block, lower,
            map::record::{Entry, Span},
            packed::prefix,
            section::{
                rules::{self, Policy, Push},
                state::decode::{Final, Indexed, Session},
            },
        },
    },
    payload::response::headers,
};

use crate::common::hpack;

pub(crate) const TEXT: headers::Entry<'static> = headers::Entry::new(
    field::literal!(headers::Name::try_bytes(b"content-type")),
    field::literal!(field::Value::try_bytes(b"text/plain; charset=utf-8")),
);

/// Admits fixture bytes once; literal callers evaluate this at compile time.
pub(crate) const fn entry<'a>(
    name: &'a [u8],
    value: &'a [u8],
) -> Result<field::Entry<'a>, field::Error> {
    let value = match field::Value::try_bytes(value) {
        Ok(value) => value,
        Err(error) => return Err(error),
    };
    let pseudo = match name {
        b":method" => field::Pseudo::Method,
        b":scheme" => field::Pseudo::Scheme,
        b":authority" => field::Pseudo::Authority,
        b":path" => field::Pseudo::Path,
        b":protocol" => field::Pseudo::Protocol,
        b":status" => field::Pseudo::Status,
        _ => {
            let name = match field::Lower::try_bytes(name) {
                Ok(name) => name,
                Err(error) => return Err(error),
            };
            return Ok(field::Entry::new(name, value));
        }
    };
    Ok(field::Entry::pseudo(pseudo, value))
}

pub fn try_head(lengths: &[&[u8]]) -> Result<Final<rules::RetainRaw>, Build> {
    let fields: [(&[u8], &[u8]); 4] = [
        (b":method", b"GET"),
        (b":scheme", b"http"),
        (b":authority", b"x"),
        (b":path", b"/"),
    ];
    let fields = fields.into_iter().chain(
        lengths
            .iter()
            .map(|value| (b"content-length".as_slice(), *value)),
    );
    let capacity = fields
        .clone()
        .map(|(name, value)| prefix::Prefix::LEN + name.len() + value.len())
        .sum();
    let lease = pool::Span::try_new(capacity).map_err(Build::Pool)?;
    let mut decoder = Session::<rules::RetainRaw>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        Default::default(),
    )
    .map_err(|(_, error)| Build::Decode(error))?;
    for (name, value) in fields {
        let name = block::Named::from_slice(name);
        let value = block::Valued::from_span(Span::from_slice(value));
        let indexed = decoder.indexed(Some(name)).map_err(Build::Decode)?;
        match indexed {
            Indexed::Skip => {
                decoder.validate(value.validation());
            }
            Indexed::Buffered(()) => decoder
                .field::<rules::RejectPush>(name, value, field::Indexing::Allowed)
                .map_err(Build::Decode)?,
        }
    }
    decoder.finish().1.map_err(Build::Decode)
}

pub struct Generated<'a>(pub &'a [u8]);

pub struct Counted<'a, S> {
    pub calls: &'a Cell<usize>,
    pub source: S,
}

impl<S: field::Source> field::Source for Counted<'_, S> {
    fn emit<'field>(&'field self, fields: &mut impl field::Sink<'field>) {
        self.calls.set(self.calls.get().saturating_add(1));
        self.source.emit(fields);
    }
}

impl field::Source for Generated<'_> {
    fn emit<'field>(&'field self, fields: &mut impl field::Sink<'field>) {
        fields.entry(field::literal!(entry(b":method", b"GET")));
        fields.entry(field::literal!(entry(b":scheme", b"http")));
        fields.entry(field::literal!(entry(b":authority", b"x")));
        fields.entry(field::literal!(entry(b":path", b"/")));
        fields.generated(
            field::literal!(field::Lower::try_bytes(b"x-generated")),
            base64::Encoder::new(self.0),
        );
    }
}

#[derive(Clone, Copy)]
pub struct Authority;

impl Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = Some(&[b"http"]);
    const AUTHORITIES: Option<&[&[u8]]> = Some(&[b"x"]);
}

pub struct Frames {
    id: Id,
}

impl Frames {
    pub fn try_new(id: u32) -> Result<Self, Build> {
        Ok(Self {
            id: Id::new(id).ok_or(Build::Id(id))?,
        })
    }

    pub fn try_split(self) -> Result<(Shared, Shared), Build> {
        let mut encoder = Encoder::try_new(4096).map_err(Build::Encoder)?;
        let block = hpack::Fields::new(&[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"http"),
            Entry::new(b":authority", b"x"),
            Entry::new(b":path", b"/"),
        ])
        .try_with(&mut encoder)
        .map_err(Build::Table)?;
        let split = 1;
        let id = self.id;
        let mut first = Vec::new();
        Headers::new(id, Boundary::End, Block::More, None, &block[..split])
            .map_err(|_| Build::Headers { id, bytes: split })?
            .encode(&mut first)
            .map_err(Build::Encode)?;
        let mut second = Vec::new();
        Continuation::new(id, Block::End, &block[split..])
            .ok_or(Build::Continuation {
                id,
                bytes: block.len() - split,
            })?
            .encode(&mut second)
            .map_err(Build::Encode)?;
        Ok((
            Shared::try_copy(&first).map_err(Build::Owner)?,
            Shared::try_copy(&second).map_err(Build::Owner)?,
        ))
    }

    pub fn try_wire(self) -> Result<Vec<Vec<u8>>, Build> {
        let mut encoder = Encoder::try_new(4096).map_err(Build::Encoder)?;
        let block = hpack::Fields::new(&[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"http"),
            Entry::new(b":authority", b"x"),
            Entry::new(b":path", b"/"),
            Entry::new(
                b"x-owner-split",
                b"abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz",
            ),
        ])
        .try_with(&mut encoder)
        .map_err(Build::Table)?;
        let id = self.id;
        let mut frames = Vec::new();
        let mut first = Vec::new();
        Headers::new(id, Boundary::End, Block::More, None, &block[..1])
            .map_err(|_| Build::Headers { id, bytes: 1 })?
            .encode(&mut first)
            .map_err(Build::Encode)?;
        frames.push(first);
        for (index, byte) in block[1..].iter().enumerate() {
            let mut frame = Vec::new();
            let chain = if index + 2 == block.len() {
                Block::End
            } else {
                Block::More
            };
            Continuation::new(id, chain, slice::from_ref(byte))
                .ok_or(Build::Continuation { id, bytes: 1 })?
                .encode(&mut frame)
                .map_err(Build::Encode)?;
            frames.push(frame);
        }
        Ok(frames)
    }

    pub fn try_owners(self) -> Result<Vec<Shared>, Build> {
        self.try_wire()?
            .into_iter()
            .map(|frame| Shared::try_copy(&frame).map_err(Build::Owner))
            .collect()
    }

    pub fn try_push(self, promised: u32) -> Result<Shared, Build> {
        let parent = self.id.into_u32();
        let mut encoder = Encoder::try_new(4096).map_err(Build::Encoder)?;
        let block = hpack::Fields::new(&[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"http"),
            Entry::new(b":path", b"/asset"),
            Entry::new(b":authority", b"x"),
        ])
        .try_with(&mut encoder)
        .map_err(Build::Table)?;
        let mut wire = Vec::new();
        PushPromise::new(
            self.id,
            Id::new(promised).ok_or(Build::Id(promised))?,
            Block::End,
            &block,
        )
        .ok_or(Build::Push {
            parent,
            promised,
            bytes: block.len(),
        })?
        .encode(&mut wire)
        .map_err(Build::Encode)?;
        Shared::try_copy(&wire).map_err(Build::Owner)
    }
}

use sark_http::field::metadata::section::state::Selected;

pub struct Response<'a, P: Policy> {
    conn: &'a mut Connection<ServerRole, P>,
}

impl<'a, P: Policy> Response<'a, P> {
    pub fn new(conn: &'a mut Connection<ServerRole, P>) -> Self {
        Self { conn }
    }

    pub fn try_finish(self) -> Result<(), Consume<Selected<P>, P::Block>> {
        let conn = self.conn;
        let id = match conn.ingress().poll_event() {
            Some(Event::Headers { stream_id: id, .. }) => id,
            event => return Err(Consume::Event(event)),
        };
        conn.streams()
            .headers()
            .respond_key(
                id,
                [field::literal!(entry(b":status", b"204"))],
                Boundary::End,
            )
            .map_err(Consume::Response)?;
        conn.transfer().drain(usize::MAX);
        Ok(())
    }
}

#[derive(Debug)]
pub enum Consume<S, B> {
    Event(Option<Event<S, B>>),
    Response(Error),
}

#[derive(Debug)]
pub enum Build {
    Pool(pool::CreateError),
    Decode(lower::Error),
    Encoder(capacity::Error),
    Table(encode::Error),
    Encode(alloc::Error),
    Owner(storage::AllocError),
    Id(u32),
    Headers {
        id: Id,
        bytes: usize,
    },
    Continuation {
        id: Id,
        bytes: usize,
    },
    Push {
        parent: u32,
        promised: u32,
        bytes: usize,
    },
}
