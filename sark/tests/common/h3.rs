use std::collections::BTreeMap;

use dope_quic::wire::stream::Id;
use sark_core::{
    body::admit::{Finished, Open, Track},
    decode::{Decode, exec, request, state},
    manifold::stream::Boundary,
};
use sark_h3::{
    conn::{
        self, Connection,
        event::{Event, Role},
        io::{Receive, Step},
        options::{self, plan},
        output::{self, WritePayload},
    },
    frame::{self, Payload, TYPE_HEADERS},
    link::{Outcome, Transport},
    qpack::encoder::{self, Encoder},
};
use sark_http::{
    field::{self, Entry, Pseudo, Value, metadata::section::storage::Block},
    payload::{fixed, response::status::Code},
};

pub trait Routing: Decode + Sized {
    fn try_conn<'d>(&self) -> Result<Connection<'d, Self::Plan>, options::Error> {
        plan::Config::new(Default::default())?.build(Role::Server)
    }

    fn try_retained(&self) -> Result<Option<usize>, Build> {
        let mut qpack = Encoder::new();
        let wire = qpack.try_frame(Id::ZERO, [
            const { Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))) },
            const { Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))) },
            const { Entry::pseudo(Pseudo::Authority, field::literal!(Value::try_bytes(b"x"))) },
            const { Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/json"))) },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"x-name")),
                    field::literal!(Value::try_bytes(b"alice")),
                )
            },
        ])?;
        let mut server = self.try_conn().map_err(Build::Config)?;
        Receive::new(&mut server)
            .owned(Id::ZERO, wire, Boundary::End)
            .map_err(Build::Connection)?;
        loop {
            match server.events().poll_token().map_err(Build::Connection)? {
                Step::Item(Event::Headers { head, .. }) => {
                    let (fields, _, _) = head.into_parts();
                    return Ok(Some(Block::into_bytes(fields).as_bytes().len()));
                }
                Step::Item(_) | Step::More => {}
                Step::Idle => return Ok(None),
            }
        }
    }
}

impl<R: Decode> Routing for R {}

#[derive(Debug)]
pub enum Build {
    Config(options::Error),
    Connection(conn::Error),
    Encode(encoder::Error),
    Frame(frame::EncodeError),
}

#[sark::response(raw)]
#[header("content-type", "text/plain")]
pub struct Reply<'d> {
    pub(crate) status: Code,
    pub(crate) body: fixed::Body<'d>,
}

#[sark::request]
pub struct JsonReq {
    #[authority]
    pub authority: storage::Shared,
    #[header("x-name", default = "none")]
    pub name: storage::Shared,
    #[header("content-length")]
    pub length: Option<usize>,
}

#[sark::request]
pub struct PlainReq {
    #[header("x-name", default = "none")]
    pub name: storage::Shared,
}

#[sark::handler(pub)]
pub(crate) fn json_h(req: JsonReq) -> Reply<'d> {
    Reply {
        status: Code::OK,
        body: if req.name == b"alice" && req.authority == b"x" && req.length == Some(0) {
            fixed::Body::new(b"hello-h3")
        } else {
            fixed::Body::new(b"missing-header")
        },
    }
}

#[sark::handler(pub)]
pub(crate) fn json_no_authority_h(req: PlainReq) -> Reply<'d> {
    Reply {
        status: Code::OK,
        body: if req.name == b"alice" {
            fixed::Body::new(b"plain")
        } else {
            fixed::Body::new(b"missing-header")
        },
    }
}

sark::define_router! {
    pub H3PlainRouter => {
        GET "/json" => request(header) json_no_authority_h,
    }
}

sark::define_router! {
    pub H3Router => {
        GET "/json" => request(header) json_h,
    }
}

pub trait Framing {
    fn try_frame(&mut self, stream: Id, fields: impl field::Source) -> Result<Vec<u8>, Build>;
}

impl Framing for Encoder {
    fn try_frame(&mut self, stream: Id, fields: impl field::Source) -> Result<Vec<u8>, Build> {
        let mut block = Vec::new();
        self.encode(stream.get(), fields, &mut block)
            .map_err(Build::Encode)?;
        let mut wire = Vec::new();
        Payload::encode(TYPE_HEADERS, &block, &mut wire).map_err(Build::Frame)?;
        Ok(wire)
    }
}

#[derive(Default)]
pub struct Wire {
    pub sent: BTreeMap<u64, Capture>,
}

pub struct Capture(pub Vec<u8>, pub Boundary);

impl<'d> Transport<'d> for Wire {
    fn send_write(&mut self, write: &output::Write<'d>) -> Outcome {
        let boundary = write.boundary();
        let capture = self
            .sent
            .entry(write.stream_id().get())
            .or_insert(Capture(Vec::new(), boundary));
        capture.1 = boundary;
        let bytes = &mut capture.0;
        bytes.extend_from_slice(&write.prefix);
        if let Some(payload) = write.payload.as_ref() {
            match payload {
                WritePayload::Owned(payload) => bytes.extend_from_slice(payload),
                WritePayload::Shared(payload) => {
                    bytes.extend_from_slice(payload.as_slice());
                }
                WritePayload::Retained(payload) => {
                    bytes.extend_from_slice(payload.as_slice());
                }
                WritePayload::Segmented(payload) => {
                    for segment in payload.as_chain().segments() {
                        bytes.extend_from_slice(segment);
                    }
                }
            }
        }
        Outcome::Accepted
    }
}

pub struct Selected;

impl<P> exec::Fold<P, state::Admitted, exec::Sync> for Selected {
    type Output = state::Uniform<P, state::Admitted>;

    fn open<S: state::Slot<P>>(self, slot: S, body: Track<Open>) -> Self::Output {
        state::Uniform::new(slot.prepare(), request::Body::Open(body))
    }

    fn finished<S: state::Slot<P>>(self, slot: S, body: Track<Finished>) -> Self::Output {
        state::Uniform::new(slot.prepare(), request::Body::Finished(body))
    }
}
