use std::collections::BTreeMap;

use dope_quic::wire::stream::Id;
use o3::mem::Rejected;
use sark_core::{
    body::{
        self,
        admit::{Finished, Open, Track},
    },
    decode::{Decode, exec, request, state, state::Homogeneous as _},
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
    manifold,
    qpack::encoder::{self, Encoder},
    stream::Target,
};
use sark_http::{
    field::{
        self, Entry, Pseudo, Value,
        metadata::section::{self, state::RetainedHead, storage::Block},
    },
    payload::{fixed, owned, response::status::Code},
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

type Plan = <H3Router as Decode>::Plan;

impl H3Router {
    pub fn respond(
        &self,
        server: &mut Connection<'_, Plan>,
        head: RetainedHead<<Plan as section::Policy>::Block, section::state::Selected<Plan>>,
    ) {
        let method = head.control().method();
        let head = self.try_head(head).expect("known route");
        let started = request::Head::try_start(head, 0, Boundary::End)
            .map_err(Rejected::discard)
            .expect("empty GET body")
            .into_state();
        let admitted = state::Admission::<state::Admitted>::admit(started);
        let request =
            H3RouterRequestFamily::fold(admitted, Complete).expect("GET body must be complete");
        let request = request
            .try_map_body(|done| body::Body::try_new(done, owned::Segments::new()))
            .map_err(|failure| failure.discard().discard())
            .expect("finished body");
        request.with_input(|prepared, input| {
            let (response, owner) = self
                .try_project(prepared, input, |scoped| {
                    let (request, slot) = scoped.into_parts();
                    let encoder = manifold::encoder::Encoder::from_slot(
                        server,
                        Target::new(Id::ZERO),
                        method,
                        slot,
                    );
                    self.try_dispatch(request, encoder)
                        .map(|response| response.detach())
                        .map_err(|(error, target)| (error.narrow(), target.stream_id().get()))
                })
                .map_err(Rejected::discard)
                .expect("matching route projection");
            response
                .expect("synchronous dispatch")
                .finish_with(owner)
                .expect("atomic H3 response");
        });
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

struct Complete;

impl<P> exec::Fold<P, state::Admitted, exec::Sync> for Complete {
    type Output = Result<request::Admitted<exec::Sync, P, Track<Finished>>, body::Error>;

    fn open<S: state::Slot<P>>(self, _: S, _: Track<Open>) -> Self::Output {
        Err(body::Error::InputIncomplete)
    }

    fn finished<S: state::Slot<P>>(self, slot: S, body: Track<Finished>) -> Self::Output {
        Ok(slot.admit(body))
    }
}
