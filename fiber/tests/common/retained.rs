use std::{cell::Cell, time::Duration};

use dope::core::driver::schedule::clock::timer::Timer;
use dope_fiber::{
    abi::Fiber,
    task::time::sleep::{self, TimerExt as _},
};
use dope_test::harness::observe::VecCellExt as _;
use o3::buffer::{segments, storage};
use sark_http::{payload::response::status, request};

struct Copy;
struct From;
const _: (Copy, From) = (Copy, From);

#[derive(Debug)]
pub enum Failure {
    Timer(sleep::Error),
    Pointer(*const u8, *const u8),
    Pointers(Vec<*const u8>, Vec<*const u8>),
    Count {
        expected: segments::Count,
        actual: segments::Count,
    },
    Segments {
        count: segments::Count,
        first: Option<Vec<u8>>,
        second: Option<Vec<u8>>,
        contiguous: bool,
    },
    Owners([bool; 3]),
    Query(Vec<u8>),
    Padding(Vec<u8>),
    Nested {
        items: usize,
        tag: Option<Vec<u8>>,
    },
}

pub struct State<'a>(pub &'a Cell<Vec<Failure>>);

impl State<'_> {
    pub fn delay<'d>(&self, timer: Timer<'d>, duration: Duration) -> impl Fiber<'d, Output = ()> {
        sark_fiber::fiber! { '_ => async move {
            match timer.sleep(duration) {
                Ok(sleep) => sleep.await,
                Err(error) => self.0.push(Failure::Timer(error)),
            }
        } }
    }

    pub fn pointer(&self, before: *const u8, after: *const u8) {
        if before != after {
            self.0.push(Failure::Pointer(before, after));
        }
    }
}

#[sark_fiber::request(ordered)]
pub struct EchoReq {
    #[path]
    pub id: storage::Shared,
    #[header("x-echo-marker", default = "MISSING")]
    pub req: storage::Shared,
    #[query("q", default = "MISSING")]
    pub __query: storage::Shared,
    #[raw_body]
    pub payload: Shared,
}

#[sark_json::json(ordered, plain)]
pub struct JsonPayload {
    pub value: storage::Shared,
}

#[sark_fiber::request(ordered)]
#[json_body(JsonPayload)]
pub struct JsonReq {}

#[sark_json::json(ordered)]
pub struct Item {
    pub value: storage::Shared,
    #[field(seq)]
    tags: Vec<storage::Shared>,
}

#[sark_json::json(ordered)]
pub struct Catalog {
    #[field(seq, nested)]
    items: Vec<Item>,
}

#[sark_fiber::request(ordered)]
#[json_body(Catalog)]
pub struct NestedReq {}

#[sark_fiber::response(raw)]
pub struct Reply {
    status: status::Code,
    body: Vec<u8>,
}

#[sark_fiber::response(raw)]
pub struct BytesReply<'d> {
    status: status::Code,
    body: request::Bytes<'d>,
}

#[sark_fiber::handler(pub)]
pub(crate) async fn echo_handler<'state>(
    request: EchoReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> Reply {
    state.delay(timer, Duration::from_millis(40)).await;
    let mut body = Vec::new();
    body.extend_from_slice(b"id=");
    body.extend_from_slice(request.id.as_slice());
    body.extend_from_slice(b" marker=");
    body.extend_from_slice(request.req.as_slice());
    body.extend_from_slice(b" payload=");
    body.extend_from_slice(request.payload.as_slice());
    Reply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn echo_bytes<'d, 'state>(
    request: EchoReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> BytesReply<'d> {
    let source = request.payload.as_slice().as_ptr();
    let id = request.payload.id();
    let owners = [
        request.id.id() == id,
        request.req.id() == id,
        request.__query.id() == id,
    ];
    if owners != [true; 3] {
        state.0.push(Failure::Owners(owners));
    }
    if request.__query.as_slice() != b"query" {
        state
            .0
            .push(Failure::Query(request.__query.as_slice().to_vec()));
    }
    state.delay(timer, Duration::from_millis(40)).await;
    let body = request.payload;
    state.pointer(source, body.as_slice().as_ptr());
    BytesReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn json_bytes<'d, 'state>(
    request: JsonReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> BytesReply<'d> {
    let source = request.body.value.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(40)).await;
    let body = request.body.value.into_inner();
    state.pointer(source, body.as_slice().as_ptr());
    BytesReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn nested_handler<'d, 'state>(
    request: NestedReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> Result<BytesReply<'d>, Reply> {
    let tag = request
        .body
        .items
        .get(1)
        .and_then(|item| item.tags.get(1))
        .map(|tag| tag.as_slice());
    if request.body.items.len() != 2 || tag != Some(b"owner".as_slice()) {
        state.0.push(Failure::Nested {
            items: request.body.items.len(),
            tag: tag.map(<[u8]>::to_vec),
        });
    }
    let mut items = request.body.items.into_iter();
    let Some(first) = items.next() else {
        return Err(Reply {
            status: status::Code::INTERNAL_SERVER_ERROR,
            body: Vec::new(),
        });
    };
    let source = first.value.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(40)).await;
    let body = first.value.into_inner();
    drop(first.tags);
    drop(items);
    state.pointer(source, body.as_slice().as_ptr());
    Ok(BytesReply {
        status: status::Code::OK,
        body,
    })
}

sark_fiber::define_route! {
    pub EchoDispatch<'state>: State<'state> => {
        POST "/echo/:id" => request(path, header, query) async(capacity = 32, body = contiguous) echo_handler,
        POST "/retained/:id" => request(path, header, query) async(capacity = 32, body = contiguous) echo_bytes,
        POST "/retained-json" => async(capacity = 32, body = json) json_bytes,
        POST "/retained-json/nested" => async(capacity = 32, body = json) nested_handler,
    }
}

pub struct Echo<'a> {
    id: &'a str,
    marker: &'a str,
    body: &'a str,
}

impl<'a> Echo<'a> {
    pub const fn new(id: &'a str, marker: &'a str, body: &'a str) -> Self {
        Self { id, marker, body }
    }

    pub fn wire(self) -> Vec<u8> {
        let Self { id, marker, body } = self;
        format!(
            "POST /echo/{id} HTTP/1.1\r\nHost: x\r\nx-echo-marker: {marker}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ).into_bytes()
    }

    pub fn expected(self) -> String {
        let Self { id, marker, body } = self;
        format!("id={id} marker={marker} payload={body}")
    }
}

pub fn wire(body: &[u8]) -> Vec<u8> {
    Post {
        path: "/retained/id?q=query",
        headers: "x-echo-marker: marker\r\n",
        body,
    }
    .wire()
}

pub struct Post<'a> {
    path: &'a str,
    headers: &'a str,
    body: &'a [u8],
}

impl<'a> Post<'a> {
    pub const fn new(path: &'a str, body: &'a [u8]) -> Self {
        Self {
            path,
            headers: "",
            body,
        }
    }

    pub fn wire(self) -> Vec<u8> {
        let Self {
            path,
            headers,
            body,
        } = self;
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\n{headers}Content-Length: {}\r\n\r\n",
            body.len(),
        )
        .into_bytes();
        request.extend_from_slice(body);
        request
    }
}

pub const NESTED: &[u8] = br#"{"items":[{"value":"first-owner","tags":["nested","owner"]},{"value":"second-owner","tags":["nested","owner"]}]}"#;
