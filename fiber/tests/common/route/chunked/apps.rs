use std::time::Duration;

use dope::core::driver::schedule::clock::timer::Timer;
use dope_test::harness::observe::VecCellExt as _;
use o3::{
    buffer::{segments, storage},
    mem::rc,
};
use sark_core::request::{BindError, Head};
use sark_fiber::request::BodyLen;
use sark_http::{
    payload::response::{Body as _, status},
    request::{self, Bytes, body},
};

use crate::common::retained::{Failure, State};

#[sark_fiber::request]
pub struct SegmentedReq {
    #[raw_body]
    pub payload: owned::Segments,
    #[body_len]
    pub len: BodyLen,
}

#[sark_fiber::request]
pub struct WholeReq {
    #[raw_body]
    pub payload: Shared,
}

#[derive(Debug)]
pub enum WholeError {
    Layout(request::Error),
    Storage(storage::AllocError),
    Bind(BindError),
    Retain(rc::Error),
}

impl<'d> WholeReq<'d> {
    pub fn try_builder(id: request::Id<'d>) -> Result<WholeReqTaskBuilder<'d>, WholeError> {
        use sark_core::{
            request::raw,
            route::{Input, Own as _},
        };
        let head = try_head()?;
        // SAFETY: WholeReq has no path or header fields, so both field sets are empty.
        let parsed = unsafe { raw::Parsed::<_, WholeReq>::new(head, (), Default::default()) };
        WholeReq::start_stream(parsed.with_id(id)).map_err(|error| match error {
            Input::Params(error) | Input::Body(error) => match error {},
            Input::Retain(error) => WholeError::Retain(error),
        })
    }
}

impl<'d> SegmentedReq<'d> {
    pub fn try_builder(id: request::Id<'d>) -> Result<SegmentedReqTaskBuilder<'d>, WholeError> {
        use sark_core::{
            request::raw,
            route::{Input, Own as _},
        };
        let head = try_head()?;
        // SAFETY: SegmentedReq has no path or header fields, so both field sets are empty.
        let parsed = unsafe { raw::Parsed::<_, SegmentedReq>::new(head, (), Default::default()) };
        SegmentedReq::start_stream(parsed.with_id(id)).map_err(|error| match error {
            Input::Params(error) | Input::Body(error) => match error {},
            Input::Retain(error) => WholeError::Retain(error),
        })
    }
}

fn try_head() -> Result<Head<storage::Shared>, WholeError> {
    use sark_http::request::{Form, Layout, Target, span::Span};

    let path = Span::checked(0, 1)
        .map_err(request::Error::Span)
        .map_err(WholeError::Layout)?;
    let target = Target::from_span(Form::Path, path).map_err(WholeError::Layout)?;
    Head::bind(
        storage::Shared::try_copy(b"/").map_err(WholeError::Storage)?,
        Layout::new(target, None, Some(path)),
    )
    .map_err(WholeError::Bind)
}

#[sark_json::json(ordered)]
pub struct JsonPayload {
    pub value: storage::Shared,
}

#[sark_fiber::request(ordered)]
#[json_body(JsonPayload)]
pub struct JsonReq {}

#[sark_fiber::request]
pub struct DiscardReq {
    #[body_len]
    pub len: BodyLen,
}

#[sark_fiber::response(raw)]
pub struct Borrowed<'req, 'd> {
    pub status: status::Code,
    pub body: body::Ref<'req, 'd>,
}

#[sark_fiber::response(raw)]
pub struct OwnedReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::response(raw)]
pub struct HeldReply<'d> {
    pub status: status::Code,
    pub body: Bytes<'d>,
}

#[sark_fiber::handler(pub)]
fn sync_segments<'state>(request: SegmentedReq, state: &State<'state>) -> Borrowed<'req, 'd> {
    if request.payload.count() != segments::Count::TWO
        || request.payload.segment(0) != Some(b"abc".as_slice())
        || request.payload.segment(1) != Some(b"defg".as_slice())
        || request.payload.as_slice().is_some()
    {
        state.0.push(Failure::Segments {
            count: request.payload.count(),
            first: request.payload.segment(0).map(<[u8]>::to_vec),
            second: request.payload.segment(1).map(<[u8]>::to_vec),
            contiguous: request.payload.as_slice().is_some(),
        });
    }
    Borrowed {
        status: status::Code::OK,
        body: request.payload,
    }
}

#[sark_fiber::handler(pub)]
async fn async_segments<'state>(
    request: SegmentedReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> OwnedReply {
    if request.payload.count() != segments::Count::new(3) {
        state.0.push(Failure::Count {
            expected: segments::Count::new(3),
            actual: request.payload.count(),
        });
    }
    let pointers = request
        .payload
        .segments()
        .map(|segment| segment.as_ptr())
        .collect::<Vec<_>>();
    state.delay(timer, Duration::from_millis(15)).await;
    let after = request
        .payload
        .segments()
        .map(|segment| segment.as_ptr())
        .collect::<Vec<_>>();
    if pointers != after {
        state.0.push(Failure::Pointers(pointers, after));
    }
    let mut body = Vec::new();
    for segment in request.payload.segments() {
        body.extend_from_slice(segment);
    }
    OwnedReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::handler(pub)]
async fn async_whole<'state>(
    request: WholeReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> OwnedReply {
    let pointer = request.payload.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(15)).await;
    state.pointer(pointer, request.payload.as_slice().as_ptr());
    OwnedReply {
        status: status::Code::OK,
        body: request.payload.as_slice().to_vec(),
    }
}

#[sark_fiber::handler(pub)]
async fn async_json<'d, 'state>(
    request: JsonReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> HeldReply<'d> {
    let pointer = request.body.value.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(15)).await;
    state.pointer(pointer, request.body.value.as_slice().as_ptr());
    HeldReply {
        status: status::Code::OK,
        body: request.body.value.into_inner(),
    }
}

#[sark_fiber::handler(pub)]
async fn async_discard<'state>(
    request: DiscardReq,
    state: &State<'state>,
    timer: Timer<'_>,
) -> OwnedReply {
    state.delay(timer, Duration::from_millis(5)).await;
    OwnedReply {
        status: status::Code::OK,
        body: request.len.len().to_string().into_bytes(),
    }
}

#[sark_fiber::handler(pub)]
fn discard(request: DiscardReq) -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: request.len.len().to_string().into_bytes(),
    }
}

#[sark_fiber::handler(pub)]
#[max_body(super::wire::LARGE_BODY_LEN)]
fn large_discard(_request: DiscardReq) -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: vec![b'x'; super::wire::LARGE_BODY_LEN],
    }
}

#[sark_fiber::handler(pub)]
fn fixed_echo(request: WholeReq) -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: request.payload.to_vec(),
    }
}

#[sark_fiber::handler(pub)]
#[max_body(5)]
fn limited(request: DiscardReq) -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: request.len.len().to_string().into_bytes(),
    }
}

#[sark_fiber::handler(pub)]
fn ping() -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: b"pong".to_vec(),
    }
}

sark_fiber::define_route! {
    pub ChunkedDispatch<'state>: State<'state> => {
        POST "/sync" => sync_segments,
        POST "/async/segments" => async(capacity = 8, body = segmented) async_segments,
        POST "/async/contiguous" => async(capacity = 8, body = contiguous) async_whole,
        POST "/async/json" => async(capacity = 8, body = json) async_json,
        POST "/async/discard" => async(capacity = 8, body = discarded) async_discard,
        POST "/discard" => discard,
        POST "/discard/large" => large_discard,
        POST "/expect" => fixed_echo,
        POST "/limited" => limited,
        GET "/ping" => ping,
    }
}
