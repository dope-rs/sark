use std::{cell, mem, ops::ControlFlow, rc, task::Poll, time::Duration};

use dope::core::driver::{
    schedule,
    schedule::{clock::timer::Timer, ready::wake},
};
use dope_fiber::{
    abi,
    abi::poll_fn,
    task::time::sleep::{Sleep, TimerExt as _},
};
use dope_quic::{
    conn::{scope, stream::Error},
    errors::Code,
    stream::Recv,
    wire::stream::Id,
};
use dope_test::{fibers::Yield, harness::observe::VecCellExt as _};
use o3::buffer::{
    resident::{self, bound},
    segments, storage,
};
use sark::{Guard, Head};
use sark_core::{duplex, duplex::Action};
use sark_h3::frame;
use sark_http::{
    method::Method,
    payload::{
        fixed, owned,
        response::{
            dispatch::{
                Stream,
                chunk::{self, Chunk},
            },
            status,
        },
    },
    request,
    request::body,
};

use crate::common::route::body::{Chunks, Trace};
use crate::common::server::h3::proof::{
    Failure, Failures, Metric, SEGMENTED_FIRST, SEGMENTED_SECOND,
    request::{
        BodyReq, EmptyReq, JsonReq, LargeJsonReq, NamedPathReq, SegmentReq, StreamReq, TreeReq,
    },
};

pub(crate) const SATURATED_ROUTE_REQUESTS: usize = schedule::LIMIT;

pub const FAIR_REQUESTS: usize = SATURATED_ROUTE_REQUESTS + 1;

pub(crate) fn stop_stream<'d, const DOMAIN: u8, B: Recv>(
    conn: &mut scope::Temporal<'_, 'd, DOMAIN, B, resident::Shared<'d>>,
    id: Id,
    code: frame::ErrorCode,
) -> Result<(), Error> {
    let code = Code::from_u16(code.wire_code());
    let mut streams = conn.streams();
    let stream = streams.view().resolve_recv(id)?;
    streams.receive().stop(stream, code)
}

pub(crate) fn cancel_stream<'d, const DOMAIN: u8, B: Recv>(
    conn: &mut scope::Temporal<'_, 'd, DOMAIN, B, resident::Shared<'d>>,
    id: Id,
    code: frame::ErrorCode,
) -> Result<(), Error> {
    let code = Code::from_u16(code.wire_code());
    let mut streams = conn.streams();
    let send = streams.view().resolve_send(id)?;
    let receive = streams.view().resolve_recv(id)?;
    streams.send().reset(send, code)?;
    streams.receive().stop(receive, code)
}

pub(crate) struct State<'d> {
    pub(crate) admitted: cell::Cell<usize>,
    pub(crate) waiters: cell::Cell<Vec<wake::Waker<'d>>>,
    pub(crate) saved: cell::Cell<Option<request::Bytes<'d>>>,
    pub(crate) cancel_dropped: rc::Rc<cell::Cell<usize>>,
    pub(crate) errors: rc::Rc<Failures>,
    pub(crate) stream: rc::Rc<Trace>,
}

impl State<'_> {
    fn fail<'d>(&self, error: Failure) -> FairReply<'d> {
        self.errors.record(error);
        FairReply {
            status: status::Code::INTERNAL_SERVER_ERROR,
            body: fixed::Body::new(b"fixture failure"),
        }
    }

    fn sleep<'d>(&self, timer: Timer<'d>, duration: Duration) -> Result<Sleep<'d>, FairReply<'d>> {
        timer
            .sleep(duration)
            .map_err(|error| self.fail(Failure::Sleep(error)))
    }

    fn preserved<'d>(&self, before: *const u8, after: *const u8) -> Result<(), FairReply<'d>> {
        if before != after {
            return Err(self.fail(Failure::Owner {
                before: before as usize,
                after: after as usize,
            }));
        }
        Ok(())
    }

    fn segments<'d>(&self, payload: &body::Ref<'_, 'd>) -> Result<[*const u8; 2], FairReply<'d>> {
        if payload.count() != segments::Count::TWO {
            return Err(self.fail(Failure::Counter(
                Metric::Segments,
                payload.count().into_raw() as usize,
                2,
            )));
        }
        let pointers = [payload.segment(0), payload.segment(1)];
        match pointers {
            [Some(first), Some(second)]
                if first == SEGMENTED_FIRST && second == SEGMENTED_SECOND =>
            {
                Ok([first.as_ptr(), second.as_ptr()])
            }
            actual => Err(self.fail(Failure::Segments(
                actual.map(|value| value.map(<[u8]>::to_vec)),
            ))),
        }
    }
}

#[sark_fiber::response(raw)]
#[header("x-sark-e2e", "owner")]
pub(crate) struct Reply<'req> {
    pub(crate) status: status::Code,
    pub(crate) body: &'req [u8],
}

#[sark_fiber::response(raw)]
#[header("x-sark-e2e", "owner")]
pub(crate) struct SegmentReply<'req, 'd> {
    pub(crate) status: status::Code,
    pub(crate) body: body::Ref<'req, 'd>,
}

#[sark_fiber::response(raw)]
pub(crate) struct RawReply<'d> {
    pub(crate) status: status::Code,
    pub(crate) body: request::Bytes<'d>,
}

#[sark_fiber::response(raw)]
pub(crate) struct FairReply<'d> {
    pub(crate) status: status::Code,
    pub(crate) body: fixed::Body<'d>,
}

pub(crate) struct Deny;

impl<'state> Guard<State<'state>> for Deny {
    type Egress<'d> = bound::AtMost<0>;

    type Break<'req, 'd>
        = FairReply<'d>
    where
        'd: 'req,
        State<'state>: 'req;

    fn check<'req, 'd>(
        head: Head<'req>,
        state: &'req State<'state>,
    ) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
        State<'state>: 'req,
    {
        if !matches!(head.method(), &Method::POST | &Method::HEAD)
            || head.path() != Some(&b"/guarded"[..])
        {
            return ControlFlow::Break(state.fail(Failure::Guard {
                method: head.method().as_bytes().to_vec(),
                path: head.path().map(<[u8]>::to_vec),
            }));
        }
        ControlFlow::Break(FairReply {
            status: status::Code::UNAUTHORIZED,
            body: fixed::Body::new(b"denied"),
        })
    }
}

#[sark_json::json(try_encode)]
pub(crate) struct JsonOutput<'d> {
    pub(crate) value: request::Bytes<'d>,
}

#[sark_fiber::response(json)]
#[header("content-type", "application/json")]
pub(crate) struct JsonReply<'d> {
    pub(crate) status: status::Code,
    pub(crate) body: JsonOutput<'d>,
}

#[sark_fiber::handler]
pub(crate) fn body_h(req: BodyReq) -> Reply<'req> {
    Reply {
        status: status::Code::OK,
        body: req.payload,
    }
}

#[sark_fiber::handler]
pub(crate) fn segmented_h(
    req: SegmentReq,
    state: &State<'_>,
) -> Result<SegmentReply<'req, 'd>, FairReply<'d>> {
    state.segments(&req.payload)?;
    if req.payload.as_slice().is_some() {
        return Err(state.fail(Failure::Coalesced));
    }
    Ok(SegmentReply {
        status: status::Code::OK,
        body: req.payload,
    })
}

#[sark_fiber::handler]
pub(crate) async fn async_raw_h<'d>(
    req: BodyReq,
    state: &State<'_>,
    timer: Timer<'_>,
) -> Result<RawReply<'d>, FairReply<'d>> {
    let ingress = req.payload.as_slice().as_ptr();
    state.sleep(timer, Duration::from_millis(10))?.await;
    state.preserved(ingress, req.payload.as_slice().as_ptr())?;
    Ok(RawReply {
        status: status::Code::OK,
        body: req.payload,
    })
}

#[derive(Debug)]
pub enum StreamError<'d> {
    Empty(chunk::Empty<'d>),
    Allocation(o3::alloc::Exhausted),
    Read(&'static str),
}

#[sark_fiber::handler]
pub(crate) async fn stream_h<'d>(
    req: StreamReq,
    state: &State<'_>,
) -> Stream<'static, Chunks<Result<Chunk<'d>, StreamError<'d>>>> {
    let before = req.payload.as_slice().as_ptr();
    Yield::new().await;
    let payload = match state
        .preserved(before, req.payload.as_slice().as_ptr())
        .and_then(|()| {
            if req.kind.as_slice() == b"resident" {
                req.payload
                    .project(0..9)
                    .map_err(|rejected| state.fail(Failure::Range(rejected.discard())))
            } else {
                Ok(req.payload)
            }
        }) {
        Ok(payload) => payload,
        Err(error) => return Stream::new(Chunks::new([], None)).with_status(error.status),
    };
    let body = payload.into_body().into_inner();
    state.stream.resident.set(body.charge().resident_bytes);
    let two = req.kind.as_slice() == b"two_park";
    let chunks = body
        .into_iter()
        .map(|part| Chunk::try_new(resident::Shared::from(part)).map_err(StreamError::Empty))
        .chain((0..if two { 2 } else { 0 }).map(|_| {
            let mut body = Vec::with_capacity(8192);
            body.resize(697, b'x');
            let body = storage::Shared::try_from(&mut body).map_err(StreamError::Allocation)?;
            Chunk::try_new(resident::Shared::from(body)).map_err(StreamError::Empty)
        }))
        .chain((req.kind.as_slice() == b"fail").then_some(Err(StreamError::Read("read-failed"))));
    Stream::new(
        Chunks::new(chunks, Some(rc::Rc::clone(&state.stream)))
            .park(req.kind.as_slice() == b"park" || two),
    )
}

pub(crate) struct QpackReply<'d> {
    headers: QpackFields,
    body: Chunks<Result<Chunk<'d>, StreamError<'d>>>,
}

pub(crate) struct QpackFields(rc::Rc<Trace>);

impl QpackFields {
    #[dope_fiber::fiber_fn('d)]
    async fn ready<'d>(state: &State<'d>) -> Self {
        while state.stream.polls.get() < 4 {
            Yield::new().await;
        }
        Self(rc::Rc::clone(&state.stream))
    }
}

impl sark_http::payload::response::headers::Source for QpackFields {
    fn emit<'field>(
        &'field self,
        sink: &mut impl sark_http::payload::response::headers::Sink<'field>,
    ) {
        self.0.heads.set(self.0.heads.get() + 1);
        for _ in 0..2 {
            sink.field(
                sark_fiber::header_name!("x-qpack"),
                sark_http::field::literal!(sark_http::field::Value::try_bytes(&[b'q'; 600])),
            );
        }
    }
}

impl<'req, 'd> sark_http::payload::response::dispatch::IntoOutput<'req, 'd> for QpackReply<'d> {
    type Output = Self;

    fn into_output(self) -> Self {
        self
    }
}

impl<'req, 'd> sark_http::payload::response::dispatch::Emit<'req, 'd> for QpackReply<'d> {
    type Kind = sark_http::payload::response::dispatch::Polled;
    type Stream = Chunks<Result<Chunk<'d>, StreamError<'d>>>;
    type Bound = bound::Unbounded;

    const BODY: sark_http::payload::Kind = sark_http::payload::Kind::Stream;

    fn emit<S: sark_http::payload::response::dispatch::Sink<'d>>(
        self,
        sink: S,
    ) -> Result<S::Stream<Self::Stream>, S::Error> {
        sink.stream(status::Code::OK, self.headers, self.body)
    }
}

#[sark_fiber::handler]
pub(crate) async fn qpack_h<'d>(req: BodyReq, state: &State<'_>) -> QpackReply<'d> {
    let headers = QpackFields::ready(state).await;
    let parts = req
        .payload
        .into_body()
        .into_inner()
        .into_iter()
        .map(|part| Chunk::try_new(resident::Shared::from(part)).map_err(StreamError::Empty));
    QpackReply {
        headers,
        body: Chunks::new(parts, Some(rc::Rc::clone(&state.stream))),
    }
}

#[sark_fiber::handler]
pub(crate) async fn qpack_fixed_h<'d>(
    req: BodyReq,
    state: &State<'_>,
) -> sark_http::payload::response::fixed::Direct<QpackFields, request::Bytes<'d>> {
    sark_http::payload::response::fixed::Direct::new(
        status::Code::OK,
        QpackFields::ready(state).await,
        req.payload,
    )
}

#[sark_fiber::handler]
pub(crate) async fn save_raw_h(
    req: BodyReq,
    state: &State<'_>,
) -> Result<FairReply<'_>, FairReply<'d>> {
    if state.saved.replace(Some(req.payload)).is_some() {
        return Err(state.fail(Failure::SavedOccupied));
    }
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"saved"),
    })
}

#[sark_fiber::handler]
pub(crate) async fn replay_raw_h(state: &State<'_>) -> Result<RawReply<'_>, FairReply<'d>> {
    let body = state
        .saved
        .take()
        .ok_or_else(|| state.fail(Failure::SavedMissing))?;
    Ok(RawReply {
        status: status::Code::OK,
        body,
    })
}

#[sark_fiber::handler]
pub(crate) async fn async_json_h<'d>(
    req: JsonReq,
    state: &State<'_>,
    timer: Timer<'_>,
) -> Result<JsonReply<'d>, FairReply<'d>> {
    let ingress = req.body.value.as_slice().as_ptr();
    state.sleep(timer, Duration::from_millis(10))?.await;
    state.preserved(ingress, req.body.value.as_slice().as_ptr())?;
    Ok(JsonReply {
        status: status::Code::OK,
        body: JsonOutput {
            value: req.body.value.into(),
        },
    })
}

#[sark_fiber::handler]
pub(crate) async fn large_json_h(
    req: LargeJsonReq,
    state: &State<'_>,
) -> Result<FairReply<'d>, FairReply<'d>> {
    if req.body.value != 1 {
        return Err(state.fail(Failure::Value(req.body.value)));
    }
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"large"),
    })
}

#[sark_fiber::handler]
pub(crate) async fn nested_json_h(
    req: TreeReq,
    state: &State<'_>,
    timer: Timer<'_>,
) -> Result<FairReply<'d>, FairReply<'d>> {
    let [first, second] = req.body.items.as_slice() else {
        return Err(state.fail(Failure::Counter(
            Metric::NestedItems,
            req.body.items.len(),
            2,
        )));
    };
    if second.id != 2 {
        return Err(state.fail(Failure::Value(second.id)));
    }
    let tag = req.body.featured.tags.get(1).map(|tag| tag.as_slice());
    if tag != Some(&b"owner"[..]) {
        return Err(state.fail(Failure::Tag(tag.map(<[u8]>::to_vec))));
    }
    let ingress = first.value.as_slice().as_ptr();
    state.sleep(timer, Duration::from_millis(10))?.await;
    state.preserved(ingress, first.value.as_slice().as_ptr())?;
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"nested"),
    })
}

#[sark_fiber::handler]
pub(crate) async fn quick_h() -> FairReply<'d> {
    FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"quick"),
    }
}

pub(crate) struct CancelGuard<'a>(&'a cell::Cell<usize>);

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[sark_fiber::handler]
pub(crate) async fn cancel_h(
    state: &State<'_>,
    timer: Timer<'_>,
) -> Result<FairReply<'_>, FairReply<'d>> {
    let _guard = CancelGuard(&state.cancel_dropped);
    state.sleep(timer, Duration::from_secs(30))?.await;
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"unexpected"),
    })
}

#[sark_fiber::handler]
pub(crate) async fn async_segmented_h(
    req: SegmentReq,
    state: &State<'_>,
    timer: Timer<'_>,
) -> Result<FairReply<'d>, FairReply<'d>> {
    let [first, second] = state.segments(&(&*req.payload).into())?;
    state.sleep(timer, Duration::from_millis(10))?.await;
    let [left, right] = state.segments(&(&*req.payload).into())?;
    state.preserved(first, left)?;
    state.preserved(second, right)?;
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"segmented"),
    })
}

pub(crate) fn fairness_gate<'state, 'd: 'state>(
    state: &'state State<'d>,
) -> impl abi::Fiber<'d, Output = ()> + 'state {
    let mut admitted = false;
    poll_fn(move |cx| {
        if !admitted {
            state.waiters.push(cx.as_ref().completion());
            let count = state.admitted.get() + 1;
            state.admitted.set(count);
            admitted = true;
            if count == FAIR_REQUESTS {
                for waiter in state.waiters.take() {
                    waiter.wake();
                }
            }
        }
        if state.admitted.get() == FAIR_REQUESTS {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
}

#[sark_fiber::handler]
pub(crate) async fn fair_a_h(state: &State<'_>) -> FairReply<'_> {
    fairness_gate(state).await;
    FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"a"),
    }
}

#[sark_fiber::handler]
pub(crate) async fn fair_b_h(state: &State<'_>) -> FairReply<'_> {
    fairness_gate(state).await;
    FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"b"),
    }
}

#[sark_fiber::handler]
pub(crate) fn named_path_h(
    req: NamedPathReq,
    state: &State<'_>,
) -> Result<FairReply<'d>, FairReply<'d>> {
    if req.team_id != b"alpha" || req.user_id != b"beta" {
        return Err(state.fail(Failure::Route {
            team: req.team_id.to_vec(),
            user: req.user_id.to_vec(),
        }));
    }
    Ok(FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"named-h3"),
    })
}

#[sark_fiber::handler]
pub(crate) fn guarded_h() -> FairReply<'d> {
    FairReply {
        status: status::Code::OK,
        body: fixed::Body::new(b"unreachable"),
    }
}

pub const DUPLEX_INITIAL: &[u8] = &[0x62; 68];

pub(crate) struct DuplexEcho {
    finished: bool,
    initial: Option<&'static [u8]>,
    errors: rc::Rc<Failures>,
}

impl<'d> duplex::Session<'d> for DuplexEcho {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        _: &mut [u8],
        _: &mut Vec<u8>,
    ) -> (usize, duplex::Action<'d>) {
        if self.finished {
            self.errors.record(Failure::DuplexInput);
            return (0, Action::close());
        }
        (0, Action::send(input))
    }

    fn finish(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        if mem::replace(&mut self.finished, true) {
            self.errors.record(Failure::DuplexFinish);
        }
        (0, Action::close())
    }

    fn activate(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        if self.finished {
            self.errors.record(Failure::DuplexActivate);
            return (0, Action::close());
        }
        match self.initial.take() {
            Some(bytes) => (
                0,
                Action::send(owned::Segments::from(resident::Shared::from(bytes))),
            ),
            None => (0, Action::idle()),
        }
    }
}

#[sark_fiber::duplex]
pub(crate) fn duplex_h(_: EmptyReq, state: &State<'_>) -> DuplexEcho {
    DuplexEcho {
        finished: false,
        initial: None,
        errors: rc::Rc::clone(&state.errors),
    }
}

#[sark_fiber::duplex]
pub(crate) fn duplex_start_h(_: EmptyReq, state: &State<'_>) -> DuplexEcho {
    DuplexEcho {
        finished: false,
        initial: Some(DUPLEX_INITIAL),
        errors: rc::Rc::clone(&state.errors),
    }
}

sark_fiber::define_task_route! {
pub(crate) SrvApp: State<'_> => {
        POST "/body" => body_h,
        HEAD "/body" => body_h,
        POST "/segmented" => segmented_h,
        POST "/async/raw" => async(capacity = 4, body = contiguous) async_raw_h,
        HEAD "/async/raw" => async(capacity = 4, body = contiguous) async_raw_h,
        POST "/async/save" => async(capacity = 1, body = contiguous) save_raw_h,
        POST "/async/replay" => async(capacity = 1, body = discarded) replay_raw_h,
        POST "/async/json" => async(capacity = 4, body = json) async_json_h,
        POST "/async/json/large" => async(capacity = 4, body = json) large_json_h,
        POST "/async/json/nested" => async(capacity = 4, body = json) nested_json_h,
        POST "/async/quick" => async(capacity = 4, body = discarded) quick_h,
        POST "/async/cancel" => async(capacity = 4, body = discarded) cancel_h,
        POST "/async/segmented" => async(capacity = 4, body = segmented) async_segmented_h,
        POST "/users/:user_id/teams/:team_id" => request(path) named_path_h,
        POST "/fair/a" => async(capacity = 300, body = discarded) fair_a_h,
        POST "/fair/b" => async(capacity = 300, body = discarded) fair_b_h,
        scope "" guard (Deny,) => [
            POST "/guarded" => guarded_h,
            HEAD "/guarded" => guarded_h,
        ],
        CONNECT "*" => duplex_h,
        CONNECT("sark-echo") "/duplex" => duplex_h,
        CONNECT("sark-start") "/duplex/start" => duplex_start_h,
        POST "/async/stream/:kind" => request(path) async stream(capacity = 1, body = contiguous) stream_h,
        HEAD "/async/stream/:kind" => request(path) async stream(capacity = 1, body = contiguous) stream_h,
        POST "/async/qpack" => async stream(capacity = 1, body = contiguous) qpack_h,
        POST "/async/qpack/fixed" => async(capacity = 1, body = contiguous) qpack_fixed_h,
    }
}
