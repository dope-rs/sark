use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};

use dope::Driver;
use dope::fiber::{Fiber, Slab, TaskId};
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application};
use dope::runtime::profile::Throughput;
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk, Wire};
use o3::buffer::Shared;

use crate::conn::{self, Conn, ConnError};
use crate::frame::ErrorCode;
use crate::hpack::{Header, OwnedHeader};
use crate::role::ServerRole;
use crate::stream::StreamId;
use crate::tuning::Tuning;

type ServerProfile = Throughput;

pub const HANDLER_SLAB_CAP: usize = 256;
const OUTBOUND_SOFT_CAP: usize = 64 * 1024;
const MAX_BODY_LEN: usize = <ServerProfile as Tuning>::MAX_BODY_LEN;
const MAX_CONN_BUFFERED_LEN: usize = <ServerProfile as Tuning>::MAX_CONN_BUFFERED_LEN;

pub struct Request {
    pub headers: Vec<OwnedHeader>,
    pub body: Vec<u8>,
}

pub struct Response {
    pub headers: Vec<OwnedHeader>,
    pub body: Shared,
    pub trailers: Vec<OwnedHeader>,
}

impl Response {
    pub fn new(headers: Vec<OwnedHeader>, body: Shared) -> Self {
        Self {
            headers,
            body,
            trailers: Vec::new(),
        }
    }
}

pub trait Handler: 'static {
    type Fut<'h>: Future<Output = Response> + 'h
    where
        Self: 'h;

    fn on_request<'h>(&'h self, req: Request) -> Fiber<'h, Self::Fut<'h>>;
}

/// A request being assembled across its HEADERS -> DATA* -> trailers event
/// sequence. Buffered until the terminating end_stream so the handler receives
/// the complete header list and body in one `Request`.
struct Incoming {
    headers: Vec<OwnedHeader>,
    body: Vec<u8>,
}

impl From<Incoming> for Request {
    fn from(inc: Incoming) -> Self {
        Request {
            headers: inc.headers,
            body: inc.body,
        }
    }
}

struct PendingBody {
    stream_id: StreamId,
    body: Shared,
    off: usize,
    stalled: bool,
    trailers: Vec<OwnedHeader>,
    trailers_sent: bool,
}

impl PendingBody {
    fn emit_trailers(&mut self, conn: &mut Conn<ServerRole>) -> Result<(), ConnError> {
        if self.trailers.is_empty() || self.trailers_sent {
            return Ok(());
        }
        let fields: Vec<Header<'_>> = self.trailers.iter().map(|h| h.as_ref()).collect();
        conn.send_trailers(self.stream_id, &fields)?;
        self.trailers_sent = true;
        Ok(())
    }

    fn pump(&mut self, conn: &mut Conn<ServerRole>) -> Result<bool, ConnError> {
        loop {
            if self.off >= self.body.len() {
                self.emit_trailers(conn)?;
                return Ok(true);
            }
            if conn.outbound().len() >= OUTBOUND_SOFT_CAP {
                self.stalled = false;
                return Ok(false);
            }
            let rest = &self.body.as_slice()[self.off..];
            let end_stream = self.trailers.is_empty();
            let n = conn.send_data(self.stream_id, rest, end_stream)?;
            if n == 0 {
                self.stalled = true;
                return Ok(false);
            }
            self.off += n;
        }
    }
}

pub struct ConnState {
    pub conn: Conn<ServerRole>,
    incoming: BTreeMap<StreamId, Incoming>,
    pending: Vec<PendingBody>,
    waiting: Vec<(StreamId, TaskId)>,
    buffered_total: usize,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            conn: Conn::<ServerRole>::with_tuning::<ServerProfile>(),
            incoming: BTreeMap::new(),
            pending: Vec::new(),
            waiting: Vec::new(),
            buffered_total: 0,
        }
    }
}

impl ConnState {
    fn take_incoming(&mut self, stream_id: StreamId) -> Option<Incoming> {
        let inc = self.incoming.remove(&stream_id)?;
        self.buffered_total = self.buffered_total.saturating_sub(inc.body.len());
        Some(inc)
    }
}

pub struct App<'h, H: Handler, W: Wire = Identity> {
    user: &'h H,
    slab: Slab<'h, H::Fut<'h>, HANDLER_SLAB_CAP>,
    _wire: PhantomData<W>,
}

impl<'h, H: Handler, W: Wire> App<'h, H, W> {
    pub fn new(user: &'h H) -> Self {
        Self {
            user,
            slab: Slab::new(),
            _wire: PhantomData,
        }
    }

    pub fn handler(&self) -> &H {
        self.user
    }
}

impl<'h, H: Handler, W: Wire> Application for App<'h, H, W> {
    type Conn = ConnState;
    type Wire = W;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let bytes = chunk.as_slice();
        let state = &mut slot.state.conn;
        if state.conn.goaway_sent() || state.conn.goaway_received().is_some() {
            return Outcome::Ok;
        }
        if let Err(e) = state.conn.ingest(bytes) {
            let code = ErrorCode::from(&e);
            state.conn.goaway(code, b"");
            Self::flush_into(slot, aux, driver, true);
            return Outcome::Ok;
        }
        self.drain_events(slot, driver);
        let close_after = slot.state.conn.conn.goaway_sent();
        Self::flush_into(slot, aux, driver, close_after);
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        Self::pump_pending(&mut slot.state.conn);
        let close_after = slot.state.conn.conn.goaway_sent();
        if !slot.state.conn.conn.outbound().is_empty() {
            Self::flush_into(slot, aux, driver, close_after);
        }
    }

    fn on_wake(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        if slot.state.conn.waiting.is_empty() {
            return;
        }
        let waker = slot.make_waker(driver);
        let mut cx = Context::from_waker(&waker);
        let conn_ptr: *mut ConnState = &mut slot.state.conn;
        // SAFETY: conn_ptr aliases slot.state.conn; the poll loop below touches only state.conn / state.waiting / self.slab through conn_ptr and never re-borrows slot.state.conn via slot (slot is used only for make_waker, already dropped), so &mut ConnState and &mut Slot stay disjoint.
        let cstate: &mut ConnState = unsafe { &mut *conn_ptr };
        let mut i = 0;
        while i < cstate.waiting.len() {
            let (stream_id, ref task) = cstate.waiting[i];
            match self.slab.poll(task, &mut cx) {
                Poll::Ready(resp) => {
                    let (_, task) = cstate.waiting.swap_remove(i);
                    self.slab.release(task);
                    Self::begin_response(cstate, stream_id, resp);
                }
                Poll::Pending => i += 1,
            }
        }
        let close_after = cstate.conn.goaway_sent();
        if !cstate.conn.outbound().is_empty() {
            Self::flush_into(slot, aux, driver, close_after);
        }
    }

    fn on_close(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
        for (_, task) in slot.state.conn.waiting.drain(..) {
            self.slab.release(task);
        }
        slot.state.conn.pending.clear();
        slot.state.conn.incoming.clear();
    }
}

impl<'h, H: Handler, W: Wire> App<'h, H, W> {
    fn drain_events(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        driver: &mut Driver,
    ) {
        let conn_ptr: *mut ConnState = &mut slot.state.conn;
        // SAFETY: conn_ptr aliases slot.state.conn; this fn touches slot only via make_waker (which borrows the parker, not state) before re-deriving the waker, and otherwise drives state.conn / self.slab through conn_ptr, keeping &mut ConnState disjoint from any &mut Slot use.
        let cstate: &mut ConnState = unsafe { &mut *conn_ptr };
        let waker = slot.make_waker(driver);
        let mut cx = Context::from_waker(&waker);
        while let Some(ev) = cstate.conn.poll_event() {
            match ev {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } => {
                    if trailing {
                        if let Some(mut inc) = cstate.take_incoming(stream_id) {
                            inc.headers.extend(headers);
                            self.dispatch(cstate, stream_id, inc.into(), &mut cx);
                        }
                    } else if end_stream {
                        let req = Request {
                            headers,
                            body: Vec::new(),
                        };
                        self.dispatch(cstate, stream_id, req, &mut cx);
                    } else {
                        cstate.incoming.insert(
                            stream_id,
                            Incoming {
                                headers,
                                body: Vec::new(),
                            },
                        );
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if end_stream {
                        if let Some(mut inc) = cstate.take_incoming(stream_id) {
                            inc.body.extend_from_slice(&data);
                            self.dispatch(cstate, stream_id, inc.into(), &mut cx);
                        }
                    } else if let Some(inc) = cstate.incoming.get_mut(&stream_id) {
                        inc.body.extend_from_slice(&data);
                        let body_len = inc.body.len();
                        cstate.buffered_total += data.len();
                        if body_len > MAX_BODY_LEN || cstate.buffered_total > MAX_CONN_BUFFERED_LEN
                        {
                            cstate.take_incoming(stream_id);
                            let _ = cstate
                                .conn
                                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                        }
                    }
                }
                conn::Event::StreamReset { stream_id, .. } => {
                    cstate.take_incoming(stream_id);
                    cstate.pending.retain(|p| p.stream_id != stream_id);
                    if let Some(pos) = cstate.waiting.iter().position(|(s, _)| *s == stream_id) {
                        let (_, task) = cstate.waiting.swap_remove(pos);
                        self.slab.release(task);
                    }
                }
                _ => {}
            }
        }
        Self::resume_pending(cstate);
    }

    fn dispatch(
        &mut self,
        cstate: &mut ConnState,
        stream_id: StreamId,
        req: Request,
        cx: &mut Context<'_>,
    ) {
        let fiber = self.user.on_request(req);
        match self.slab.alloc(fiber) {
            Some(task) => match self.slab.poll(&task, cx) {
                Poll::Ready(resp) => {
                    self.slab.release(task);
                    Self::begin_response(cstate, stream_id, resp);
                }
                Poll::Pending => cstate.waiting.push((stream_id, task)),
            },
            None => {
                let _ = cstate
                    .conn
                    .reset_stream(stream_id, ErrorCode::RefusedStream);
            }
        }
    }

    fn begin_response(cstate: &mut ConnState, stream_id: StreamId, resp: Response) {
        if !cstate.conn.has_stream(stream_id) {
            return;
        }
        let has_trailers = !resp.trailers.is_empty();
        let end_stream = resp.body.is_empty() && !has_trailers;
        if cstate
            .conn
            .send_response(
                stream_id,
                resp.headers.iter().map(|h| h.as_ref()),
                end_stream,
            )
            .is_err()
        {
            return;
        }
        if end_stream {
            return;
        }
        let mut body = PendingBody {
            stream_id,
            body: resp.body,
            off: 0,
            stalled: false,
            trailers: resp.trailers,
            trailers_sent: false,
        };
        match body.pump(&mut cstate.conn) {
            Ok(true) => {}
            Ok(false) | Err(_) => cstate.pending.push(body),
        }
    }

    fn resume_pending(cstate: &mut ConnState) {
        if !cstate.conn.take_window_opened() {
            return;
        }
        for body in cstate.pending.iter_mut() {
            body.stalled = false;
        }
        Self::pump_pending(cstate);
    }

    fn pump_pending(cstate: &mut ConnState) {
        let ConnState { conn, pending, .. } = cstate;
        let mut i = 0;
        while i < pending.len() {
            if pending[i].stalled || conn.outbound().len() >= OUTBOUND_SOFT_CAP {
                i += 1;
                continue;
            }
            match pending[i].pump(conn) {
                Ok(true) | Err(_) => {
                    pending.swap_remove(i);
                }
                Ok(false) => i += 1,
            }
        }
    }

    fn flush_into(
        slot: &mut Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        close_after: bool,
    ) {
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let state = &mut slot.state.conn;
        let n = state.conn.drain_into(write_buf);
        if close_after {
            slot.core.set_close_after();
        }
        slot.submit_buffered(write_buf, n, send_ud, driver);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::CLIENT_PREFACE;
    use crate::frame::{self, FrameHeader, WindowUpdate};
    use crate::hpack::{Encoder, Header};

    type TestApp = App<'static, NoopHandler>;

    struct NoopHandler;

    impl Handler for NoopHandler {
        type Fut<'h> = std::future::Ready<Response>;

        fn on_request<'h>(&'h self, _req: Request) -> Fiber<'h, Self::Fut<'h>> {
            Fiber::new(std::future::ready(Response::new(
                Vec::new(),
                Shared::from(Vec::new()),
            )))
        }
    }

    fn open_stream(cstate: &mut ConnState, stream_id: StreamId) {
        cstate.conn.drain_outbound(cstate.conn.outbound().len());
        cstate.conn.ingest(CLIENT_PREFACE).unwrap();
        let mut enc = Encoder::new(4096);
        let mut block = Vec::new();
        enc.encode(
            [
                Header {
                    name: b":method",
                    value: b"GET",
                },
                Header {
                    name: b":scheme",
                    value: b"http",
                },
                Header {
                    name: b":path",
                    value: b"/",
                },
                Header {
                    name: b":authority",
                    value: b"x",
                },
            ],
            &mut block,
        );
        let mut frame = Vec::new();
        crate::frame::Headers {
            stream_id,
            end_stream: true,
            end_headers: true,
            priority: None,
            block_fragment: &block,
        }
        .encode(&mut frame);
        cstate.conn.ingest(&frame).unwrap();
        while cstate.conn.poll_event().is_some() {}
        cstate.conn.drain_outbound(cstate.conn.outbound().len());
    }

    fn ingest_window_update(cstate: &mut ConnState, stream_id: StreamId, increment: u32) {
        let mut bytes = Vec::new();
        WindowUpdate {
            stream_id,
            increment,
        }
        .encode(&mut bytes);
        cstate.conn.ingest(&bytes).unwrap();
        while cstate.conn.poll_event().is_some() {}
        TestApp::resume_pending(cstate);
    }

    fn collect_data(out: &[u8]) -> (Vec<u8>, bool) {
        let mut pos = 0;
        let mut body = Vec::new();
        let mut saw_end = false;
        while pos < out.len() {
            let h = FrameHeader::parse(&out[pos..]).unwrap();
            let total = 9 + h.length as usize;
            if h.kind == frame::Type::Data {
                assert!(!saw_end, "DATA after END_STREAM");
                let payload = &out[pos + 9..pos + total];
                body.extend_from_slice(payload);
                if h.flags.has(frame::Flags::END_STREAM) {
                    saw_end = true;
                }
            }
            pos += total;
        }
        (body, saw_end)
    }

    fn drain_data(cstate: &mut ConnState, acc: &mut Vec<u8>, end: &mut bool) {
        let out = cstate.conn.outbound().to_vec();
        let (body, saw_end) = collect_data(&out);
        acc.extend_from_slice(&body);
        *end |= saw_end;
        cstate.conn.drain_outbound(cstate.conn.outbound().len());
    }

    #[test]
    fn body_larger_than_window_delivered_across_stream_and_conn_updates() {
        let mut cstate = ConnState::default();
        let stream_id = StreamId(1);
        open_stream(&mut cstate, stream_id);

        let total = 200_000usize;
        let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        let resp = Response::new(
            vec![OwnedHeader::new(b":status", b"200")],
            Shared::from(payload.clone()),
        );
        TestApp::begin_response(&mut cstate, stream_id, resp);

        let mut acc = Vec::new();
        let mut end = false;
        drain_data(&mut cstate, &mut acc, &mut end);

        assert!(acc.len() <= 65_535, "stalled at the initial send window");
        assert!(!end, "END_STREAM must not be set before the true end");

        loop {
            if end {
                break;
            }
            ingest_window_update(&mut cstate, StreamId::CONNECTION, 50_000);
            ingest_window_update(&mut cstate, stream_id, 50_000);
            drain_data(&mut cstate, &mut acc, &mut end);
        }

        assert_eq!(acc, payload, "all body bytes delivered intact and in order");
        assert!(end, "END_STREAM seen at the true end");
    }

    #[test]
    fn awaiting_handler_suspends_then_pump_after_ready() {
        use std::cell::Cell;
        use std::pin::Pin;
        use std::task::Poll;

        struct OnceReady {
            polled: Cell<bool>,
            out: Cell<Option<Response>>,
        }

        impl Future for OnceReady {
            type Output = Response;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Response> {
                if self.polled.get() {
                    Poll::Ready(self.out.take().unwrap())
                } else {
                    self.polled.set(true);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let mut cstate = ConnState::default();
        let stream_id = StreamId(1);
        open_stream(&mut cstate, stream_id);

        let resp = Response::new(
            vec![OwnedHeader::new(b":status", b"200")],
            Shared::from(b"deferred".to_vec()),
        );
        let fut = OnceReady {
            polled: Cell::new(false),
            out: Cell::new(Some(resp)),
        };
        let mut slab: Slab<'static, OnceReady, HANDLER_SLAB_CAP> = Slab::new();
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let task = slab.alloc(Fiber::new(fut)).unwrap();
        assert!(matches!(slab.poll(&task, &mut cx), Poll::Pending));
        cstate.waiting.push((stream_id, task));

        let (_, task) = cstate.waiting.pop().unwrap();
        let resp = match slab.poll(&task, &mut cx) {
            Poll::Ready(r) => r,
            Poll::Pending => panic!("expected ready on second poll"),
        };
        slab.release(task);
        TestApp::begin_response(&mut cstate, stream_id, resp);

        let mut acc = Vec::new();
        let mut end = false;
        drain_data(&mut cstate, &mut acc, &mut end);
        assert_eq!(acc, b"deferred");
        assert!(end);
    }

    #[test]
    fn sync_handler_completes_inline_without_suspension() {
        let mut cstate = ConnState::default();
        let mut app: TestApp = App::new(&NoopHandler);
        let stream_id = StreamId(1);
        open_stream(&mut cstate, stream_id);

        let body = Shared::from(b"small body".to_vec());
        let resp = Response::new(vec![OwnedHeader::new(b":status", b"200")], body);
        TestApp::begin_response(&mut cstate, stream_id, resp);

        assert!(
            cstate.waiting.is_empty(),
            "a non-awaiting handler retains nothing"
        );
        assert!(
            cstate.pending.is_empty(),
            "a small body fits the window and leaves no pending pump"
        );
        let _ = &mut app;

        let mut acc = Vec::new();
        let mut end = false;
        drain_data(&mut cstate, &mut acc, &mut end);
        assert_eq!(acc, b"small body");
        assert!(end);
    }

    #[test]
    fn connection_level_update_alone_resumes() {
        let mut cstate = ConnState::default();
        let s1 = StreamId(1);
        open_stream(&mut cstate, s1);

        let total = 120_000usize;
        let payload = vec![7u8; total];
        let resp = Response::new(
            vec![OwnedHeader::new(b":status", b"200")],
            Shared::from(payload.clone()),
        );
        TestApp::begin_response(&mut cstate, s1, resp);

        let mut acc = Vec::new();
        let mut end = false;
        drain_data(&mut cstate, &mut acc, &mut end);
        let first = acc.len();
        assert!(first <= 65_535);
        assert!(!end);

        ingest_window_update(&mut cstate, s1, 200_000);
        drain_data(&mut cstate, &mut acc, &mut end);
        assert_eq!(
            acc.len(),
            first,
            "stream update alone cannot exceed the conn window"
        );
        assert!(!end);

        ingest_window_update(&mut cstate, StreamId::CONNECTION, 200_000);
        drain_data(&mut cstate, &mut acc, &mut end);
        assert_eq!(acc, payload);
        assert!(end);
    }

    fn walk_frames(out: &[u8]) -> (Vec<u8>, bool, usize, bool, bool) {
        let mut pos = 0;
        let mut data = Vec::new();
        let mut data_end_stream = false;
        let mut header_frames = 0usize;
        let mut trailers_end_stream = false;
        let mut trailers_after_data = false;
        while pos < out.len() {
            let h = FrameHeader::parse(&out[pos..]).unwrap();
            let total = 9 + h.length as usize;
            let payload = &out[pos + 9..pos + total];
            match h.kind {
                frame::Type::Data => {
                    data.extend_from_slice(payload);
                    if h.flags.has(frame::Flags::END_STREAM) {
                        data_end_stream = true;
                    }
                }
                frame::Type::Headers => {
                    header_frames += 1;
                    if header_frames == 2 {
                        trailers_after_data = !data.is_empty();
                        if h.flags.has(frame::Flags::END_STREAM) {
                            trailers_end_stream = true;
                        }
                    }
                }
                _ => {}
            }
            pos += total;
        }
        (
            data,
            data_end_stream,
            header_frames,
            trailers_end_stream,
            trailers_after_data,
        )
    }

    #[test]
    fn trailers_emitted_after_body_with_end_stream() {
        let mut cstate = ConnState::default();
        let stream_id = StreamId(1);
        open_stream(&mut cstate, stream_id);

        let mut resp = Response::new(
            vec![OwnedHeader::new(b":status", b"200")],
            Shared::from(b"hello".to_vec()),
        );
        resp.trailers = vec![OwnedHeader::new(b"grpc-status", b"0")];
        TestApp::begin_response(&mut cstate, stream_id, resp);

        let out = cstate.conn.outbound().to_vec();
        let (data, data_end, headers, trailers_end, trailers_after_data) = walk_frames(&out);
        assert_eq!(data, b"hello");
        assert!(
            !data_end,
            "DATA must not carry END_STREAM when trailers follow"
        );
        assert_eq!(headers, 2, "response HEADERS then trailing HEADERS");
        assert!(trailers_end, "trailing HEADERS must carry END_STREAM");
        assert!(trailers_after_data, "trailers follow the DATA frame");
    }

    #[test]
    fn trailers_with_empty_body_emit_without_data() {
        let mut cstate = ConnState::default();
        let stream_id = StreamId(1);
        open_stream(&mut cstate, stream_id);

        let mut resp = Response::new(
            vec![OwnedHeader::new(b":status", b"200")],
            Shared::from(Vec::new()),
        );
        resp.trailers = vec![OwnedHeader::new(b"grpc-status", b"5")];
        TestApp::begin_response(&mut cstate, stream_id, resp);

        let out = cstate.conn.outbound().to_vec();
        let (data, _, headers, trailers_end, _) = walk_frames(&out);
        assert!(data.is_empty(), "no DATA frame for an empty body");
        assert_eq!(headers, 2, "response HEADERS then trailing HEADERS");
        assert!(trailers_end, "trailing HEADERS must carry END_STREAM");
    }
}
