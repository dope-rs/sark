use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

use dope::launcher::Ctx;
use dope::manifold::env::Bundle;
use dope::manifold::listener::{self, Application, Listener, config};
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::transport::link::Slot;
use dope::transport::wire::{RecvChunk, Wire};
use dope::wire::Identity;
use dope::{Driver, DriverConfig, Executor, manifold};
use dope_extra::Trigger;
use dope_tls::{Endpoint, Tls};
use sark_h2::frame::ParseError;
use sark_h2::{Conn, ConnError, ErrorCode, ServerRole, StreamId, conn};

use crate::Codec;
use crate::frame::{Deframer, MessageFrame};
use crate::headers::{HeaderBlock, RequestHead};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_message_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_message_len: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Cfg {
    pub bind: SocketAddr,
    pub readiness: Option<SocketAddr>,
    pub max_conn: usize,
    pub backlog: i32,
    pub grpc: Config,
}

pub type Env = Bundle<Tcp, Identity, Throughput>;
pub type TlsEnv = Bundle<Tcp, Tls, Throughput>;

fn listener_config(bind: SocketAddr, cfg: &Cfg) -> config::Config<Tcp> {
    config::Config::<Tcp> {
        max_conn: cfg.max_conn,
        bind,
        backlog: cfg.backlog,
        stream_opts: Default::default(),
        listener_opts: dope::transport::config::tcp::ListenerOpts {
            reuseport: dope::transport::config::SocketToggle::Enabled,
            per_ip_cap: Some((cfg.max_conn / 2) as u32),
            ..Default::default()
        },
    }
}

fn driver_config(cfg: &Cfg, ctx: &Ctx) -> dope::DriverCfg {
    <dope::DriverCfg as DriverConfig>::for_tcp_profile::<Throughput>(cfg.max_conn)
        .with_cpu_id(Some(ctx.cpu))
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<0, App<H>, Env>,
}

pub fn serve<H: Handler>(
    handler: H,
    cfg: Cfg,
    ctx: Ctx,
    shutdown: Option<&Trigger>,
) -> io::Result<()> {
    let mut exec = Executor::new(driver_config(&cfg, &ctx))?;
    let drv = exec.driver_mut();
    if let Some(trigger) = shutdown {
        trigger.register(drv);
    }
    let mut app = App::with_config(handler, cfg.grpc.clone());
    app.liveness_fallback = cfg.readiness.is_some();
    let listener = Listener::<0, App<H>, Env>::open_in(app, listener_config(cfg.bind, &cfg), drv)?;
    let mut app = core::pin::pin!(Dispatcher { listener });
    exec.run(app.as_mut())
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct TlsDispatcher<H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<0, App<H, Tls>, TlsEnv>,
    #[pin]
    #[manifold(optional)]
    readiness: Option<Listener<1, liveness::Liveness, Env>>,
}

pub fn serve_tls<H: Handler>(
    handler: H,
    cfg: Cfg,
    tls_cfg: shin::server::Config,
    ctx: Ctx,
    shutdown: Option<&Trigger>,
) -> io::Result<()> {
    let mut exec = Executor::new(driver_config(&cfg, &ctx))?;
    let drv = exec.driver_mut();
    if let Some(trigger) = shutdown {
        trigger.register(drv);
    }
    let mut listener = Listener::<0, App<H, Tls>, TlsEnv>::open_in(
        App::with_config(handler, cfg.grpc.clone()),
        listener_config(cfg.bind, &cfg),
        drv,
    )?;
    listener.set_cfg(Endpoint::Server(Box::new(tls_cfg)));
    let readiness = match cfg.readiness {
        Some(addr) => Some(Listener::<1, liveness::Liveness, Env>::open_in(
            liveness::Liveness,
            listener_config(addr, &cfg),
            drv,
        )?),
        None => None,
    };
    let mut app = core::pin::pin!(TlsDispatcher {
        listener,
        readiness,
    });
    exec.run(app.as_mut())
}

mod liveness {
    use super::{Application, Driver, Identity, RecvChunk, Slot, Wire, listener, manifold};

    const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

    const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    pub fn is_plain_request(first: &[u8]) -> bool {
        let n = first.len().min(H2_PREFACE.len());
        first[..n] != H2_PREFACE[..n]
    }

    pub fn respond<W: Wire, C: Default + 'static>(
        slot: &mut Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        if slot.core.is_send_inflight() {
            return;
        }
        let ud = slot.token();
        let buf = aux.write_buf_for(slot);
        buf[..RESPONSE.len()].copy_from_slice(RESPONSE);
        slot.core.set_close_after();
        slot.submit_buffered(buf, RESPONSE.len(), ud, driver);
    }

    pub struct Liveness;

    impl Application for Liveness {
        type Conn = ();
        type Wire = Identity;

        fn on_chunk(
            &mut self,
            slot: &mut Slot<Identity, listener::State<()>>,
            _chunk: RecvChunk<'_>,
            aux: &mut listener::Aux,
            driver: &mut Driver,
        ) -> manifold::Outcome {
            respond(slot, aux, driver);
            manifold::Outcome::Ok
        }

        fn on_send(
            &mut self,
            _slot: &mut Slot<Identity, listener::State<()>>,
            _sent: usize,
            _aux: &mut listener::Aux,
            _driver: &mut Driver,
        ) {
        }

        fn on_close(
            &mut self,
            _slot: &mut Slot<Identity, listener::State<()>>,
            _aux: &mut listener::Aux,
        ) {
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub stream_id: StreamId,
    pub head: RequestHead,
    pub messages: Vec<MessageFrame>,
    pub trailers: Metadata,
}

#[derive(Clone, Debug)]
pub struct Response {
    pub metadata: Metadata,
    pub messages: Vec<Vec<u8>>,
    pub trailers: Metadata,
    pub status: Status,
}

impl Response {
    pub fn new() -> Self {
        Self {
            metadata: Metadata::new(),
            messages: Vec::new(),
            trailers: Metadata::new(),
            status: Status::ok(),
        }
    }

    pub fn with_status(status: Status) -> Self {
        Self {
            status,
            ..Self::new()
        }
    }

    pub fn push_message(&mut self, payload: Vec<u8>) {
        self.messages.push(payload);
    }
}

impl Default for Response {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Handler: 'static {
    fn on_start(
        &mut self,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let _ = (stream_id, head, reply);
        StreamMode::Buffered
    }

    fn on_message(&mut self, stream_id: StreamId, message: MessageFrame, reply: &mut StreamReply) {
        let _ = (stream_id, message, reply);
    }

    fn on_trailers(&mut self, stream_id: StreamId, trailers: Metadata, reply: &mut StreamReply) {
        let _ = (stream_id, trailers, reply);
    }

    fn on_request(&mut self, request: Request, response: &mut Response);
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StreamMode {
    Buffered,
    Live,
}

#[derive(Clone, Debug)]
pub struct StreamReply {
    pub metadata: Metadata,
    pub messages: Vec<Vec<u8>>,
    pub trailers: Metadata,
    pub status: Option<Status>,
}

impl StreamReply {
    pub fn new() -> Self {
        Self {
            metadata: Metadata::new(),
            messages: Vec::new(),
            trailers: Metadata::new(),
            status: None,
        }
    }

    pub fn finish(status: Status) -> Self {
        Self {
            status: Some(status),
            ..Self::new()
        }
    }

    pub fn push_message(&mut self, payload: Vec<u8>) {
        self.messages.push(payload);
    }

    pub fn finish_with(&mut self, status: Status) {
        self.status = Some(status);
    }

    pub fn apply_live<C: Codec>(&mut self, response: LiveResponse<C::Encode>, codec: &mut C) {
        self.metadata = response.metadata;
        self.trailers = response.trailers;
        self.status = response.status;
        for message in response.messages {
            let mut encoded = Vec::new();
            match codec.encode(&message, &mut encoded) {
                Ok(()) => self.push_message(encoded),
                Err(status) => {
                    self.messages.clear();
                    self.finish_with(status);
                    return;
                }
            }
        }
    }
}

impl Default for StreamReply {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Routes<H: Handler> {
    routes: Vec<Route<H>>,
    active: BTreeMap<StreamId, usize>,
}

struct Route<H: Handler> {
    path: Vec<u8>,
    handler: H,
}

impl<H: Handler> Routes<H> {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            active: BTreeMap::new(),
        }
    }

    pub fn route(mut self, path: &[u8], handler: H) -> Self {
        self.push(path, handler);
        self
    }

    pub fn push(&mut self, path: &[u8], handler: H) {
        self.routes.push(Route {
            path: path.to_vec(),
            handler,
        });
    }
}

impl<H: Handler> Default for Routes<H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: Handler> Handler for Routes<H> {
    fn on_start(
        &mut self,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let Some(route_idx) = self.routes.iter().position(|route| route.path == head.path) else {
            return StreamMode::Buffered;
        };
        let mode = self.routes[route_idx]
            .handler
            .on_start(stream_id, head, reply);
        if mode == StreamMode::Live {
            self.active.insert(stream_id, route_idx);
        }
        mode
    }

    fn on_message(&mut self, stream_id: StreamId, message: MessageFrame, reply: &mut StreamReply) {
        let Some(&route_idx) = self.active.get(&stream_id) else {
            return;
        };
        self.routes[route_idx]
            .handler
            .on_message(stream_id, message, reply);
    }

    fn on_trailers(&mut self, stream_id: StreamId, trailers: Metadata, reply: &mut StreamReply) {
        let Some(route_idx) = self.active.remove(&stream_id) else {
            return;
        };
        self.routes[route_idx]
            .handler
            .on_trailers(stream_id, trailers, reply);
    }

    fn on_request(&mut self, request: Request, response: &mut Response) {
        let Some(route) = self
            .routes
            .iter_mut()
            .find(|route| route.path == request.head.path)
        else {
            response.status = Status::new(Code::Unimplemented, "unknown gRPC method");
            return;
        };
        route.handler.on_request(request, response);
    }
}

#[derive(Clone, Debug)]
pub struct UnaryRequest<T> {
    pub stream_id: StreamId,
    pub head: RequestHead,
    pub message: T,
    pub trailers: Metadata,
}

#[derive(Clone, Debug)]
pub struct UnaryResponse<T> {
    pub metadata: Metadata,
    pub message: Option<T>,
    pub trailers: Metadata,
    pub status: Status,
}

impl<T> UnaryResponse<T> {
    pub fn new(message: T) -> Self {
        Self {
            metadata: Metadata::new(),
            message: Some(message),
            trailers: Metadata::new(),
            status: Status::ok(),
        }
    }

    pub fn empty(status: Status) -> Self {
        Self {
            metadata: Metadata::new(),
            message: None,
            trailers: Metadata::new(),
            status,
        }
    }
}

pub trait UnaryHandler: 'static {
    type Request;
    type Response;
    type Codec: Codec<Decode = Self::Request, Encode = Self::Response>;

    fn on_unary(&mut self, request: UnaryRequest<Self::Request>) -> UnaryResponse<Self::Response>;
}

#[derive(Clone, Debug)]
pub struct StreamingRequest<T> {
    pub stream_id: StreamId,
    pub head: RequestHead,
    pub messages: Vec<T>,
    pub trailers: Metadata,
}

#[derive(Clone, Debug)]
pub struct StreamingResponse<T> {
    pub metadata: Metadata,
    pub messages: Vec<T>,
    pub trailers: Metadata,
    pub status: Status,
}

impl<T> StreamingResponse<T> {
    pub fn new(messages: Vec<T>) -> Self {
        Self {
            metadata: Metadata::new(),
            messages,
            trailers: Metadata::new(),
            status: Status::ok(),
        }
    }

    pub fn empty(status: Status) -> Self {
        Self {
            metadata: Metadata::new(),
            messages: Vec::new(),
            trailers: Metadata::new(),
            status,
        }
    }
}

pub trait StreamingHandler: 'static {
    type Request;
    type Response;
    type Codec: Codec<Decode = Self::Request, Encode = Self::Response>;

    fn on_stream(
        &mut self,
        request: StreamingRequest<Self::Request>,
    ) -> StreamingResponse<Self::Response>;
}

#[derive(Clone, Debug)]
pub struct LiveMessage<T> {
    pub stream_id: StreamId,
    pub message: T,
}

#[derive(Clone, Debug)]
pub struct LiveTrailers {
    pub stream_id: StreamId,
    pub trailers: Metadata,
}

#[derive(Clone, Debug)]
pub struct LiveResponse<T> {
    pub metadata: Metadata,
    pub messages: Vec<T>,
    pub trailers: Metadata,
    pub status: Option<Status>,
}

impl<T> LiveResponse<T> {
    pub fn new() -> Self {
        Self {
            metadata: Metadata::new(),
            messages: Vec::new(),
            trailers: Metadata::new(),
            status: None,
        }
    }

    pub fn message(message: T) -> Self {
        Self {
            messages: vec![message],
            ..Self::new()
        }
    }

    pub fn finish(status: Status) -> Self {
        Self {
            status: Some(status),
            ..Self::new()
        }
    }

    pub fn push_message(&mut self, message: T) {
        self.messages.push(message);
    }

    pub fn finish_with(&mut self, status: Status) {
        self.status = Some(status);
    }
}

impl<T> Default for LiveResponse<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait LiveStreamingHandler: 'static {
    type Request;
    type Response;
    type Codec: Codec<Decode = Self::Request, Encode = Self::Response>;

    fn on_start(
        &mut self,
        stream_id: StreamId,
        head: &RequestHead,
    ) -> LiveResponse<Self::Response> {
        let _ = (stream_id, head);
        LiveResponse::new()
    }

    fn on_message(&mut self, message: LiveMessage<Self::Request>) -> LiveResponse<Self::Response>;

    fn on_trailers(&mut self, trailers: LiveTrailers) -> LiveResponse<Self::Response> {
        let _ = trailers;
        LiveResponse::finish(Status::ok())
    }
}

pub struct Unary<H: UnaryHandler> {
    handler: H,
    codec: H::Codec,
}

pub struct Streaming<H: StreamingHandler> {
    handler: H,
    codec: H::Codec,
}

pub struct LiveStreaming<H: LiveStreamingHandler> {
    handler: H,
    codec: H::Codec,
}

impl<H: UnaryHandler> Unary<H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self { handler, codec }
    }
}

impl<H: StreamingHandler> Streaming<H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self { handler, codec }
    }
}

impl<H: LiveStreamingHandler> LiveStreaming<H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self { handler, codec }
    }
}

impl<H: UnaryHandler> Handler for Unary<H> {
    fn on_request(&mut self, request: Request, response: &mut Response) {
        let [message] = request.messages.as_slice() else {
            response.status = Status::new(Code::InvalidArgument, "unary request needs one message");
            return;
        };
        let decoded = match self.codec.decode(&message.payload) {
            Ok(decoded) => decoded,
            Err(status) => {
                response.status = status;
                return;
            }
        };
        let unary_request = UnaryRequest {
            stream_id: request.stream_id,
            head: request.head,
            message: decoded,
            trailers: request.trailers,
        };
        let unary_response = self.handler.on_unary(unary_request);
        response.metadata = unary_response.metadata;
        response.trailers = unary_response.trailers;
        response.status = unary_response.status;
        if let Some(message) = unary_response.message {
            let mut encoded = Vec::new();
            match self.codec.encode(&message, &mut encoded) {
                Ok(()) => response.push_message(encoded),
                Err(status) => response.status = status,
            }
        }
    }
}

impl<H: StreamingHandler> Handler for Streaming<H> {
    fn on_request(&mut self, request: Request, response: &mut Response) {
        let mut messages = Vec::with_capacity(request.messages.len());
        for message in request.messages {
            match self.codec.decode(&message.payload) {
                Ok(decoded) => messages.push(decoded),
                Err(status) => {
                    response.status = status;
                    return;
                }
            }
        }
        let stream_request = StreamingRequest {
            stream_id: request.stream_id,
            head: request.head,
            messages,
            trailers: request.trailers,
        };
        let stream_response = self.handler.on_stream(stream_request);
        response.metadata = stream_response.metadata;
        response.trailers = stream_response.trailers;
        response.status = stream_response.status;
        for message in stream_response.messages {
            let mut encoded = Vec::new();
            match self.codec.encode(&message, &mut encoded) {
                Ok(()) => response.push_message(encoded),
                Err(status) => {
                    response.status = status;
                    response.messages.clear();
                    return;
                }
            }
        }
    }
}

impl<H: LiveStreamingHandler> Handler for LiveStreaming<H> {
    fn on_start(
        &mut self,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        reply.apply_live(self.handler.on_start(stream_id, head), &mut self.codec);
        StreamMode::Live
    }

    fn on_message(&mut self, stream_id: StreamId, message: MessageFrame, reply: &mut StreamReply) {
        let decoded = match self.codec.decode(&message.payload) {
            Ok(decoded) => decoded,
            Err(status) => {
                reply.finish_with(status);
                return;
            }
        };
        reply.apply_live(
            self.handler.on_message(LiveMessage {
                stream_id,
                message: decoded,
            }),
            &mut self.codec,
        );
    }

    fn on_trailers(&mut self, stream_id: StreamId, trailers: Metadata, reply: &mut StreamReply) {
        reply.apply_live(
            self.handler.on_trailers(LiveTrailers {
                stream_id,
                trailers,
            }),
            &mut self.codec,
        );
    }

    fn on_request(&mut self, _request: Request, response: &mut Response) {
        response.status = Status::new(Code::Internal, "live streaming request was buffered");
    }
}

pub struct ConnState {
    h2: Conn<ServerRole>,
    streams: BTreeMap<StreamId, StreamState>,
    pending: BTreeMap<StreamId, PendingResponse>,
    probed: bool,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            h2: Conn::<ServerRole>::new(),
            streams: BTreeMap::new(),
            pending: BTreeMap::new(),
            probed: false,
        }
    }
}

#[derive(Clone, Debug)]
struct StreamState {
    head: RequestHead,
    deframer: Deframer,
    messages: Vec<MessageFrame>,
    trailers: Metadata,
    mode: StreamMode,
}

#[derive(Clone, Debug)]
struct PendingResponse {
    headers: HeaderBlock,
    body: Vec<u8>,
    trailers: Option<HeaderBlock>,
    headers_sent: bool,
    body_pos: usize,
}

pub struct App<H: Handler, W: Wire = Identity> {
    handler: H,
    config: Config,
    liveness_fallback: bool,
    _wire: ::std::marker::PhantomData<W>,
}

impl<H: Handler, W: Wire> App<H, W> {
    pub fn new(handler: H) -> Self {
        Self::with_config(handler, Config::default())
    }

    pub fn with_config(handler: H, config: Config) -> Self {
        Self {
            handler,
            config,
            liveness_fallback: false,
            _wire: ::std::marker::PhantomData,
        }
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    fn drain_events(&mut self, state: &mut ConnState) {
        while let Some(event) = state.h2.poll_event() {
            self.on_h2_event(state, event);
        }
    }

    fn on_h2_event(&mut self, state: &mut ConnState, event: conn::Event) {
        match event {
            conn::Event::Headers {
                stream_id,
                headers,
                end_stream,
                trailing,
            } if trailing => {
                self.on_trailers(state, stream_id, headers);
                if end_stream {
                    self.finish_stream(state, stream_id);
                }
            }
            conn::Event::Headers {
                stream_id,
                headers,
                end_stream,
                ..
            } => {
                self.on_headers(state, stream_id, headers);
                if end_stream {
                    self.finish_stream(state, stream_id);
                }
            }
            conn::Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                self.on_data(state, stream_id, &data);
                if end_stream {
                    self.finish_stream(state, stream_id);
                }
            }
            conn::Event::StreamReset { stream_id, .. } => {
                state.streams.remove(&stream_id);
            }
            _ => {}
        }
    }

    fn on_headers(
        &mut self,
        state: &mut ConnState,
        stream_id: StreamId,
        headers: Vec<sark_h2::hpack::OwnedHeader>,
    ) {
        let fields = HeaderBlock::from_h2_owned(headers);
        match RequestHead::parse_h2(&fields) {
            Ok(head) => {
                let mut reply = StreamReply::new();
                let mode = self.handler.on_start(stream_id, &head, &mut reply);
                state.streams.insert(
                    stream_id,
                    StreamState {
                        head,
                        deframer: Deframer::new(self.config.max_message_len),
                        messages: Vec::new(),
                        trailers: Metadata::new(),
                        mode,
                    },
                );
                state.enqueue_reply(stream_id, reply);
            }
            Err(status) => {
                state.send_error(stream_id, status);
            }
        }
    }

    fn on_trailers(
        &mut self,
        state: &mut ConnState,
        stream_id: StreamId,
        headers: Vec<sark_h2::hpack::OwnedHeader>,
    ) {
        let Some(stream) = state.streams.get_mut(&stream_id) else {
            return;
        };
        let fields = HeaderBlock::from_h2_owned(headers);
        match RequestHead::parse_h2_trailers(&fields) {
            Ok(metadata) => {
                stream.trailers = metadata;
            }
            Err(status) => {
                state.streams.remove(&stream_id);
                state.send_error(stream_id, status);
            }
        }
    }

    fn on_data(&mut self, state: &mut ConnState, stream_id: StreamId, data: &[u8]) {
        let (mode, messages) = {
            let Some(stream) = state.streams.get_mut(&stream_id) else {
                state.send_error(
                    stream_id,
                    Status::new(Code::Internal, "DATA before gRPC headers"),
                );
                return;
            };
            if let Err(err) = stream.deframer.push(data, &mut stream.messages) {
                state.streams.remove(&stream_id);
                state.send_error(stream_id, Status::from_frame_err(err));
                return;
            }
            (stream.mode, core::mem::take(&mut stream.messages))
        };
        if mode != StreamMode::Live {
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.messages = messages;
            }
            return;
        }
        for message in messages {
            if message.compressed {
                state.streams.remove(&stream_id);
                state.send_error(
                    stream_id,
                    Status::new(Code::Unimplemented, "compressed messages are not supported"),
                );
                return;
            }
            let mut reply = StreamReply::new();
            self.handler.on_message(stream_id, message, &mut reply);
            state.enqueue_reply(stream_id, reply);
        }
        state.drive_pending();
    }

    fn finish_stream(&mut self, state: &mut ConnState, stream_id: StreamId) {
        let Some(stream) = state.streams.remove(&stream_id) else {
            return;
        };
        if stream.mode == StreamMode::Live {
            let mut reply = StreamReply::new();
            self.handler
                .on_trailers(stream_id, stream.trailers, &mut reply);
            if reply.status.is_none() {
                reply.status = Some(Status::ok());
            }
            state.enqueue_reply(stream_id, reply);
            state.drive_pending();
            return;
        }
        if stream.messages.iter().any(|m| m.compressed) {
            state.send_error(
                stream_id,
                Status::new(Code::Unimplemented, "compressed messages are not supported"),
            );
            return;
        }

        let request = Request {
            stream_id,
            head: stream.head,
            messages: stream.messages,
            trailers: stream.trailers,
        };
        let mut response = Response::new();
        self.handler.on_request(request, &mut response);
        state.enqueue_response(stream_id, response);
        state.drive_pending();
    }

    fn flush_into(
        slot: &mut Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        close_after: bool,
    ) {
        if slot.core.is_send_inflight() {
            return;
        }
        let out_is_empty = slot.state.conn.h2.outbound().is_empty();
        if out_is_empty {
            if close_after {
                slot.core.set_close_after();
            }
            return;
        }
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let state = &mut slot.state.conn;
        let out = state.h2.outbound();
        let n = out.len().min(write_buf.len());
        write_buf[..n].copy_from_slice(&out[..n]);
        state.h2.drain_outbound(n);
        let close_now = close_after && state.h2.outbound().is_empty();
        if close_now {
            slot.core.set_close_after();
        }
        slot.submit_buffered(write_buf, n, send_ud, driver);
    }
}

impl<H: Handler, W: Wire> Application for App<H, W> {
    type Conn = ConnState;
    type Wire = W;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> manifold::Outcome {
        let bytes = chunk.as_slice();
        if self.liveness_fallback && !slot.state.conn.probed {
            slot.state.conn.probed = true;
            if liveness::is_plain_request(bytes) {
                liveness::respond(slot, aux, driver);
                return manifold::Outcome::Ok;
            }
        }
        let state = &mut slot.state.conn;
        if state.h2.goaway_sent() || state.h2.goaway_received().is_some() {
            return manifold::Outcome::Ok;
        }
        if let Err(e) = state.h2.ingest(bytes) {
            let code = ConnState::map_conn_err(&e);
            state.h2.goaway(code, b"");
            Self::flush_into(slot, aux, driver, true);
            return manifold::Outcome::Ok;
        }
        self.drain_events(state);
        state.drive_pending();
        let close_after = state.h2.goaway_sent();
        Self::flush_into(slot, aux, driver, close_after);
        manifold::Outcome::Ok
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        let close_after = slot.state.conn.h2.goaway_sent();
        Self::flush_into(slot, aux, driver, close_after);
    }

    fn defer_close(&self, slot: &Slot<W, listener::State<ConnState>>) -> bool {
        !slot.state.conn.h2.outbound().is_empty()
    }

    fn on_wake(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        let close_after = slot.state.conn.h2.goaway_sent();
        Self::flush_into(slot, aux, driver, close_after);
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl ConnState {
    fn enqueue_response(&mut self, stream_id: StreamId, response: Response) {
        let reply = StreamReply {
            metadata: response.metadata,
            messages: response.messages,
            trailers: response.trailers,
            status: Some(response.status),
        };
        self.enqueue_reply(stream_id, reply);
    }

    fn enqueue_reply(&mut self, stream_id: StreamId, reply: StreamReply) {
        if reply.messages.is_empty()
            && reply.status.is_none()
            && reply.metadata.entries().is_empty()
        {
            return;
        }
        let headers = match HeaderBlock::for_response(&reply.metadata) {
            Ok(headers) => headers,
            Err(status) => {
                self.send_error(stream_id, status);
                return;
            }
        };

        let mut body = Vec::new();
        for payload in reply.messages {
            if MessageFrame::encode(false, &payload, &mut body).is_err() {
                self.send_error(
                    stream_id,
                    Status::new(Code::Internal, "response message too large"),
                );
                return;
            }
        }

        let trailers = if let Some(status) = reply.status {
            match HeaderBlock::for_trailers(&status, &reply.trailers) {
                Ok(trailers) => Some(trailers),
                Err(status) => {
                    self.send_error(stream_id, status);
                    return;
                }
            }
        } else {
            None
        };

        match self.pending.get_mut(&stream_id) {
            Some(pending) => {
                pending.body.extend_from_slice(&body);
                if trailers.is_some() {
                    pending.trailers = trailers;
                }
            }
            None => {
                self.pending.insert(
                    stream_id,
                    PendingResponse {
                        headers,
                        body,
                        trailers,
                        headers_sent: false,
                        body_pos: 0,
                    },
                );
            }
        }
    }

    fn drive_pending(&mut self) {
        let ids: Vec<StreamId> = self.pending.keys().copied().collect();
        for stream_id in ids {
            let Some(mut pending) = self.pending.remove(&stream_id) else {
                continue;
            };
            match pending.drive(&mut self.h2, stream_id) {
                Ok(true) => {}
                Ok(false) => {
                    self.pending.insert(stream_id, pending);
                }
                Err(()) => {}
            }
        }
    }

    fn send_error(&mut self, stream_id: StreamId, status: Status) {
        let headers = HeaderBlock::for_response(&Metadata::new()).ok();
        if let Some(headers) = headers {
            let h2_headers = headers.as_h2();
            if self
                .h2
                .send_response(stream_id, h2_headers.iter().copied(), false)
                .is_err()
            {
                let _ = self.h2.reset_stream(stream_id, ErrorCode::InternalError);
                return;
            }
        }
        if let Ok(trailers) = HeaderBlock::for_trailers(&status, &Metadata::new()) {
            let h2_trailers = trailers.as_h2();
            let _ = self.h2.send_trailers(stream_id, &h2_trailers);
        }
    }

    fn map_conn_err(e: &ConnError) -> ErrorCode {
        match e {
            ConnError::BadPreface
            | ConnError::Protocol
            | ConnError::BadStream
            | ConnError::Continuation
            | ConnError::BadSettings
            | ConnError::StreamGoneAway => ErrorCode::ProtocolError,
            ConnError::StreamClosed => ErrorCode::StreamClosed,
            ConnError::ParseError(ParseError::FrameSize)
            | ConnError::ParseError(ParseError::BadLength) => ErrorCode::FrameSize,
            ConnError::ParseError(_) => ErrorCode::ProtocolError,
            ConnError::FlowControl => ErrorCode::FlowControl,
            ConnError::FrameSize => ErrorCode::FrameSize,
            ConnError::Hpack(_) => ErrorCode::Compression,
            ConnError::HeaderListTooLarge | ConnError::Overload => ErrorCode::EnhanceYourCalm,
            ConnError::GoAwayReceived(c) => *c,
            ConnError::StreamLimit => ErrorCode::RefusedStream,
        }
    }
}

impl PendingResponse {
    fn drive(&mut self, conn: &mut Conn<ServerRole>, stream_id: StreamId) -> Result<bool, ()> {
        if !self.headers_sent {
            let h2_headers = self.headers.as_h2();
            conn.send_response(stream_id, h2_headers.iter().copied(), false)
                .map_err(|_| ())?;
            self.headers_sent = true;
        }

        while self.body_pos < self.body.len() {
            let n = conn
                .send_data(stream_id, &self.body[self.body_pos..], false)
                .map_err(|_| ())?;
            if n == 0 {
                return Ok(false);
            }
            self.body_pos += n;
        }

        if let Some(trailers) = &self.trailers {
            let h2_trailers = trailers.as_h2();
            conn.send_trailers(stream_id, &h2_trailers)
                .map_err(|_| ())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
