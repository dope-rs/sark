use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use dope::manifold::env::Bundle;
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::profile::Throughput;
use dope::runtime::{Executor, ShutdownTrigger, WorkerContext};
use dope::{DriverContext, manifold};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use dope_net::{tcp, tcp::Tcp};
use dope_tls::tls::{Endpoint, Tls};
use o3::buffer::{RetainBytes, SharedPool};
use o3::collections::{FixedHashTable, FixedQueue, Slab, SlabKey};
use sark_core::identity_mut;
use sark_h2::tuning::Tuning;
use sark_h2::{Conn, ErrorCode, ServerRole, StreamId, conn};

use crate::Codec;
use crate::frame::{DataChunk, Deframer, MessageFrame};
use crate::headers::{HeaderBlock, RequestHead};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_in_flight: usize,
    pub max_message_len: usize,
    pub max_fragmented_messages: usize,
    pub max_buffered_len: usize,
    pub max_buffered_msgs: usize,
    pub max_conn_buffered_len: usize,
    pub max_pending_replies: usize,
    pub max_pending_len: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            max_message_len: 4 * 1024 * 1024,
            max_fragmented_messages: 4,
            max_buffered_len: <Throughput as Tuning>::MAX_BODY_LEN,
            max_buffered_msgs: 8192,
            max_conn_buffered_len: <Throughput as Tuning>::MAX_CONN_BUFFERED_LEN,
            max_pending_replies: 8192,
            max_pending_len: <Throughput as Tuning>::MAX_CONN_BUFFERED_LEN,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: SocketAddr,
    pub readiness: Option<SocketAddr>,
    pub max_connections: usize,
    pub backlog: i32,
    pub grpc: Limits,
}

pub type Env = Bundle<Tcp, Identity, Throughput>;
pub type TlsEnv = Bundle<Tcp, Tls, Throughput>;

fn listener_config(bind: SocketAddr, cfg: &Config) -> listener::Config<Tcp> {
    listener::Config::<Tcp> {
        max_connections: cfg.max_connections,
        bind,
        backlog: cfg.backlog,
        stream: Default::default(),
        transport: tcp::listener::Config {
            reuse_port: true,
            per_ip_limit: Some((cfg.max_connections / 2) as u32),
            ..Default::default()
        },
        egress: Default::default(),
    }
}

fn driver_config(cfg: &Config) -> dope::driver::Config {
    dope::driver::Config::for_tcp_profile::<Throughput>(cfg.max_connections)
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d, H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, App<H>, Env>,
}

pub fn serve<H: Handler>(
    handler: H,
    cfg: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    let exec = Executor::with_seed(driver_config(&cfg), context.seed())?;
    exec.enter(|mut sess| {
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let mut app = App::with_config(handler, cfg.grpc.clone());
        app.liveness_fallback = cfg.readiness.is_some();
        let listener = {
            let mut driver = sess.driver_access();
            if let Some(trigger) = shutdown {
                trigger.try_register(&mut driver)?;
            }
            Listener::<0, App<H>, Env>::open_in(
                app,
                listener_config(cfg.bind, &cfg),
                hash_builder,
                &mut driver,
            )?
        };
        let app = core::pin::pin!(o3::cell::BrandCell::new(Dispatcher { listener }));
        sess.run(app.as_ref())
    })
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct TlsDispatcher<'d, H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, App<H, Tls>, TlsEnv>,
    #[pin]
    #[manifold(optional)]
    readiness: Option<Listener<'d, 1, liveness::Liveness, Env>>,
}

pub fn serve_tls<H: Handler>(
    handler: H,
    cfg: Config,
    tls_cfg: shin::server::Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    let exec = Executor::with_seed(driver_config(&cfg), context.seed())?;
    exec.enter(|mut sess| {
        let accept_hash = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let readiness_hash = sess.seed().derive(dope::hash::domain::ACCEPT ^ 1).state();
        let (listener, readiness) = {
            let mut driver = sess.driver_access();
            if let Some(trigger) = shutdown {
                trigger.try_register(&mut driver)?;
            }
            let mut listener = Listener::<0, App<H, Tls>, TlsEnv>::open_in(
                App::with_config(handler, cfg.grpc.clone()),
                listener_config(cfg.bind, &cfg),
                accept_hash,
                &mut driver,
            )?;
            listener.set_config(Endpoint::Server(Box::new(tls_cfg)));
            let readiness = match cfg.readiness {
                Some(addr) => Some(Listener::<1, liveness::Liveness, Env>::open_in(
                    liveness::Liveness,
                    listener_config(addr, &cfg),
                    readiness_hash,
                    &mut driver,
                )?),
                None => None,
            };
            (listener, readiness)
        };
        let app = core::pin::pin!(o3::cell::BrandCell::new(TlsDispatcher {
            listener,
            readiness,
        }));
        sess.run(app.as_ref())
    })
}

mod liveness {
    use super::{
        Application, DriverContext, Identity, Pin, RetainBytes, Slot, Wire, listener, manifold,
    };

    const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

    pub const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    pub fn is_h2_preface_prefix(buf: &[u8]) -> bool {
        let n = buf.len().min(H2_PREFACE.len());
        buf[..n] == H2_PREFACE[..n]
    }

    pub fn is_plain_request(first: &[u8]) -> bool {
        !is_h2_preface_prefix(first)
    }

    pub fn respond<'d, W: Wire, C: Default + 'static>(
        slot: &mut Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        if slot.is_send_inflight() {
            return;
        }
        let ud = slot.token();
        let mut buf = aux.write_buf_for(slot);
        buf[..RESPONSE.len()].copy_from_slice(RESPONSE);
        slot.set_close_after();
        dope::manifold::listener::SlotEgress::submit_buffered(
            slot,
            buf,
            RESPONSE.len(),
            ud,
            driver,
        );
    }

    pub struct Liveness;

    impl<'d> Application<'d> for Liveness {
        type Conn = ();
        type Wire = Identity;

        fn chunk<R: RetainBytes>(
            self: Pin<&mut Self>,
            slot: &mut Slot<'d, Identity, listener::State<()>>,
            _chunk: R,
            aux: &mut listener::Aux,
            driver: &mut DriverContext<'_, 'd>,
        ) -> manifold::Outcome {
            respond(slot, aux, driver);
            manifold::Outcome::Ok
        }

        fn send(
            self: Pin<&mut Self>,
            _slot: &mut Slot<'d, Identity, listener::State<()>>,
            _sent: usize,
            _aux: &mut listener::Aux,
            _driver: &mut DriverContext<'_, 'd>,
        ) {
        }

        fn close(
            self: Pin<&mut Self>,
            _slot: &mut Slot<'d, Identity, listener::State<()>>,
            _aux: &mut listener::Aux,
        ) {
        }
    }
}

pub use liveness::{H2_PREFACE, is_h2_preface_prefix, is_plain_request};

pub struct MessageList<'a> {
    repr: MessageListRepr<'a>,
    len: usize,
}

enum MessageListRepr<'a> {
    Chain {
        nodes: &'a Slab<MessageNode, MessageNodeTag>,
        next: Option<MessageNodeKey>,
    },
    Queue(&'a FixedQueue<MessageFrame>),
}

impl MessageList<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> MessageIter<'_> {
        let repr = match &self.repr {
            MessageListRepr::Chain { nodes, next } => MessageIterRepr::Chain { nodes, next: *next },
            MessageListRepr::Queue(queue) => MessageIterRepr::Queue { queue, index: 0 },
        };
        MessageIter { repr }
    }
}

impl<'a> IntoIterator for &'a MessageList<'_> {
    type Item = &'a MessageFrame;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct MessageIter<'a> {
    repr: MessageIterRepr<'a>,
}

enum MessageIterRepr<'a> {
    Chain {
        nodes: &'a Slab<MessageNode, MessageNodeTag>,
        next: Option<MessageNodeKey>,
    },
    Queue {
        queue: &'a FixedQueue<MessageFrame>,
        index: usize,
    },
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = &'a MessageFrame;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.repr {
            MessageIterRepr::Chain { nodes, next } => {
                let node = nodes.get((*next)?)?;
                *next = node.next;
                Some(&node.message)
            }
            MessageIterRepr::Queue { queue, index } => {
                let message = queue.get(*index)?;
                *index += 1;
                Some(message)
            }
        }
    }
}

pub struct Request<'a> {
    pub stream_id: StreamId,
    pub head: RequestHead,
    pub messages: MessageList<'a>,
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

    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), Status> {
        encode_frames(&self.messages, out)
    }
}

fn encode_frames(messages: &[Vec<u8>], out: &mut Vec<u8>) -> Result<(), Status> {
    out.reserve(messages.iter().map(|p| p.len() + 5).sum());
    for payload in messages {
        MessageFrame::encode(false, payload, out)
            .map_err(|_| Status::new(Code::Internal, "response message too large"))?;
    }
    Ok(())
}

fn compression_unsupported() -> Status {
    Status::new(Code::Unimplemented, "compressed messages are not supported")
}

impl Default for Response {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct StreamRoutes {
    map: FixedHashTable<StreamRoute>,
}

#[derive(Clone, Debug)]
struct StreamRoute {
    stream_id: StreamId,
    route: usize,
}

impl StreamRoutes {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: FixedHashTable::with_capacity(capacity),
        }
    }

    fn bind(&mut self, stream_id: StreamId, route: usize) {
        if self
            .map
            .try_insert(
                u64::from(stream_id.0),
                StreamRoute { stream_id, route },
                |entry| entry.stream_id == stream_id,
            )
            .is_err()
        {
            unreachable!();
        }
    }

    fn route(&self, stream_id: StreamId) -> Option<usize> {
        self.map
            .get(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
            .map(|entry| entry.route)
    }

    fn release(&mut self, stream_id: StreamId) -> Option<usize> {
        self.map
            .remove(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
            .map(|entry| entry.route)
    }
}

impl Default for StreamRoutes {
    fn default() -> Self {
        Self::with_capacity(Limits::default().max_in_flight)
    }
}

pub trait Handler: 'static {
    fn start(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let _ = (routes, stream_id, head, reply);
        StreamMode::Buffered
    }

    fn message(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        message: MessageFrame,
        reply: &mut StreamReply,
    ) {
        let _ = (routes, stream_id, message, reply);
    }

    fn trailers(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        trailers: Metadata,
        reply: &mut StreamReply,
    ) {
        let _ = (routes, stream_id, trailers, reply);
    }

    fn request(&mut self, request: Request<'_>, response: &mut Response);
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
}

struct Route<H: Handler> {
    path: Vec<u8>,
    handler: H,
}

impl<H: Handler> Routes<H> {
    pub fn new() -> Self {
        Self { routes: Vec::new() }
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
    fn start(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let Some(route_idx) = self.routes.iter().position(|route| route.path == head.path) else {
            return StreamMode::Buffered;
        };
        let mode = self.routes[route_idx]
            .handler
            .start(routes, stream_id, head, reply);
        if mode == StreamMode::Live {
            routes.bind(stream_id, route_idx);
        }
        mode
    }

    fn message(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        message: MessageFrame,
        reply: &mut StreamReply,
    ) {
        let Some(route_idx) = routes.route(stream_id) else {
            return;
        };
        self.routes[route_idx]
            .handler
            .message(routes, stream_id, message, reply);
    }

    fn trailers(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        trailers: Metadata,
        reply: &mut StreamReply,
    ) {
        let Some(route_idx) = routes.release(stream_id) else {
            return;
        };
        self.routes[route_idx]
            .handler
            .trailers(routes, stream_id, trailers, reply);
    }

    fn request(&mut self, request: Request<'_>, response: &mut Response) {
        let Some(route) = self
            .routes
            .iter_mut()
            .find(|route| route.path == request.head.path)
        else {
            response.status = Status::new(Code::Unimplemented, "unknown gRPC method");
            return;
        };
        route.handler.request(request, response);
    }
}

fn dispatch_request(
    handler: &mut (impl Handler + ?Sized),
    stream_id: StreamId,
    head: RequestHead,
    messages: MessageList<'_>,
    trailers: Metadata,
) -> Result<Response, Status> {
    if messages.iter().any(|message| message.compressed) {
        return Err(compression_unsupported());
    }
    let mut response = Response::new();
    handler.request(
        Request {
            stream_id,
            head,
            messages,
            trailers,
        },
        &mut response,
    );
    Ok(response)
}

pub fn dispatch_buffered(
    handler: &mut (impl Handler + ?Sized),
    head: RequestHead,
    body: &[u8],
    config: &Limits,
) -> Response {
    let mut deframer = Deframer::new(config.max_message_len);
    let input_pool = SharedPool::new(1, body.len().max(1));
    let mut lease = input_pool.try_acquire().unwrap();
    if lease.spare_writer().try_extend_from_slice(body).is_err() {
        return Response::with_status(Status::new(Code::ResourceExhausted, "request is too large"));
    }
    let mut input = DataChunk::from_pooled(lease.freeze());
    let fragment_pool = SharedPool::new(1, config.max_message_len.max(1));
    let capacity = config
        .max_buffered_msgs
        .min(body.len() / 5 + usize::from(!body.is_empty()));
    let mut queue = FixedQueue::with_capacity(capacity);
    while !input.is_empty() {
        match deframer.next(&mut input, &fragment_pool) {
            Ok(Some(message)) => {
                if queue.push_back(message).is_err() {
                    return Response::with_status(Status::new(
                        Code::ResourceExhausted,
                        "request message buffer is full",
                    ));
                }
            }
            Ok(None) => continue,
            Err(error) => return Response::with_status(Status::from_frame_err(error)),
        }
    }
    let len = queue.len();
    dispatch_request(
        handler,
        StreamId(0),
        head,
        MessageList {
            repr: MessageListRepr::Queue(&queue),
            len,
        },
        Metadata::new(),
    )
    .unwrap_or_else(Response::with_status)
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

    fn unary(&mut self, request: UnaryRequest<Self::Request>) -> UnaryResponse<Self::Response>;
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

    fn stream(
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

    fn start(&mut self, stream_id: StreamId, head: &RequestHead) -> LiveResponse<Self::Response> {
        let _ = (stream_id, head);
        LiveResponse::new()
    }

    fn message(&mut self, message: LiveMessage<Self::Request>) -> LiveResponse<Self::Response>;

    fn trailers(&mut self, trailers: LiveTrailers) -> LiveResponse<Self::Response> {
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
    fn request(&mut self, request: Request<'_>, response: &mut Response) {
        let mut messages = request.messages.iter();
        let Some(message) = messages.next() else {
            response.status = Status::new(Code::InvalidArgument, "unary request needs one message");
            return;
        };
        if messages.next().is_some() {
            response.status = Status::new(Code::InvalidArgument, "unary request needs one message");
            return;
        }
        let decoded = match self.codec.decode(message.payload.as_slice()) {
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
        let unary_response = self.handler.unary(unary_request);
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
    fn request(&mut self, request: Request<'_>, response: &mut Response) {
        let mut messages = Vec::with_capacity(request.messages.len());
        for message in request.messages.iter() {
            match self.codec.decode(message.payload.as_slice()) {
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
        let stream_response = self.handler.stream(stream_request);
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
    fn start(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let _ = routes;
        reply.apply_live(self.handler.start(stream_id, head), &mut self.codec);
        StreamMode::Live
    }

    fn message(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        message: MessageFrame,
        reply: &mut StreamReply,
    ) {
        let _ = routes;
        let decoded = match self.codec.decode(message.payload.as_slice()) {
            Ok(decoded) => decoded,
            Err(status) => {
                reply.finish_with(status);
                return;
            }
        };
        reply.apply_live(
            self.handler.message(LiveMessage {
                stream_id,
                message: decoded,
            }),
            &mut self.codec,
        );
    }

    fn trailers(
        &mut self,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        trailers: Metadata,
        reply: &mut StreamReply,
    ) {
        let _ = routes;
        reply.apply_live(
            self.handler.trailers(LiveTrailers {
                stream_id,
                trailers,
            }),
            &mut self.codec,
        );
    }

    fn request(&mut self, _request: Request<'_>, response: &mut Response) {
        response.status = Status::new(Code::Internal, "live streaming request was buffered");
    }
}

pub struct ConnState {
    h2: Conn<ServerRole>,
    calls: FixedHashTable<CallRecord>,
    pending: FixedQueue<StreamId>,
    replies: Slab<ReplyBatch, ReplyBatchTag>,
    messages: Slab<MessageNode, MessageNodeTag>,
    pending_len: usize,
    pending_capacity: usize,
    live_routes: StreamRoutes,
    message_pool: SharedPool,
    buffered_total: usize,
    probed: bool,
}

impl Default for ConnState {
    fn default() -> Self {
        Self::with_limits(&Limits::default())
    }
}

impl ConnState {
    fn with_limits(limits: &Limits) -> Self {
        let capacity = limits.max_in_flight;
        let h2 = Conn::<ServerRole>::with_config(conn::Config {
            stream_capacity: capacity,
            ..conn::Config::default()
        });
        Self {
            h2,
            calls: FixedHashTable::with_capacity(capacity),
            pending: FixedQueue::with_capacity(capacity),
            replies: Slab::with_capacity(limits.max_pending_replies),
            messages: Slab::with_capacity(limits.max_buffered_msgs),
            pending_len: 0,
            pending_capacity: limits.max_pending_len,
            live_routes: StreamRoutes::with_capacity(capacity),
            message_pool: SharedPool::new(
                limits.max_fragmented_messages,
                limits.max_message_len.max(1),
            ),
            buffered_total: 0,
            probed: false,
        }
    }
}

struct CallRecord {
    stream_id: StreamId,
    stream: Option<StreamState>,
    pending: Option<PendingResponse>,
    queued: bool,
}

struct StreamState {
    head: RequestHead,
    deframer: Deframer,
    message_head: Option<MessageNodeKey>,
    message_tail: Option<MessageNodeKey>,
    message_count: usize,
    trailers: Metadata,
    mode: StreamMode,
    buffered_len: usize,
}

enum MessageNodeTag {}

type MessageNodeKey = SlabKey<MessageNodeTag>;

struct MessageNode {
    message: MessageFrame,
    next: Option<MessageNodeKey>,
}

#[derive(Clone, Debug)]
struct PendingResponse {
    headers: HeaderBlock,
    head: Option<ReplyBatchKey>,
    tail: Option<ReplyBatchKey>,
    trailers: Option<HeaderBlock>,
    headers_sent: bool,
    message_pos: usize,
    frame_pos: usize,
}

enum ReplyBatchTag {}

type ReplyBatchKey = SlabKey<ReplyBatchTag>;

struct ReplyBatch {
    messages: Vec<Vec<u8>>,
    next: Option<ReplyBatchKey>,
}

#[pin_project::pin_project]
pub struct App<H: Handler, W: Wire = Identity> {
    handler: H,
    config: Limits,
    liveness_fallback: bool,
    _wire: ::std::marker::PhantomData<W>,
}

impl<H: Handler, W: Wire> App<H, W> {
    pub fn new(handler: H) -> Self {
        Self::with_config(handler, Limits::default())
    }

    pub fn with_config(handler: H, config: Limits) -> Self {
        assert!(config.max_in_flight > 0, "max_in_flight must be positive");
        assert!(
            config.max_pending_replies > 0,
            "max_pending_replies must be positive"
        );
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

    fn drain_events(&mut self, state: &mut ConnState) -> usize {
        let mut drained = 0;
        while let Some(event) = state.h2.poll_event() {
            drained += 1;
            self.h2_event(state, event);
        }
        drained
    }

    fn h2_event(&mut self, state: &mut ConnState, event: conn::Event) {
        match event {
            conn::Event::Headers {
                stream_id,
                headers,
                end_stream,
                trailing,
            } if trailing => {
                self.trailers(state, stream_id, headers);
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
                self.headers(state, stream_id, headers);
                if end_stream {
                    self.finish_stream(state, stream_id);
                }
            }
            conn::Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                self.data(state, stream_id, data);
                if end_stream {
                    self.finish_stream(state, stream_id);
                }
            }
            conn::Event::StreamReset { stream_id, .. } => {
                state.remove_stream(stream_id);
                state.live_routes.release(stream_id);
            }
            _ => {}
        }
    }

    fn headers(
        &mut self,
        state: &mut ConnState,
        stream_id: StreamId,
        headers: sark_h2::hpack::HeaderBlock,
    ) {
        let fields = HeaderBlock::from_h2(&headers);
        match RequestHead::parse_h2(&fields) {
            Ok(head) => {
                let mut reply = StreamReply::new();
                let mode = self
                    .handler
                    .start(&mut state.live_routes, stream_id, &head, &mut reply);
                if !state.insert_stream(
                    stream_id,
                    StreamState {
                        head,
                        deframer: Deframer::new(self.config.max_message_len),
                        message_head: None,
                        message_tail: None,
                        message_count: 0,
                        trailers: Metadata::new(),
                        mode,
                        buffered_len: 0,
                    },
                ) {
                    state.live_routes.release(stream_id);
                    state.send_error(
                        stream_id,
                        Status::new(Code::ResourceExhausted, "too many in-flight streams"),
                    );
                    return;
                }
                state.enqueue_reply(stream_id, reply);
            }
            Err(status) => {
                state.send_error(stream_id, status);
            }
        }
    }

    fn trailers(
        &mut self,
        state: &mut ConnState,
        stream_id: StreamId,
        headers: sark_h2::hpack::HeaderBlock,
    ) {
        let fields = HeaderBlock::from_h2(&headers);
        match RequestHead::parse_h2_trailers(&fields) {
            Ok(metadata) => {
                if let Some(stream) = state.stream_mut(stream_id) {
                    stream.trailers = metadata;
                }
            }
            Err(status) => {
                state.remove_stream(stream_id);
                state.send_error(stream_id, status);
            }
        }
    }

    fn data(
        &mut self,
        state: &mut ConnState,
        stream_id: StreamId,
        data: sark_h2::conn::DataPayload,
    ) {
        let mut data = DataChunk::new(data);
        while !data.is_empty() {
            let (next, mode) = {
                let Some(stream) = state
                    .calls
                    .get_mut(ConnState::call_hash(stream_id), |call| {
                        call.stream_id == stream_id
                    })
                    .and_then(|call| call.stream.as_mut())
                else {
                    state.send_error(
                        stream_id,
                        Status::new(Code::Internal, "DATA before gRPC headers"),
                    );
                    return;
                };
                (
                    stream.deframer.next(&mut data, &state.message_pool),
                    stream.mode,
                )
            };
            let message = match next {
                Ok(next) => next,
                Err(err) => {
                    state.remove_stream(stream_id);
                    state.send_error(stream_id, Status::from_frame_err(err));
                    return;
                }
            };
            let Some(message) = message else {
                continue;
            };
            if mode == StreamMode::Live {
                if message.compressed {
                    state.remove_stream(stream_id);
                    state.send_error(stream_id, compression_unsupported());
                    return;
                }
                let mut reply = StreamReply::new();
                self.handler
                    .message(&mut state.live_routes, stream_id, message, &mut reply);
                state.enqueue_reply(stream_id, reply);
                state.drive_pending();
                continue;
            }
            let added = message.payload.len();
            let over_limit = {
                let stream = state.stream_mut(stream_id).unwrap();
                stream.buffered_len.saturating_add(added) > self.config.max_buffered_len
                    || stream.message_count == self.config.max_buffered_msgs
            };
            if over_limit
                || state.buffered_total.saturating_add(added) > self.config.max_conn_buffered_len
                || state.push_message(stream_id, message).is_err()
            {
                state.remove_stream(stream_id);
                state.send_error(
                    stream_id,
                    Status::new(Code::ResourceExhausted, "stream buffer limit exceeded"),
                );
                return;
            }
            state.buffered_total += added;
            state.stream_mut(stream_id).unwrap().buffered_len += added;
        }
        state.drive_pending();
    }

    fn finish_stream(&mut self, state: &mut ConnState, stream_id: StreamId) {
        let Some(stream) = state.take_stream(stream_id) else {
            return;
        };
        if stream.mode == StreamMode::Live {
            let mut reply = StreamReply::new();
            self.handler.trailers(
                &mut state.live_routes,
                stream_id,
                stream.trailers,
                &mut reply,
            );
            if reply.status.is_none() {
                reply.status = Some(Status::ok());
            }
            state.enqueue_reply(stream_id, reply);
            state.drive_pending();
            return;
        }
        let message_head = stream.message_head;
        let messages = MessageList {
            repr: MessageListRepr::Chain {
                nodes: &state.messages,
                next: message_head,
            },
            len: stream.message_count,
        };
        let response = dispatch_request(
            &mut self.handler,
            stream_id,
            stream.head,
            messages,
            stream.trailers,
        );
        state.clear_messages(message_head);
        match response {
            Ok(response) => {
                state.enqueue_response(stream_id, response);
                state.drive_pending();
            }
            Err(status) => state.send_error(stream_id, status),
        }
    }

    fn flush_into_proj<'d, C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
        slot: &mut Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        close_after: bool,
        project: &P,
    ) {
        if slot.is_send_inflight() {
            return;
        }
        let out_is_empty = project(&mut slot.state.conn).h2.outbound().is_empty();
        if out_is_empty {
            if close_after {
                slot.set_close_after();
            }
            return;
        }
        let send_ud = slot.token();
        let mut write_buf = aux.write_buf_for(slot);
        let state = project(&mut slot.state.conn);
        let n = state.h2.drain_into(&mut write_buf);
        let close_now = close_after && state.h2.outbound().is_empty();
        if close_now {
            slot.set_close_after();
        }
        dope::manifold::listener::SlotEgress::submit_buffered(slot, write_buf, n, send_ud, driver);
    }

    pub fn chunk_proj<'d, C: Default + 'static, R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<C>>,
        project: impl Fn(&mut C) -> &mut ConnState,
        chunk: R,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> manifold::Outcome {
        let bytes = chunk.as_slice();
        if self.liveness_fallback && !project(&mut slot.state.conn).probed {
            project(&mut slot.state.conn).probed = true;
            if liveness::is_plain_request(bytes) {
                liveness::respond(slot, aux, driver);
                return manifold::Outcome::Ok;
            }
        }
        let error = {
            let state = project(&mut slot.state.conn);
            if state.h2.goaway_sent() || state.h2.goaway_received().is_some() {
                return manifold::Outcome::Ok;
            }
            let mut result = state.h2.ingest(bytes);
            loop {
                let drained = self.drain_events(state);
                state.drive_pending();
                match result {
                    Ok(()) => break None,
                    Err(conn::ConnError::Overload) if drained != 0 => {
                        result = state.h2.resume();
                    }
                    Err(error) => break Some(error),
                }
            }
        };
        if let Some(error) = error {
            let state = project(&mut slot.state.conn);
            let code = ErrorCode::from(&error);
            let _ = state.h2.goaway(code, b"");
            Self::flush_into_proj(slot, aux, driver, true, &project);
            return manifold::Outcome::Ok;
        }
        let close_after = project(&mut slot.state.conn).h2.goaway_sent();
        Self::flush_into_proj(slot, aux, driver, close_after, &project);
        manifold::Outcome::Ok
    }

    pub fn send_proj<'d, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<C>>,
        _sent: usize,
        project: impl Fn(&mut C) -> &mut ConnState,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let close_after = project(&mut slot.state.conn).h2.goaway_sent();
        Self::flush_into_proj(slot, aux, driver, close_after, &project);
    }

    pub fn activate_proj<'d, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<C>>,
        project: impl Fn(&mut C) -> &mut ConnState,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let close_after = project(&mut slot.state.conn).h2.goaway_sent();
        Self::flush_into_proj(slot, aux, driver, close_after, &project);
    }
}

impl<'d, H: Handler, W: Wire> Application<'d> for App<H, W> {
    type Conn = ConnState;
    type Wire = W;

    fn connection(self: Pin<&Self>) -> Self::Conn {
        ConnState::with_limits(&self.get_ref().config)
    }

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        chunk: R,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> manifold::Outcome {
        self.get_mut()
            .chunk_proj(slot, identity_mut, chunk, aux, driver)
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.get_mut()
            .send_proj(slot, sent, identity_mut, aux, driver)
    }

    fn defer_close(self: Pin<&Self>, slot: &Slot<'d, W, listener::State<ConnState>>) -> bool {
        !slot.state.conn.h2.outbound().is_empty()
    }

    fn activate(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.get_mut()
            .activate_proj(slot, identity_mut, aux, driver)
    }

    fn close(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl ConnState {
    fn call_hash(stream_id: StreamId) -> u64 {
        u64::from(stream_id.0)
    }

    fn call_mut(&mut self, stream_id: StreamId) -> Option<&mut CallRecord> {
        self.calls.get_mut(Self::call_hash(stream_id), |call| {
            call.stream_id == stream_id
        })
    }

    fn stream_mut(&mut self, stream_id: StreamId) -> Option<&mut StreamState> {
        self.call_mut(stream_id)?.stream.as_mut()
    }

    fn insert_stream(&mut self, stream_id: StreamId, stream: StreamState) -> bool {
        self.calls
            .try_insert(
                Self::call_hash(stream_id),
                CallRecord {
                    stream_id,
                    stream: Some(stream),
                    pending: None,
                    queued: false,
                },
                |call| call.stream_id == stream_id,
            )
            .is_ok()
    }

    fn push_message(
        &mut self,
        stream_id: StreamId,
        message: MessageFrame,
    ) -> Result<(), MessageFrame> {
        let Some((tail, count)) = self
            .stream_mut(stream_id)
            .map(|stream| (stream.message_tail, stream.message_count))
        else {
            return Err(message);
        };
        let key = match self.messages.insert(MessageNode {
            message,
            next: None,
        }) {
            Ok(key) => key,
            Err(node) => return Err(node.message),
        };
        if let Some(tail) = tail {
            self.messages.get_mut(tail).unwrap().next = Some(key);
        }
        let stream = self.stream_mut(stream_id).unwrap();
        if stream.message_head.is_none() {
            stream.message_head = Some(key);
        }
        stream.message_tail = Some(key);
        stream.message_count = count + 1;
        Ok(())
    }

    fn clear_messages(&mut self, mut next: Option<MessageNodeKey>) {
        while let Some(key) = next {
            let node = self.messages.remove(key).unwrap();
            next = node.next;
        }
    }

    fn remove_stream(&mut self, stream_id: StreamId) -> Option<StreamState> {
        let call = self.calls.remove(Self::call_hash(stream_id), |call| {
            call.stream_id == stream_id
        })?;
        if call.queued {
            self.pending.retain(|pending| *pending != stream_id);
        }
        if let Some(pending) = call.pending {
            self.release_response(pending);
        }
        let stream = call.stream?;
        self.buffered_total = self.buffered_total.saturating_sub(stream.buffered_len);
        self.clear_messages(stream.message_head);
        Some(stream)
    }

    fn take_stream(&mut self, stream_id: StreamId) -> Option<StreamState> {
        let stream = self.call_mut(stream_id)?.stream.take()?;
        self.buffered_total = self.buffered_total.saturating_sub(stream.buffered_len);
        Some(stream)
    }

    fn remove_empty_call(&mut self, stream_id: StreamId) {
        let empty = self
            .calls
            .get(Self::call_hash(stream_id), |call| {
                call.stream_id == stream_id
            })
            .is_some_and(|call| call.stream.is_none() && call.pending.is_none());
        if empty {
            let _ = self.calls.remove(Self::call_hash(stream_id), |call| {
                call.stream_id == stream_id
            });
        }
    }

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

        let mut added = 0usize;
        for message in &reply.messages {
            if MessageFrame::header(false, message.len()).is_err() {
                self.send_error(
                    stream_id,
                    Status::new(Code::Internal, "response message too large"),
                );
                return;
            }
            let Some(next) = added.checked_add(message.len() + 5) else {
                self.send_error(
                    stream_id,
                    Status::new(Code::ResourceExhausted, "pending response bytes are full"),
                );
                return;
            };
            added = next;
        }
        if added > self.pending_capacity.saturating_sub(self.pending_len) {
            self.send_error(
                stream_id,
                Status::new(Code::ResourceExhausted, "pending response bytes are full"),
            );
            return;
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

        let Some((tail, queued)) = self.call_mut(stream_id).map(|call| {
            (
                call.pending.as_ref().and_then(|pending| pending.tail),
                call.queued,
            )
        }) else {
            self.send_error(
                stream_id,
                Status::new(Code::Internal, "reply for unknown gRPC stream"),
            );
            return;
        };

        let batch = if reply.messages.is_empty() {
            None
        } else {
            match self.replies.insert(ReplyBatch {
                messages: reply.messages,
                next: None,
            }) {
                Ok(key) => Some(key),
                Err(_) => {
                    self.send_error(
                        stream_id,
                        Status::new(
                            Code::ResourceExhausted,
                            "pending response messages are full",
                        ),
                    );
                    return;
                }
            }
        };
        if let (Some(tail), Some(batch)) = (tail, batch) {
            self.replies.get_mut(tail).unwrap().next = Some(batch);
        }

        let needs_queue;
        let call = self.call_mut(stream_id).unwrap();
        match call.pending.as_mut() {
            Some(pending) => {
                if pending.head.is_none() {
                    pending.head = batch;
                }
                if batch.is_some() {
                    pending.tail = batch;
                }
                if trailers.is_some() {
                    pending.trailers = trailers;
                }
                needs_queue = !queued;
            }
            None => {
                call.pending = Some(PendingResponse {
                    headers,
                    head: batch,
                    tail: batch,
                    trailers,
                    headers_sent: false,
                    message_pos: 0,
                    frame_pos: 0,
                });
                needs_queue = true;
            }
        }
        self.pending_len += added;
        if needs_queue {
            if self.pending.is_full() {
                self.send_error(
                    stream_id,
                    Status::new(Code::ResourceExhausted, "pending response queue is full"),
                );
                return;
            }
            self.call_mut(stream_id).unwrap().queued = true;
            self.pending.vacant_entry().unwrap().push_back(stream_id);
        }
    }

    fn drive_pending(&mut self) {
        let len = self.pending.len();
        for _ in 0..len {
            let stream_id = self.pending.pop_front().unwrap();
            let Some(mut pending) = self.call_mut(stream_id).and_then(|call| {
                call.queued = false;
                call.pending.take()
            }) else {
                continue;
            };
            match pending.drive(
                &mut self.h2,
                &mut self.replies,
                &mut self.pending_len,
                stream_id,
            ) {
                Ok(ResponseDrive::Complete) => {
                    self.remove_empty_call(stream_id);
                }
                Ok(ResponseDrive::Blocked) => {
                    if let Some(call) = self.call_mut(stream_id) {
                        call.pending = Some(pending);
                        call.queued = true;
                        self.pending.vacant_entry().unwrap().push_back(stream_id);
                    }
                }
                Ok(ResponseDrive::Idle) => {
                    if let Some(call) = self.call_mut(stream_id) {
                        call.pending = Some(pending);
                    }
                }
                Err(()) => {
                    self.release_response(pending);
                    self.remove_empty_call(stream_id);
                }
            }
        }
    }

    fn release_response(&mut self, pending: PendingResponse) {
        self.pending_len = self
            .pending_len
            .saturating_sub(pending.remaining_len(&self.replies));
        let mut key = pending.head;
        while let Some(current) = key {
            let batch = self.replies.remove(current).unwrap();
            key = batch.next;
        }
    }

    fn send_error(&mut self, stream_id: StreamId, status: Status) {
        self.remove_stream(stream_id);
        self.live_routes.release(stream_id);
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
}

enum ResponseDrive {
    Complete,
    Blocked,
    Idle,
}

impl PendingResponse {
    fn drive(
        &mut self,
        conn: &mut Conn<ServerRole>,
        replies: &mut Slab<ReplyBatch, ReplyBatchTag>,
        pending_len: &mut usize,
        stream_id: StreamId,
    ) -> Result<ResponseDrive, ()> {
        if !self.headers_sent {
            let h2_headers = self.headers.as_h2();
            conn.send_response(stream_id, h2_headers.iter().copied(), false)
                .map_err(|_| ())?;
            self.headers_sent = true;
        }

        while let Some(key) = self.head {
            let message_len = {
                let batch = replies.get(key).unwrap();
                if self.message_pos == batch.messages.len() {
                    let batch = replies.remove(key).unwrap();
                    self.head = batch.next;
                    if self.head.is_none() {
                        self.tail = None;
                    }
                    self.message_pos = 0;
                    continue;
                }
                batch.messages[self.message_pos].len()
            };
            let header = MessageFrame::header(false, message_len).map_err(|_| ())?;
            let n = {
                let payload = &replies.get(key).unwrap().messages[self.message_pos];
                if self.frame_pos < header.len() {
                    conn.send_data_parts(stream_id, &header[self.frame_pos..], payload, false)
                } else {
                    conn.send_data(stream_id, &payload[self.frame_pos - header.len()..], false)
                }
            }
            .map_err(|_| ())?;
            if n == 0 {
                return Ok(ResponseDrive::Blocked);
            }
            self.frame_pos += n;
            *pending_len -= n;
            if self.frame_pos == header.len() + message_len {
                self.message_pos += 1;
                self.frame_pos = 0;
            }
        }

        if let Some(trailers) = &self.trailers {
            let h2_trailers = trailers.as_h2();
            conn.send_trailers(stream_id, &h2_trailers)
                .map_err(|_| ())?;
            Ok(ResponseDrive::Complete)
        } else {
            Ok(ResponseDrive::Idle)
        }
    }

    fn remaining_len(&self, replies: &Slab<ReplyBatch, ReplyBatchTag>) -> usize {
        let mut total = 0usize;
        let mut key = self.head;
        let mut first = true;
        while let Some(current) = key {
            let batch = replies.get(current).unwrap();
            let start = if first { self.message_pos } else { 0 };
            for (index, message) in batch.messages[start..].iter().enumerate() {
                let len = message.len() + 5;
                total += if first && index == 0 {
                    len - self.frame_pos
                } else {
                    len
                };
            }
            first = false;
            key = batch.next;
        }
        total
    }
}
