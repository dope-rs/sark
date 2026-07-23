use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;

mod call;
mod egress;

use call::{CallRecord, CallStore, StreamState};
pub use call::{MessageIter, MessageList};
use egress::Egress;

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
use o3::collections::{FixedHashTable, FixedQueue};
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

type Env = Bundle<Tcp, Identity, Throughput>;
type TlsEnv = Bundle<Tcp, Tls, Throughput>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d, H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, App<H>, Env>,
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

impl Config {
    fn listener_config(&self, bind: SocketAddr) -> listener::Config<Tcp> {
        listener::Config::<Tcp> {
            max_connections: self.max_connections,
            bind,
            backlog: self.backlog,
            stream: Default::default(),
            transport: tcp::listener::Config {
                reuse_port: true,
                per_ip_limit: Some((self.max_connections / 2) as u32),
                ..Default::default()
            },
            egress: Default::default(),
        }
    }

    pub fn serve<H: Handler>(
        self,
        handler: H,
        context: WorkerContext,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()> {
        let driver = dope::driver::Config::for_tcp_profile::<Throughput>(self.max_connections);
        let exec = Executor::with_seed(driver, context.seed())?;
        exec.enter(|mut sess| {
            let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
            let mut app = App::with_config(handler, self.grpc.clone());
            app.liveness_fallback = self.readiness.is_some();
            let listener = {
                let mut driver = sess.driver_access();
                if let Some(trigger) = shutdown {
                    trigger.try_register(&mut driver)?;
                }
                Listener::<0, App<H>, Env>::open_in(
                    app,
                    self.listener_config(self.bind),
                    hash_builder,
                    &mut driver,
                )?
            };
            let app = core::pin::pin!(o3::cell::BrandCell::new(Dispatcher { listener }));
            sess.run(app.as_ref())
        })
    }

    pub fn serve_tls<H: Handler>(
        self,
        handler: H,
        tls_cfg: shin::server::Config,
        context: WorkerContext,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()> {
        let driver = dope::driver::Config::for_tcp_profile::<Throughput>(self.max_connections);
        let exec = Executor::with_seed(driver, context.seed())?;
        exec.enter(|mut sess| {
            let accept_hash = sess.seed().derive(dope::hash::domain::ACCEPT).state();
            let readiness_hash = sess.seed().derive(dope::hash::domain::ACCEPT ^ 1).state();
            let (listener, readiness) = {
                let mut driver = sess.driver_access();
                if let Some(trigger) = shutdown {
                    trigger.try_register(&mut driver)?;
                }
                let listener = Listener::<0, App<H, Tls>, TlsEnv>::open_in_with_wire(
                    App::with_config(handler, self.grpc.clone()),
                    self.listener_config(self.bind),
                    Endpoint::server(tls_cfg).map_err(io::Error::other)?,
                    accept_hash,
                    &mut driver,
                )?;
                let readiness = match self.readiness {
                    Some(addr) => Some(Listener::<1, liveness::Liveness, Env>::open_in(
                        liveness::Liveness,
                        self.listener_config(addr),
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
}

mod liveness {
    use super::{
        Application, DriverContext, Identity, Pin, RetainBytes, Slot, Wire, listener, manifold,
    };

    const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

    pub struct Liveness;

    impl Liveness {
        const H2_PREFACE: &'static [u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

        pub(super) fn is_plain_request(first: &[u8]) -> bool {
            let n = first.len().min(Self::H2_PREFACE.len());
            first[..n] != Self::H2_PREFACE[..n]
        }

        pub(super) fn respond<'d, W: Wire, C: Default + 'static>(
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
    }

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
            Self::respond(slot, aux, driver);
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
        out.reserve(self.messages.iter().map(|payload| payload.len() + 5).sum());
        for payload in &self.messages {
            MessageFrame::encode(false, payload, out)
                .map_err(|_| Status::new(Code::Internal, "response message too large"))?;
        }
        Ok(())
    }
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

/// A route handler that borrows one service owned by its dispatcher.
///
/// Unlike cloning a reference-counted interior-mutable handle into every
/// route, this makes the single mutable service borrow explicit at dispatch
/// and has no runtime ownership or borrow bookkeeping.
pub trait ServiceHandler<S>: 'static {
    fn start(
        &mut self,
        service: &mut S,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        head: &RequestHead,
        reply: &mut StreamReply,
    ) -> StreamMode {
        let _ = (service, routes, stream_id, head, reply);
        StreamMode::Buffered
    }

    fn message(
        &mut self,
        service: &mut S,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        message: MessageFrame,
        reply: &mut StreamReply,
    ) {
        let _ = (service, routes, stream_id, message, reply);
    }

    fn trailers(
        &mut self,
        service: &mut S,
        routes: &mut StreamRoutes,
        stream_id: StreamId,
        trailers: Metadata,
        reply: &mut StreamReply,
    ) {
        let _ = (service, routes, stream_id, trailers, reply);
    }

    fn request(&mut self, service: &mut S, request: Request<'_>, response: &mut Response);
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

/// Path dispatcher that owns its service exactly once.
pub struct ServiceRoutes<S, H: ServiceHandler<S>> {
    service: S,
    routes: Vec<ServiceRoute<H>>,
}

struct ServiceRoute<H> {
    path: Vec<u8>,
    handler: H,
}

impl<S, H: ServiceHandler<S>> ServiceRoutes<S, H> {
    pub fn new(service: S) -> Self {
        Self {
            service,
            routes: Vec::new(),
        }
    }

    pub fn push(&mut self, path: &[u8], handler: H) {
        self.routes.push(ServiceRoute {
            path: path.to_vec(),
            handler,
        });
    }
}

impl<S: 'static, H: ServiceHandler<S>> Handler for ServiceRoutes<S, H> {
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
        let mode =
            self.routes[route_idx]
                .handler
                .start(&mut self.service, routes, stream_id, head, reply);
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
        self.routes[route_idx].handler.message(
            &mut self.service,
            routes,
            stream_id,
            message,
            reply,
        );
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
        self.routes[route_idx].handler.trailers(
            &mut self.service,
            routes,
            stream_id,
            trailers,
            reply,
        );
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
        route.handler.request(&mut self.service, request, response);
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

impl Limits {
    pub fn dispatch_buffered(
        &self,
        handler: &mut (impl Handler + ?Sized),
        head: RequestHead,
        body: &[u8],
    ) -> Response {
        let mut deframer = Deframer::new(self.max_message_len);
        let input_pool = SharedPool::new(1, body.len().max(1));
        let Some(mut lease) = input_pool.try_acquire() else {
            return Response::with_status(Status::new(
                Code::ResourceExhausted,
                "request buffer is unavailable",
            ));
        };
        if lease.spare_writer().try_extend_from_slice(body).is_err() {
            return Response::with_status(Status::new(
                Code::ResourceExhausted,
                "request is too large",
            ));
        }
        let mut input = DataChunk::from_pooled(lease.freeze());
        let fragment_pool = SharedPool::new(1, self.max_message_len.max(1));
        let capacity = self
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
        dispatch_request(
            handler,
            StreamId(0),
            head,
            MessageList::from_queue(&queue),
            Metadata::new(),
        )
        .unwrap_or_else(Response::with_status)
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

    fn unary(&mut self, request: UnaryRequest<Self::Request>) -> UnaryResponse<Self::Response>;
}

pub trait UnaryService<S>: 'static {
    type Request;
    type Response;
    type Codec: Codec<Decode = Self::Request, Encode = Self::Response>;

    fn unary(
        &mut self,
        service: &mut S,
        request: UnaryRequest<Self::Request>,
    ) -> UnaryResponse<Self::Response>;
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

pub trait StreamingService<S>: 'static {
    type Request;
    type Response;
    type Codec: Codec<Decode = Self::Request, Encode = Self::Response>;

    fn stream(
        &mut self,
        service: &mut S,
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

pub struct ServiceUnary<S, H: UnaryService<S>> {
    handler: H,
    codec: H::Codec,
    service: PhantomData<fn(&mut S)>,
}

pub struct ServiceStreaming<S, H: StreamingService<S>> {
    handler: H,
    codec: H::Codec,
    service: PhantomData<fn(&mut S)>,
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

impl<S, H: UnaryService<S>> ServiceUnary<S, H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            service: PhantomData,
        }
    }
}

impl<S, H: StreamingService<S>> ServiceStreaming<S, H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            service: PhantomData,
        }
    }
}

impl<H: LiveStreamingHandler> LiveStreaming<H> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self { handler, codec }
    }
}

fn dispatch_unary<C: Codec>(
    codec: &mut C,
    request: Request<'_>,
    response: &mut Response,
    invoke: impl FnOnce(UnaryRequest<C::Decode>) -> UnaryResponse<C::Encode>,
) {
    let mut messages = request.messages.iter();
    let Some(message) = messages.next() else {
        response.status = Status::new(Code::InvalidArgument, "unary request needs one message");
        return;
    };
    if messages.next().is_some() {
        response.status = Status::new(Code::InvalidArgument, "unary request needs one message");
        return;
    }
    let decoded = match codec.decode(message.payload.as_slice()) {
        Ok(decoded) => decoded,
        Err(status) => {
            response.status = status;
            return;
        }
    };
    let unary_response = invoke(UnaryRequest {
        stream_id: request.stream_id,
        head: request.head,
        message: decoded,
        trailers: request.trailers,
    });
    response.metadata = unary_response.metadata;
    response.trailers = unary_response.trailers;
    response.status = unary_response.status;
    if let Some(message) = unary_response.message {
        let mut encoded = Vec::new();
        match codec.encode(&message, &mut encoded) {
            Ok(()) => response.push_message(encoded),
            Err(status) => response.status = status,
        }
    }
}

fn dispatch_streaming<C: Codec>(
    codec: &mut C,
    request: Request<'_>,
    response: &mut Response,
    invoke: impl FnOnce(StreamingRequest<C::Decode>) -> StreamingResponse<C::Encode>,
) {
    let mut messages = Vec::with_capacity(request.messages.len());
    for message in request.messages.iter() {
        match codec.decode(message.payload.as_slice()) {
            Ok(decoded) => messages.push(decoded),
            Err(status) => {
                response.status = status;
                return;
            }
        }
    }
    let stream_response = invoke(StreamingRequest {
        stream_id: request.stream_id,
        head: request.head,
        messages,
        trailers: request.trailers,
    });
    response.metadata = stream_response.metadata;
    response.trailers = stream_response.trailers;
    response.status = stream_response.status;
    for message in stream_response.messages {
        let mut encoded = Vec::new();
        match codec.encode(&message, &mut encoded) {
            Ok(()) => response.push_message(encoded),
            Err(status) => {
                response.status = status;
                response.messages.clear();
                return;
            }
        }
    }
}

impl<H: UnaryHandler> Handler for Unary<H> {
    fn request(&mut self, request: Request<'_>, response: &mut Response) {
        dispatch_unary(&mut self.codec, request, response, |request| {
            self.handler.unary(request)
        });
    }
}

impl<H: StreamingHandler> Handler for Streaming<H> {
    fn request(&mut self, request: Request<'_>, response: &mut Response) {
        dispatch_streaming(&mut self.codec, request, response, |request| {
            self.handler.stream(request)
        });
    }
}

impl<S: 'static, H: UnaryService<S>> ServiceHandler<S> for ServiceUnary<S, H> {
    fn request(&mut self, service: &mut S, request: Request<'_>, response: &mut Response) {
        dispatch_unary(&mut self.codec, request, response, |request| {
            self.handler.unary(service, request)
        });
    }
}

impl<S: 'static, H: StreamingService<S>> ServiceHandler<S> for ServiceStreaming<S, H> {
    fn request(&mut self, service: &mut S, request: Request<'_>, response: &mut Response) {
        dispatch_streaming(&mut self.codec, request, response, |request| {
            self.handler.stream(service, request)
        });
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
    calls: CallStore,
    egress: Egress,
    live_routes: StreamRoutes,
    message_pool: SharedPool,
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
            calls: CallStore::with_capacity(capacity, limits.max_buffered_msgs),
            egress: Egress::with_capacity(
                capacity,
                limits.max_pending_replies,
                limits.max_pending_len,
            ),
            live_routes: StreamRoutes::with_capacity(capacity),
            message_pool: SharedPool::new(
                limits.max_fragmented_messages,
                limits.max_message_len.max(1),
            ),
            probed: false,
        }
    }
}

#[pin_project::pin_project]
pub struct App<H: Handler, W: Wire = Identity> {
    handler: H,
    config: Limits,
    liveness_fallback: bool,
    _wire: ::std::marker::PhantomData<W>,
}

struct GrpcTransport<'a, H: Handler, W: Wire> {
    app: &'a mut App<H, W>,
    state: &'a mut ConnState,
}

impl<H: Handler, W: Wire> sark_h2::server::driver::Transport for GrpcTransport<'_, H, W> {
    fn connection(&mut self) -> &mut Conn<ServerRole> {
        &mut self.state.h2
    }

    fn drain_events(&mut self) -> usize {
        let drained = self.app.drain_events(self.state);
        self.state.drive_pending();
        drained
    }
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
        match RequestHead::parse_h2(&headers) {
            Ok(head) => {
                let mut reply = StreamReply::new();
                let mode = self
                    .handler
                    .start(&mut state.live_routes, stream_id, &head, &mut reply);
                if !state.insert_stream(
                    stream_id,
                    StreamState::new(head, self.config.max_message_len, mode),
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
        match RequestHead::parse_h2_trailers(&headers) {
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
                let message_pool = &state.message_pool;
                let Some(stream) = state.calls.stream_mut(stream_id) else {
                    state.send_error(
                        stream_id,
                        Status::new(Code::Internal, "DATA before gRPC headers"),
                    );
                    return;
                };
                (stream.deframer.next(&mut data, message_pool), stream.mode)
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
            if state
                .calls
                .push_message(
                    stream_id,
                    message,
                    self.config.max_buffered_len,
                    self.config.max_buffered_msgs,
                    self.config.max_conn_buffered_len,
                )
                .is_err()
            {
                state.remove_stream(stream_id);
                state.send_error(
                    stream_id,
                    Status::new(Code::ResourceExhausted, "stream buffer limit exceeded"),
                );
                return;
            }
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
        let message_chain = stream.message_chain();
        let messages = state.calls.message_list(message_chain);
        let response = dispatch_request(
            &mut self.handler,
            stream_id,
            stream.head,
            messages,
            stream.trailers,
        );
        state.calls.release_messages(message_chain);
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
            if liveness::Liveness::is_plain_request(bytes) {
                liveness::Liveness::respond(slot, aux, driver);
                return manifold::Outcome::Ok;
            }
        }
        let error = sark_h2::server::driver::Driver::new(&mut GrpcTransport {
            app: self,
            state: project(&mut slot.state.conn),
        })
        .ingest(bytes)
        .err();
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
        let app = self.get_ref();
        let mut state = ConnState::with_limits(&app.config);
        state.probed = !app.liveness_fallback;
        state
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
    fn call_mut(&mut self, stream_id: StreamId) -> Option<&mut CallRecord> {
        self.calls.get_mut(stream_id)
    }

    fn stream_mut(&mut self, stream_id: StreamId) -> Option<&mut StreamState> {
        self.call_mut(stream_id)?.stream.as_mut()
    }

    fn insert_stream(&mut self, stream_id: StreamId, stream: StreamState) -> bool {
        self.calls.insert(stream_id, stream)
    }

    fn remove_stream(&mut self, stream_id: StreamId) -> Option<StreamState> {
        let mut call = self.calls.remove(stream_id)?;
        self.egress.detach(stream_id, &mut call);
        let stream = call.stream?;
        self.calls.release_stream(&stream);
        Some(stream)
    }

    fn take_stream(&mut self, stream_id: StreamId) -> Option<StreamState> {
        self.calls.take_stream(stream_id)
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
        if let Err(status) = self.egress.enqueue(&mut self.calls, stream_id, reply) {
            self.send_error(stream_id, status);
        }
    }

    fn drive_pending(&mut self) {
        self.egress.drive(&mut self.calls, &mut self.h2);
    }

    fn send_error(&mut self, stream_id: StreamId, status: Status) {
        self.remove_stream(stream_id);
        self.live_routes.release(stream_id);
        let headers = HeaderBlock::for_response(&Metadata::new()).ok();
        if let Some(headers) = headers
            && self
                .h2
                .send_response(stream_id, headers.iter(), false)
                .is_err()
        {
            let _ = self.h2.reset_stream(stream_id, ErrorCode::InternalError);
            return;
        }
        if let Ok(trailers) = HeaderBlock::for_trailers(&status, &Metadata::new()) {
            let _ = self.h2.send_trailers_fields(stream_id, trailers.iter());
        }
    }
}
