use std::sync::atomic::{AtomicUsize, Ordering};

use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::RequestHead;
use sark_grpc::server::{Limits, Routes};
use sark_grpc::{
    Code, Codec, LiveMessage, LiveResponse, LiveStreaming, LiveStreamingHandler, Status, Streaming,
    StreamingHandler, StreamingRequest, StreamingResponse, Unary, UnaryHandler, UnaryRequest,
    UnaryResponse,
};

static CALLS: AtomicUsize = AtomicUsize::new(0);
static UNARY_DROPS: AtomicUsize = AtomicUsize::new(0);
static STREAM_DROPS: AtomicUsize = AtomicUsize::new(0);

struct Service {
    calls: usize,
    drops: &'static AtomicUsize,
}

impl Drop for Service {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct Method;
struct StreamMethod;
struct LiveMethod;
struct StatelessMethod;

struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;

    fn encode(&mut self, item: &Self::Encode, out: &mut Vec<u8>) -> Result<(), Status> {
        out.extend_from_slice(item);
        Ok(())
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Decode, Status> {
        Ok(bytes.to_vec())
    }
}

impl UnaryHandler<Service> for Method {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Codec = BytesCodec;

    fn unary(
        &mut self,
        context: &mut Service,
        request: UnaryRequest<Self::Request>,
    ) -> UnaryResponse<Self::Response> {
        context.calls += 1;
        CALLS.fetch_add(1, Ordering::Relaxed);
        UnaryResponse::new(request.message)
    }
}

impl StreamingHandler<Service> for StreamMethod {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Codec = BytesCodec;

    fn stream(
        &mut self,
        context: &mut Service,
        request: StreamingRequest<Self::Request>,
    ) -> StreamingResponse<Self::Response> {
        context.calls += 1;
        StreamingResponse::new(request.messages)
    }
}

impl LiveStreamingHandler<Service> for LiveMethod {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Codec = BytesCodec;

    fn message(
        &mut self,
        context: &mut Service,
        message: LiveMessage<Self::Request>,
    ) -> LiveResponse<Self::Response> {
        context.calls += 1;
        LiveResponse::message(message.message)
    }
}

impl UnaryHandler for StatelessMethod {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Codec = BytesCodec;

    fn unary(
        &mut self,
        _context: &mut (),
        request: UnaryRequest<Self::Request>,
    ) -> UnaryResponse<Self::Response> {
        UnaryResponse::new(request.message)
    }
}

#[test]
fn context_routes_own_one_service_and_keep_route_adapters_zero_sized() {
    CALLS.store(0, Ordering::Relaxed);
    UNARY_DROPS.store(0, Ordering::Relaxed);

    let route = Unary::<_, Service>::new(Method, BytesCodec);
    assert_eq!(std::mem::size_of_val(&route), 0);
    let mut routes = Routes::with_context(Service {
        calls: 0,
        drops: &UNARY_DROPS,
    });
    routes.push(b"/use.case/echo", route);

    let mut body = Vec::new();
    MessageFrame::encode(false, b"proof", &mut body).unwrap();
    let response = Limits::default().dispatch_buffered(
        &mut routes,
        RequestHead {
            path: b"/use.case/echo".to_vec(),
            authority: None,
            metadata: Default::default(),
        },
        &body,
    );

    assert_eq!(response.status.code(), Code::Ok);
    assert_eq!(response.messages[0], b"proof");
    assert_eq!(CALLS.load(Ordering::Relaxed), 1);
    assert_eq!(UNARY_DROPS.load(Ordering::Relaxed), 0);

    drop(routes);
    assert_eq!(UNARY_DROPS.load(Ordering::Relaxed), 1);
}

#[test]
fn every_context_adapter_is_zero_sized_and_streaming_uses_the_owned_context() {
    STREAM_DROPS.store(0, Ordering::Relaxed);

    let stream = Streaming::<_, Service>::new(StreamMethod, BytesCodec);
    let live = LiveStreaming::<_, Service>::new(LiveMethod, BytesCodec);
    assert_eq!(std::mem::size_of_val(&stream), 0);
    assert_eq!(std::mem::size_of_val(&live), 0);

    let mut routes = Routes::with_context(Service {
        calls: 0,
        drops: &STREAM_DROPS,
    });
    routes.push(b"/use.case/stream", stream);

    let mut body = Vec::new();
    MessageFrame::encode(false, b"one", &mut body).unwrap();
    MessageFrame::encode(false, b"two", &mut body).unwrap();
    let response = Limits::default().dispatch_buffered(
        &mut routes,
        RequestHead {
            path: b"/use.case/stream".to_vec(),
            authority: None,
            metadata: Default::default(),
        },
        &body,
    );

    assert_eq!(response.status.code(), Code::Ok);
    assert_eq!(response.messages, [b"one".to_vec(), b"two".to_vec()]);

    drop(routes);
    assert_eq!(STREAM_DROPS.load(Ordering::Relaxed), 1);
}

#[test]
fn unit_context_keeps_the_plain_constructor_and_route_shape() {
    let route = Unary::new(StatelessMethod, BytesCodec);
    assert_eq!(std::mem::size_of_val(&route), 0);

    let mut routes = Routes::new();
    routes.push(b"/use.case/plain", route);
}
