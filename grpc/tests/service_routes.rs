use std::sync::atomic::{AtomicUsize, Ordering};

use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::RequestHead;
use sark_grpc::server::{Limits, ServiceRoutes};
use sark_grpc::{Code, Codec, ServiceUnary, Status, UnaryRequest, UnaryResponse, UnaryService};

static CALLS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);

struct Service;

impl Drop for Service {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

struct Method;

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

impl UnaryService<Service> for Method {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Codec = BytesCodec;

    fn unary(
        &mut self,
        _service: &mut Service,
        request: UnaryRequest<Self::Request>,
    ) -> UnaryResponse<Self::Response> {
        CALLS.fetch_add(1, Ordering::Relaxed);
        UnaryResponse::new(request.message)
    }
}

#[test]
fn service_routes_own_one_service_and_keep_route_adapters_zero_sized() {
    CALLS.store(0, Ordering::Relaxed);
    DROPS.store(0, Ordering::Relaxed);

    let route = ServiceUnary::new(Method, BytesCodec);
    assert_eq!(std::mem::size_of_val(&route), 0);
    let mut routes = ServiceRoutes::new(Service);
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
    assert_eq!(DROPS.load(Ordering::Relaxed), 0);

    drop(routes);
    assert_eq!(DROPS.load(Ordering::Relaxed), 1);
}
