use http::StatusCode;
use o3::buffer::{Borrowed, Bytes};
use sark::http::{FixedResponse, OwnedShape, Serve};

#[sark_gen::request]
struct Req {}

#[sark_gen::response(raw)]
struct BorrowedReply<'req> {
    status: StatusCode,
    body: Vec<u8>,
    #[header("x-local")]
    local: Bytes<Borrowed<'req>>,
}

#[sark_gen::handler]
async fn borrowed(_request: Req, _state: &()) -> BorrowedReply<'static> {
    BorrowedReply {
        status: StatusCode::OK,
        body: Vec::new(),
        local: Bytes::<Borrowed<'_>>::from(b"local"),
    }
}

fn require_owned<T: OwnedShape>() {}

fn main() {
    require_owned::<FixedResponse>();
    require_owned::<Serve>();
}
