use super::OwnedReply;
use sark_http::payload::response::status;

#[sark_fiber::handler(pub)]
fn ping() -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: b"pong".to_vec(),
    }
}

sark_fiber::define_route! {
    pub LifetimeDispatch: () => {
        GET "/ping" => ping,
    }
}
