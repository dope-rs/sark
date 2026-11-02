use sark_http::payload::response::status;

#[sark_fiber::response(raw)]
pub struct Reply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
fn ping() -> Reply {
    Reply {
        status: status::Code::OK,
        body: b"pong".to_vec(),
    }
}

sark_fiber::define_route! {
    pub LifetimeDispatch: () => {
        GET "/ping" => ping,
    }
}
