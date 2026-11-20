use sark_http::payload::{fixed, response::status};

#[sark_fiber::response(raw)]
#[header("content-type", "text/plain")]
pub struct PingReply<'d> {
    status: status::Code,
    body: fixed::Body<'d>,
}

const OK: PingReply<'static> = PingReply {
    status: status::Code::OK,
    body: fixed::Body::new(b"ok"),
};

#[sark_fiber::handler(pub)]
#[skip(date, server)]
fn lean() -> PingReply<'d> {
    OK
}

#[sark_fiber::handler(pub)]
fn full() -> PingReply<'d> {
    OK
}

#[sark_fiber::handler(pub)]
#[skip(server)]
fn no_server() -> PingReply<'d> {
    OK
}

#[sark_fiber::handler(pub)]
#[skip(date)]
async fn no_date() -> PingReply<'d> {
    OK
}

sark_fiber::define_route! {
    pub SkipDispatch: () => {
        GET "/lean" => lean,
        GET "/full" => full,
        GET "/no-server" => no_server,
        GET "/no-date" => async(capacity = 1, body = discarded) no_date,
    }
}
