use sark_http::payload::{fixed, response::status};

#[sark_fiber::response(raw)]
#[header("content-type", "text/plain")]
pub struct Reply<'d> {
    pub status: status::Code,
    pub body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn plain_h() -> Reply<'d> {
    Reply {
        status: status::Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

/// Declares path fields opposite to positional route order to check binding by name.
#[sark_fiber::request]
pub struct PairReq {
    #[path]
    pub __sark_m: storage::Shared,
    #[path]
    pub req: storage::Shared,
}

#[sark_fiber::handler(pub)]
fn pair_h(req: PairReq) -> Reply<'d> {
    Reply {
        status: if req.__sark_m == b"alpha" && req.req == b"beta" {
            status::Code::IM_A_TEAPOT
        } else {
            status::Code::BAD_REQUEST
        },
        body: fixed::Body::new(b"named-pair"),
    }
}

sark_fiber::define_route! {
    pub AgnApp: sark_fiber::EmptyState => {
        GET "/json" => plain_h,
        GET "/users/:req/teams/:__sark_m" => request(path) pair_h,
        OPTIONS "*" => plain_h,
    }
}

use sark_http::field::metadata::map::record::Entry;

pub const FIELDS: [Entry<'static>; 3] = [
    Entry::new(b":method", b"GET"),
    Entry::new(b":scheme", b"https"),
    Entry::new(b":path", b"/users/beta/teams/alpha"),
];

pub const ASTERISK: [Entry<'static>; 4] = [
    Entry::new(b":method", b"OPTIONS"),
    Entry::new(b":scheme", b"https"),
    Entry::new(b":authority", b"example.test"),
    Entry::new(b":path", b"*"),
];
