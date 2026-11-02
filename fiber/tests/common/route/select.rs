use sark_http::payload::{fixed, response::status};

#[sark_fiber::request(full)]
pub struct FullReq {}

#[sark_fiber::request]
pub struct RoutedReq {
    #[path]
    id: storage::Shared,
    #[query("tag", default = "")]
    tag: storage::Shared,
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    status: status::Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn minimal() -> Reply<'d> {
    Reply {
        status: status::Code::OK,
        body: fixed::Body::new(b"minimal"),
    }
}

#[sark_fiber::handler(pub, request = FullReq)]
fn full() -> Reply<'d> {
    Reply {
        status: status::Code::OK,
        body: fixed::Body::new(b"full"),
    }
}

#[sark_fiber::handler(pub)]
fn routed(req: RoutedReq) -> Reply<'d> {
    Reply {
        status: if req.id == b"42" && req.tag == b"rust" {
            status::Code::IM_A_TEAPOT
        } else {
            status::Code::OK
        },
        body: fixed::Body::new(b"routed"),
    }
}

sark_fiber::define_route! {
    pub SelectiveDispatch: () => {
        GET "/minimal" => minimal,
        GET "/full" => full,
        GET "/items/:id" => request(path, query) routed,
        GET "/items/new" => minimal,
        GET "/items/42suffix" => minimal,
        GET "/items/42suffix.gz" => full,
    }
}
