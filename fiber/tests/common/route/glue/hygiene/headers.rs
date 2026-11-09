use sark_http::payload::{fixed, response::status};

#[sark_fiber::request]
pub struct Request {
    #[header("x-token-a", default = "")]
    pub __sark_m: Shared,
    #[header("x-token-abc", default = "")]
    pub __sark_mode: Shared,
    #[header("x-token-abcdefg", default = "")]
    pub eight: Shared,
}

#[sark_fiber::response(raw)]
#[header_static("x-hygiene", "kept")]
pub struct Reply<'d> {
    pub(super) r#status: status::Code,
    pub(super) r#body: r#fixed::r#Body<'d>,
}
