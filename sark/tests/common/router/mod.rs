use sark::http::payload::response::status::Code;

pub mod planned;

#[sark::request]
pub struct EchoRequest {
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::response(raw)]
pub struct EchoResponse<'req> {
    pub(crate) status: Code,
    pub(crate) body: &'req [u8],
}

#[sark::request]
pub struct ParamReq {
    #[path]
    pub id: storage::Shared,
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::handler(pub)]
pub(crate) fn probe(req: EchoRequest) -> EchoResponse<'req> {
    EchoResponse {
        status: Code::OK,
        body: req.payload,
    }
}

sark::define_router! {
    pub ProbeRouter => {
        POST "/probe" => probe,
    }
}
