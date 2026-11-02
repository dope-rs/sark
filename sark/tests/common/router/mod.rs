use sark::http::payload::response::status::Code;

pub mod planned;

pub const CHUNKED_FRAMES: [&[u8]; 2] = [
    b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\nd\r\nborrowed-body\r\n0\r\n\r\n",
    b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n9\r\nborrowed-\r\n4\r\nbody\r\n0\r\n\r\n",
];

pub struct State {
    pub body_ptr: usize,
    pub path_ptr: usize,
}

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

#[sark::handler(pub)]
pub(crate) fn echo(req: EchoRequest, state: &State) -> EchoResponse<'req> {
    let valid = req.payload == b"borrowed-body"
        && (state.body_ptr == 0 || req.payload.as_ref().as_ptr() as usize == state.body_ptr);
    EchoResponse {
        status: if valid {
            Code::OK
        } else {
            Code::INTERNAL_SERVER_ERROR
        },
        body: req.payload,
    }
}

#[sark::request]
pub struct ParamReq {
    #[path]
    pub id: storage::Shared,
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::request]
pub struct NamedReq {
    #[path]
    pub team_id: storage::Shared,
    #[path]
    pub user_id: storage::Shared,
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::handler(pub)]
pub(crate) fn echo_param(req: ParamReq, state: &State) -> EchoResponse<'req> {
    let valid = req.id == b"item-42"
        && req.id.as_ref().as_ptr() as usize == state.path_ptr
        && req.payload.as_ref().as_ptr() as usize == state.body_ptr;
    EchoResponse {
        status: if valid {
            Code::OK
        } else {
            Code::INTERNAL_SERVER_ERROR
        },
        body: req.payload,
    }
}

#[sark::handler(pub)]
pub(crate) fn echo_named_param(req: NamedReq, state: &State) -> EchoResponse<'req> {
    let valid = req.team_id == b"alpha"
        && req.user_id == b"beta"
        && req.user_id.as_ref().as_ptr() as usize == state.path_ptr
        && req.payload.as_ref().as_ptr() as usize == state.body_ptr;
    EchoResponse {
        status: if valid {
            Code::OK
        } else {
            Code::INTERNAL_SERVER_ERROR
        },
        body: req.payload,
    }
}

sark::define_router! {
    pub Router: State => {
        POST "/echo" => echo,
        POST "/echo/:id" => request(path) echo_param,
        POST "/users/:user_id/teams/:team_id" => request(path) echo_named_param,
    }
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
