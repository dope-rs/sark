use sark_http::payload::response::status::Code;

#[derive(Debug)]
#[repr(u8)]
pub enum Reject {
    BadRequest,
    Internal,
    Unavailable,
}

impl Reject {
    pub const fn status(self) -> Code {
        match self {
            Self::BadRequest => Code::BAD_REQUEST,
            Self::Internal => Code::INTERNAL_SERVER_ERROR,
            Self::Unavailable => Code::SERVICE_UNAVAILABLE,
        }
    }
}
