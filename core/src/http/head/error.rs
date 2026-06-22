use crate::error::Error;

pub const ERR_TOO_MANY_HEADERS: &str = "Too many headers";
pub const ERR_INVALID_HEADER_NAME: &str = "Invalid header name";

pub fn bad_request(msg: &'static str) -> Error {
    Error::BadRequest(msg.into())
}
