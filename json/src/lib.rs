use http::StatusCode;
use sark_core::http::Response;

mod body;
mod depth;
mod encode;
mod error;
mod parse;
mod scan;
mod traits;

pub use body::InlineToken;
pub use depth::{DepthGuard, MAX_DEPTH};
pub use encode::{Encode, Writer};
pub use o3::pool::Scratch;
pub use parse::Parse;
pub use scan::Scan;
pub use traits::{JsonDecode, JsonEncode, JsonPreserve, JsonScan};

pub type Result<T> = sark_core::error::Result<T>;

pub struct Json;

impl Json {
    pub fn ok<T: JsonEncode>(value: T) -> Response {
        Self::status(StatusCode::OK, value)
    }

    pub fn ok_preserve<T: JsonEncode + JsonPreserve>(value: T) -> Response {
        Self::status_preserve(StatusCode::OK, value)
    }

    pub fn status<T: JsonEncode>(status: StatusCode, value: T) -> Response {
        let body = value.encode_json();
        let mut response = Response::new(status);
        response.content_type("application/json");
        response.set_body(body);
        response
    }

    pub fn status_preserve<T: JsonEncode + JsonPreserve>(status: StatusCode, value: T) -> Response {
        let body = if let Some(raw) = value.raw_json() {
            raw.clone()
        } else {
            value.encode_json().freeze()
        };
        let mut response = Response::new(status);
        response.content_type("application/json");
        response.set_body(body);
        response
    }

    pub fn bad_request(msg: impl Into<String>) -> sark_core::error::Error {
        error::Fail::with(msg)
    }
}
