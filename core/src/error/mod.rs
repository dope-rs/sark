use std::borrow::Cow;
use std::io;

use http::StatusCode;
use thiserror::Error;

use crate::http::Response;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Not found")]
    NotFound,

    #[error("Method not allowed")]
    MethodNotAllowed,

    #[error("Bad request: {0}")]
    BadRequest(Cow<'static, str>),

    #[error("Payload too large: {0}")]
    PayloadTooLarge(Cow<'static, str>),

    #[error("Unauthorized: {0}")]
    Unauthorized(Cow<'static, str>),

    #[error("Forbidden: {0}")]
    Forbidden(Cow<'static, str>),

    #[error("Internal server error: {0}")]
    InternalServerError(Cow<'static, str>),

    #[error("Internal error: {0}")]
    Internal(Cow<'static, str>),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP parse error: {0}")]
    HttpParse(#[from] httparse::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ErrorBodyFormat {
    #[default]
    PlainText,
    Json,
}

impl Error {
    pub fn invalid_integer_header() -> Self {
        Self::BadRequest("Invalid integer header".into())
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Http(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Json(_) => StatusCode::BAD_REQUEST,
            Self::HttpParse(_) => StatusCode::BAD_REQUEST,
        }
    }

    pub fn to_response(&self) -> Response {
        self.to_response_with_format(ErrorBodyFormat::PlainText)
    }

    pub fn to_response_with_format(&self, format: ErrorBodyFormat) -> Response {
        let status = self.status_code();
        match format {
            ErrorBodyFormat::PlainText => Response::text_with_status(status, &self.to_string()),
            ErrorBodyFormat::Json => {
                let payload = serde_json::json!({
                    "status": status.as_u16(),
                    "error": self.to_string(),
                });
                match Response::json_with_status(status, &payload) {
                    Ok(r) => r,
                    Err(e) => Response::text_with_status(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Internal server error: {}", e),
                    ),
                }
            }
        }
    }
}

impl From<&Error> for Response {
    fn from(err: &Error) -> Self {
        err.to_response()
    }
}

impl From<Error> for Response {
    fn from(err: Error) -> Self {
        err.to_response()
    }
}

#[cfg(test)]
mod tests;
