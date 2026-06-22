pub mod decode;
pub mod encode;
mod header_utils;

pub use decode::{BodyKind, DecodeMode, DecodedHead, HeaderScan, ParsedRequestHead, chunked};
pub use encode::Wire;
pub use header_utils::{Header, HeaderLookup};

use crate::error::Result;

pub struct Parse;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BodyFraming {
    Length(usize),
    Chunked,
}

impl HeaderScan {
    pub fn validate_for_request(&self) -> Result<BodyFraming> {
        if self.duplicate_content_length {
            return Err(crate::error::Error::BadRequest(
                "Multiple Content-Length headers are not allowed".into(),
            ));
        }

        if self.has_transfer_encoding {
            if self.content_length.is_some() {
                return Err(crate::error::Error::BadRequest(
                    "Content-Length with Transfer-Encoding is not allowed".into(),
                ));
            }
            if self.is_chunked_transfer {
                return Ok(BodyFraming::Chunked);
            }
            return Err(crate::error::Error::BadRequest(
                "Transfer-Encoding is not supported".into(),
            ));
        }

        Ok(BodyFraming::Length(self.content_length.unwrap_or(0)))
    }
}
