use sark_core::http::OwnedField;
use sark_h2::hpack::{Header, HeaderBlock as H2HeaderBlock};

use crate::metadata::{Metadata, MetadataError};
use crate::status::{Code, Status};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderBlock {
    headers: Vec<OwnedField>,
}

impl HeaderBlock {
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) {
        self.headers.push(OwnedField {
            name: name.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn as_h2(&self) -> Vec<Header<'_>> {
        self.headers
            .iter()
            .map(|field| Header {
                name: &field.name,
                value: &field.value,
            })
            .collect()
    }

    pub fn owned(&self) -> &[OwnedField] {
        &self.headers
    }

    pub fn from_h2(headers: &H2HeaderBlock) -> Vec<OwnedField> {
        headers
            .iter()
            .map(|field| OwnedField {
                name: field.name.to_vec(),
                value: field.value.to_vec(),
            })
            .collect()
    }

    pub fn is_grpc_content_type(value: &[u8]) -> bool {
        value == b"application/grpc" || value.starts_with(b"application/grpc+")
    }

    fn find_one<'a>(headers: &'a [OwnedField], name: &[u8]) -> Option<&'a [u8]> {
        headers
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.value.as_slice())
    }

    pub fn for_request(
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<HeaderBlock, Status> {
        RequestHead::validate_path(path)?;
        let mut out = HeaderBlock::new();
        out.push(b":method", b"POST");
        out.push(b":scheme", b"http");
        out.push(b":path", path);
        if let Some(authority) = authority {
            out.push(b":authority", authority);
        }
        out.push(b"content-type", b"application/grpc+proto");
        out.push(b"te", b"trailers");
        out.push_metadata(metadata)?;
        Ok(out)
    }

    pub fn for_response(metadata: &Metadata) -> Result<HeaderBlock, Status> {
        let mut out = HeaderBlock::new();
        out.push(b":status", b"200");
        out.push(b"content-type", b"application/grpc+proto");
        out.push_metadata(metadata)?;
        Ok(out)
    }

    pub fn for_trailers(status: &Status, metadata: &Metadata) -> Result<HeaderBlock, Status> {
        let mut out = HeaderBlock::new();
        let mut code = Vec::new();
        status.write_grpc_status_value(&mut code);
        out.push(b"grpc-status", &code);
        if !status.message().is_empty() {
            let mut message = Vec::new();
            Status::encode_message(status.message(), &mut message);
            out.push(b"grpc-message", &message);
        }
        out.push_metadata(metadata)?;
        Ok(out)
    }

    fn push_metadata(&mut self, metadata: &Metadata) -> Result<(), Status> {
        for entry in metadata.entries() {
            if entry.name.starts_with(b":") || Metadata::is_reserved(&entry.name) {
                return Err(Status::new(Code::Internal, "reserved metadata name"));
            }
            self.push(&entry.name, &entry.value);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead {
    pub path: Vec<u8>,
    pub authority: Option<Vec<u8>>,
    pub metadata: Metadata,
}

impl RequestHead {
    pub fn parse_h2(headers: &[OwnedField]) -> Result<RequestHead, Status> {
        let method = HeaderBlock::find_one(headers, b":method");
        let path = HeaderBlock::find_one(headers, b":path");
        let authority = HeaderBlock::find_one(headers, b":authority");
        let content_type = HeaderBlock::find_one(headers, b"content-type");
        let te = HeaderBlock::find_one(headers, b"te");

        if method != Some(b"POST".as_slice()) {
            return Err(Status::new(Code::Unimplemented, "gRPC requires POST"));
        }
        let Some(path) = path else {
            return Err(Status::new(Code::Unimplemented, "missing :path"));
        };
        Self::validate_path(path)?;
        if !content_type.is_some_and(HeaderBlock::is_grpc_content_type) {
            return Err(Status::new(
                Code::InvalidArgument,
                "missing application/grpc content-type",
            ));
        }
        if te != Some(b"trailers".as_slice()) {
            return Err(Status::new(Code::InvalidArgument, "missing te: trailers"));
        }

        Ok(RequestHead {
            path: path.to_vec(),
            authority: authority.map(<[u8]>::to_vec),
            metadata: Metadata::from_h2_fields(headers)?,
        })
    }

    pub fn parse_h2_trailers(headers: &[OwnedField]) -> Result<Metadata, Status> {
        Metadata::from_h2_fields(headers)
    }

    fn validate_path(path: &[u8]) -> Result<(), Status> {
        if !path.starts_with(b"/") || path.iter().filter(|&&b| b == b'/').count() != 2 {
            return Err(Status::new(Code::Unimplemented, "invalid gRPC path"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead {
    pub metadata: Metadata,
}

impl ResponseHead {
    pub fn parse_h2(headers: &[OwnedField]) -> Result<ResponseHead, Status> {
        if HeaderBlock::find_one(headers, b":status") != Some(b"200".as_slice()) {
            return Err(Status::new(Code::Unknown, "non-200 HTTP/2 response"));
        }
        if !HeaderBlock::find_one(headers, b"content-type")
            .is_some_and(HeaderBlock::is_grpc_content_type)
        {
            return Err(Status::new(
                Code::Unknown,
                "missing application/grpc content-type",
            ));
        }
        Ok(ResponseHead {
            metadata: Metadata::from_h2_fields(headers)?,
        })
    }
}

impl Status {
    pub fn parse_h2_trailers(headers: &[OwnedField]) -> Result<(Status, Metadata), Status> {
        let raw_code = headers
            .iter()
            .find(|h| h.name == b"grpc-status")
            .map(|h| h.value.as_slice());
        let Some(raw_code) = raw_code else {
            return Err(Status::new(Code::Internal, "missing grpc-status"));
        };
        let Some(code) = Code::parse_ascii(raw_code) else {
            return Err(Status::new(Code::Internal, "invalid grpc-status"));
        };
        let message = headers
            .iter()
            .find(|h| h.name == b"grpc-message")
            .map(|h| Status::decode_message(&h.value))
            .unwrap_or_default();
        let metadata = Metadata::from_h2_fields(headers)?;
        Ok((Status::new(code, message), metadata))
    }
}

impl Metadata {
    pub fn from_h2_fields(headers: &[OwnedField]) -> Result<Metadata, Status> {
        let mut metadata = Metadata::new();
        for h in headers {
            if h.name.starts_with(b":") || Self::is_reserved(&h.name) {
                continue;
            }
            metadata
                .push(&h.name, &h.value)
                .map_err(Status::from_metadata_err)?;
        }
        Ok(metadata)
    }

    pub(super) fn is_reserved(name: &[u8]) -> bool {
        matches!(
            name,
            b"content-type" | b"te" | b"grpc-status" | b"grpc-message" | b"grpc-status-details-bin"
        )
    }
}

impl Status {
    pub fn from_metadata_err(err: MetadataError) -> Status {
        Status::new(Code::Internal, format!("bad metadata: {err:?}"))
    }
}
