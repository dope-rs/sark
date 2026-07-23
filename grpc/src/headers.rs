use sark_core::http::{Field, FieldBlock as Fields, FieldStorage, VecFieldBlock};

use crate::metadata::Metadata;
use crate::status::{Code, Status};

pub(crate) struct ParsedFields<'a> {
    pub(crate) method: Option<&'a [u8]>,
    pub(crate) path: Option<&'a [u8]>,
    pub(crate) authority: Option<&'a [u8]>,
    pub(crate) status: Option<&'a [u8]>,
    pub(crate) content_type: Option<&'a [u8]>,
    pub(crate) te: Option<&'a [u8]>,
    pub(crate) grpc_status: Option<&'a [u8]>,
    pub(crate) grpc_message: Option<&'a [u8]>,
    pub(crate) metadata: Metadata,
}

impl<'a> ParsedFields<'a> {
    pub(crate) fn parse<S: FieldStorage>(fields: &'a Fields<S>) -> Result<Self, Status> {
        let mut parsed = Self {
            method: None,
            path: None,
            authority: None,
            status: None,
            content_type: None,
            te: None,
            grpc_status: None,
            grpc_message: None,
            metadata: Metadata::new(),
        };
        for field in fields {
            let slot = match field.name {
                b":method" => Some(&mut parsed.method),
                b":path" => Some(&mut parsed.path),
                b":authority" => Some(&mut parsed.authority),
                b":status" => Some(&mut parsed.status),
                b"content-type" => Some(&mut parsed.content_type),
                b"te" => Some(&mut parsed.te),
                b"grpc-status" => Some(&mut parsed.grpc_status),
                b"grpc-message" => Some(&mut parsed.grpc_message),
                name if name.starts_with(b":") || Metadata::is_reserved(name) => None,
                name => {
                    parsed
                        .metadata
                        .push(name, field.value)
                        .map_err(Status::from_metadata_err)?;
                    None
                }
            };
            if let Some(slot) = slot {
                slot.get_or_insert(field.value);
            }
        }
        Ok(parsed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderBlock {
    fields: VecFieldBlock,
}

impl HeaderBlock {
    const DEFAULT_CAPACITY: usize = 256;

    pub fn new() -> Self {
        Self {
            fields: VecFieldBlock::with_capacity(Self::DEFAULT_CAPACITY),
        }
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) {
        self.fields.push(name, value);
    }

    pub fn iter(&self) -> impl Iterator<Item = Field<'_>> {
        self.fields.iter()
    }

    pub fn is_grpc_content_type(value: &[u8]) -> bool {
        value == b"application/grpc" || value.starts_with(b"application/grpc+")
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
        let (code, code_len) = status.grpc_status_value();
        out.push(b"grpc-status", &code[..code_len]);
        if !status.message().is_empty() {
            let message = status.message();
            out.fields.push_encoded(
                b"grpc-message",
                Status::encoded_message_len(message),
                |writer| Status::encode_message(message, |byte| writer.push(byte)),
            );
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

impl Default for HeaderBlock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead {
    pub path: Vec<u8>,
    pub authority: Option<Vec<u8>>,
    pub metadata: Metadata,
}

impl RequestHead {
    pub fn parse_h2<S: FieldStorage>(headers: &Fields<S>) -> Result<RequestHead, Status> {
        let parsed = ParsedFields::parse(headers)?;

        if parsed.method != Some(b"POST".as_slice()) {
            return Err(Status::new(Code::Unimplemented, "gRPC requires POST"));
        }
        let Some(path) = parsed.path else {
            return Err(Status::new(Code::Unimplemented, "missing :path"));
        };
        Self::validate_path(path)?;
        if !parsed
            .content_type
            .is_some_and(HeaderBlock::is_grpc_content_type)
        {
            return Err(Status::new(
                Code::InvalidArgument,
                "missing application/grpc content-type",
            ));
        }
        if parsed.te != Some(b"trailers".as_slice()) {
            return Err(Status::new(Code::InvalidArgument, "missing te: trailers"));
        }

        Ok(RequestHead {
            path: path.to_vec(),
            authority: parsed.authority.map(<[u8]>::to_vec),
            metadata: parsed.metadata,
        })
    }

    pub fn parse_h2_trailers<S: FieldStorage>(headers: &Fields<S>) -> Result<Metadata, Status> {
        Ok(ParsedFields::parse(headers)?.metadata)
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
    pub fn parse_h2<S: FieldStorage>(headers: &Fields<S>) -> Result<ResponseHead, Status> {
        let parsed = ParsedFields::parse(headers)?;
        if parsed.status != Some(b"200".as_slice()) {
            return Err(Status::new(Code::Unknown, "non-200 HTTP/2 response"));
        }
        if !parsed
            .content_type
            .is_some_and(HeaderBlock::is_grpc_content_type)
        {
            return Err(Status::new(
                Code::Unknown,
                "missing application/grpc content-type",
            ));
        }
        Ok(ResponseHead {
            metadata: parsed.metadata,
        })
    }
}
