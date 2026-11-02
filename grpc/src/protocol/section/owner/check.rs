use sark_http::{
    field::{
        Indexing, head,
        metadata::{block, map::record, section::rules},
    },
    payload::response::status,
};

use crate::{
    details::Fault,
    protocol::{
        request::{self, timeout},
        section::{Seal, seal},
    },
};

#[derive(Clone, Copy)]
pub(super) enum Field {
    Ignore,
    Copy,
    Text,
    Binary,
    Method,
    Scheme,
    Path,
    Authority,
    HttpStatus,
    ContentType,
    Encoding,
    Te,
    GrpcStatus,
    Message,
    Details,
}

pub(super) struct Check {
    pub(super) seal: Seal,
}

impl Check {
    pub(super) fn new(kind: rules::Kind) -> Self {
        Self {
            seal: Seal::new(kind),
        }
    }

    pub(super) fn field(
        &mut self,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: Indexing,
    ) -> Result<Field, timeout::Error> {
        if self.seal.has_fault() || (self.seal.is_info() && !name.starts_with(b":")) {
            return Ok(Field::Ignore);
        }
        if self.seal.kind() == rules::Kind::Request && name.is_equal(b"grpc-timeout") {
            if self.seal.observe(seal::Flag::Timeout) == seal::Observation::Duplicate {
                return Err(timeout::Error::Duplicate);
            }
            self.seal.set_timeout(timeout::Value::parse(value)?);
            return Ok(Field::Copy);
        }
        let field = self.classify(name, block::Name::from_span(name).head());
        self.validate(field, value, indexing);
        Ok(field)
    }

    fn classify(&mut self, name: record::Span<'_>, known: head::Name) -> Field {
        let (field, flag) = match known {
            head::Name::Method => (Field::Method, Some(seal::Flag::Method)),
            head::Name::Scheme => (Field::Scheme, None),
            head::Name::Path => (Field::Path, Some(seal::Flag::Path)),
            head::Name::Authority => (Field::Authority, Some(seal::Flag::Authority)),
            head::Name::Status => (Field::HttpStatus, Some(seal::Flag::HttpStatus)),
            head::Name::Te => (Field::Te, Some(seal::Flag::Te)),
            head::Name::Regular if name.is_equal(b"content-type") => {
                (Field::ContentType, Some(seal::Flag::ContentType))
            }
            head::Name::Regular if name.is_equal(b"grpc-encoding") => {
                (Field::Encoding, Some(seal::Flag::Encoding))
            }
            head::Name::Regular if name.is_equal(b"grpc-status") => {
                (Field::GrpcStatus, Some(seal::Flag::GrpcStatus))
            }
            head::Name::Regular if name.is_equal(b"grpc-message") => {
                (Field::Message, Some(seal::Flag::Message))
            }
            head::Name::Regular if name.is_equal(b"grpc-status-details-bin") => {
                (Field::Details, Some(seal::Flag::Details))
            }
            head::Name::Regular => match crate::metadata::Metadata::check_span(name) {
                Ok(crate::metadata::Encoding::Ascii) => (Field::Text, None),
                Ok(crate::metadata::Encoding::Binary) => (Field::Binary, None),
                Err(crate::metadata::Error::ReservedName) => (
                    if name.starts_with(b"grpc-") {
                        Field::Ignore
                    } else {
                        Field::Text
                    },
                    None,
                ),
                Err(
                    crate::metadata::Error::EmptyName
                    | crate::metadata::Error::InvalidName { .. }
                    | crate::metadata::Error::EmptyValue
                    | crate::metadata::Error::InvalidValue { .. }
                    | crate::metadata::Error::Pack(_),
                ) => {
                    self.seal.fault(Fault::Metadata);
                    (Field::Ignore, None)
                }
            },
            _ => (
                if name.starts_with(b":") {
                    Field::Copy
                } else {
                    Field::Text
                },
                None,
            ),
        };
        let allowed = match field {
            Field::Method | Field::Scheme | Field::Path | Field::Authority | Field::Te => {
                self.seal.kind() == rules::Kind::Request
            }
            Field::HttpStatus => self.seal.kind() == rules::Kind::Response,
            Field::ContentType | Field::Encoding => matches!(
                self.seal.kind(),
                rules::Kind::Request | rules::Kind::Response
            ),
            Field::GrpcStatus | Field::Message | Field::Details => matches!(
                self.seal.kind(),
                rules::Kind::Response | rules::Kind::ResponseTrailers
            ),
            Field::Ignore | Field::Copy | Field::Text | Field::Binary => true,
        };
        if !allowed {
            self.seal.fault(Fault::Metadata);
            return Field::Ignore;
        }
        if flag.is_some_and(|flag| self.seal.observe(flag) == seal::Observation::Duplicate) {
            self.seal.fault(match field {
                Field::GrpcStatus => Fault::DuplicateStatus,
                _ => Fault::Duplicate,
            });
            Field::Ignore
        } else {
            field
        }
    }

    fn validate(&mut self, field: Field, value: record::Span<'_>, indexing: Indexing) {
        let valid = match field {
            Field::Method => value.is_equal(b"POST"),
            Field::Scheme => value.is_equal(b"http") || value.is_equal(b"https"),
            Field::Path => request::is_path(value.iter()),
            Field::HttpStatus => match Self::http_status(value) {
                Some(status) => {
                    self.seal.set_http(status);
                    true
                }
                None => false,
            },
            Field::ContentType => {
                let grpc =
                    value.is_equal(b"application/grpc") || value.starts_with(b"application/grpc+");
                if grpc {
                    self.seal.observe(seal::Flag::GrpcContentType);
                }
                grpc || self.seal.is_non_ok()
            }
            Field::Te => value.is_equal(b"trailers"),
            // Valid retains this admission; messages only need to admit flag zero.
            Field::Encoding => value.is_equal(b"identity"),
            Field::GrpcStatus => match crate::Code::parse_span(value) {
                Some(code) => {
                    self.seal.set_code(code, indexing);
                    true
                }
                None => false,
            },
            Field::Ignore
            | Field::Copy
            | Field::Text
            | Field::Binary
            | Field::Authority
            | Field::Message
            | Field::Details => true,
        };
        if !valid {
            let fault = match field {
                Field::Method => Fault::Method,
                Field::Path => Fault::Path,
                Field::HttpStatus => Fault::HttpStatus,
                Field::ContentType => Fault::ContentType,
                Field::Encoding => match self.seal.kind() {
                    rules::Kind::Request => Fault::RequestEncoding,
                    _ => Fault::ResponseEncoding,
                },
                Field::Te => Fault::Te,
                Field::GrpcStatus => Fault::GrpcStatus,
                Field::Scheme | Field::Text | Field::Binary => Fault::Metadata,
                _ => Fault::Capacity,
            };
            self.seal.fault(fault);
        }
    }

    fn http_status(value: record::Span<'_>) -> Option<status::Code> {
        let mut bytes = value.iter().copied();
        match (bytes.next(), bytes.next(), bytes.next(), bytes.next()) {
            (
                Some(hundreds @ b'0'..=b'9'),
                Some(tens @ b'0'..=b'9'),
                Some(ones @ b'0'..=b'9'),
                None,
            ) => {
                let value = u16::from(hundreds - b'0') * 100
                    + u16::from(tens - b'0') * 10
                    + u16::from(ones - b'0');
                status::Code::from_u16(value).ok()
            }
            _ => None,
        }
    }

    pub(super) fn require(&mut self) {
        let required = match self.seal.kind() {
            rules::Kind::Request => [
                (seal::Flag::Method, Fault::Method),
                (seal::Flag::Path, Fault::Path),
                (seal::Flag::ContentType, Fault::ContentType),
                (seal::Flag::Te, Fault::Te),
            ]
            .as_slice(),
            rules::Kind::Response if self.seal.is_non_ok() => {
                [(seal::Flag::HttpStatus, Fault::HttpStatus)].as_slice()
            }
            rules::Kind::Response => [
                (seal::Flag::HttpStatus, Fault::HttpStatus),
                (seal::Flag::ContentType, Fault::ContentType),
            ]
            .as_slice(),
            rules::Kind::ResponseTrailers | rules::Kind::RequestTrailers => [].as_slice(),
            rules::Kind::PushPromise => {
                self.seal.fault(Fault::Metadata);
                [].as_slice()
            }
        };
        for &(flag, fault) in required {
            if !self.seal.has_flag(flag) {
                self.seal.fault(fault);
            }
        }
    }
}
