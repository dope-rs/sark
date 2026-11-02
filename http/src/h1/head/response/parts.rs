use core::convert::Infallible;
use std::num::NonZero;

use line::error;

use crate::{
    field::head::Name,
    h1::{
        field::{self, Connection, KeepAlive},
        framing,
        head::{
            Boundary, Limit, ParseError, Semantic, line,
            response::{
                Admitted,
                fields::{Fields, Flags},
            },
            state,
            tail::Tail,
        },
        switch,
    },
    payload::response::{Method, status::Code},
    request::span,
};

#[derive(Clone, Copy)]
pub struct Parts {
    coding: super::Coding,
    pub framing: framing::Message,
    pub connection: Connection,
    pub keep_alive: KeepAlive,
    pub upgrade: Option<span::Span>,
    version: line::Version,
    status: Code,
    tail: Tail,
}

#[derive(Clone, Copy)]
pub(super) struct Start {
    version: line::Version,
    status: Code,
    fields: u32,
}

#[derive(Clone, Copy)]
pub(super) enum Kind {}

impl Start {
    pub(super) fn from(line: line::Response<'_>) -> Result<Self, Boundary> {
        let end = line.end();
        if end > u32::MAX as usize {
            return Err(Boundary::TooLarge {
                actual: end,
                limit: u32::MAX as usize,
            });
        }
        Ok(Self {
            version: line.version(),
            status: line.status(),
            fields: end as u32,
        })
    }
}

impl Parts {
    pub fn parse<'owner, E>(
        bytes: &'owner [u8],
        limit: Limit,
        sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<Option<Self>, ParseError<E>> {
        let (_, parts) = state::State::<Kind>::new(limit).parse(bytes, sink)?;
        Ok(parts)
    }

    pub fn scan(bytes: &[u8], limit: Limit) -> Result<Option<Self>, ParseError<Infallible>> {
        let (_, parts) = state::State::<Kind>::new(limit).scan(bytes)?;
        Ok(parts)
    }

    pub const fn version(self) -> line::Version {
        self.version
    }

    pub const fn status(self) -> Code {
        self.status
    }

    pub const fn fields(self) -> span::Span {
        self.tail.fields()
    }

    pub const fn wire_len(self) -> NonZero<u32> {
        self.tail.wire_len()
    }

    /// Finishes response framing and connection reuse without retaining the
    /// wire owner or materializing fields.
    pub fn admit(self, request: Method) -> Result<Admitted, framing::Error> {
        let version = match self.version {
            line::Version::Ten => 0,
            line::Version::Eleven => 1,
        };
        let body = self.framing.response(request, self.status.as_u16())?;
        Ok(Admitted {
            coding: self.coding,
            fields: self.fields(),
            body,
            control: framing::Control::new(
                version,
                self.status.as_u16(),
                body,
                self.connection,
                self.keep_alive,
            ),
            wire: self.wire_len(),
            status: self.status,
            version,
        })
    }
}

impl state::Kind for Kind {
    type Start = Start;
    type Fields = Fields;
    type Parts = Parts;

    fn start<E>(bytes: &[u8]) -> Result<line::Progress<Start>, ParseError<E>> {
        let progress = match line::Response::progress(bytes) {
            Ok(progress) => progress,
            Err(error::Response::Syntax) => return Err(ParseError::invalid()),
            Err(error::Response::Status(cause)) => {
                return Err(ParseError::Semantic(Semantic::Status(cause)));
            }
        };
        match progress {
            line::Progress::Complete(line) => match Start::from(line) {
                Ok(start) => Ok(line::Progress::Complete(start)),
                Err(cause) => Err(ParseError::Boundary(cause)),
            },
            line::Progress::Partial(cursor) => Ok(line::Progress::Partial(cursor)),
        }
    }

    fn fields(start: Start) -> u32 {
        start.fields
    }

    fn observe(
        _: &Start,
        mut fields: Fields,
        field: &field::Entry<'_>,
    ) -> Result<Fields, Semantic> {
        match field.kind() {
            Name::Regular if field.name().eq_ignore_ascii_case(b"content-encoding") => {
                fields.coding = fields.coding.observe(field.value());
            }
            Name::ContentLength => {
                fields.framing = fields
                    .framing
                    .try_length(field.value())
                    .map_err(Semantic::Field)?;
            }
            Name::TransferEncoding => {
                fields.framing = fields
                    .framing
                    .try_coding(field.value())
                    .map_err(Semantic::Field)?;
            }
            Name::Connection => {
                fields.connection = fields
                    .connection
                    .try_push(field.value())
                    .map_err(Semantic::Connection)?;
            }
            Name::KeepAlive => {
                if let Some(timeout) = KeepAlive::parse(fields.timeout(), field.value()) {
                    fields.timeout_secs = timeout;
                    fields.flags.insert(Flags::TIMEOUT);
                }
            }
            Name::Upgrade => {
                switch::Offers::validate(field.value()).map_err(Semantic::Upgrade)?;
                if !fields.flags.contains(Flags::UPGRADE_AMBIGUOUS) {
                    if fields.upgrade.is_some() {
                        fields.upgrade = None;
                        fields.flags.insert(Flags::UPGRADE_AMBIGUOUS);
                    } else {
                        fields.upgrade = Some(field.value_span());
                    }
                }
            }
            Name::Invalid
            | Name::Method
            | Name::Scheme
            | Name::Path
            | Name::Status
            | Name::Authority
            | Name::Protocol
            | Name::Host
            | Name::Expect
            | Name::AcceptEncoding
            | Name::Te
            | Name::ProxyConnection
            | Name::Regular => {}
        }
        Ok(fields)
    }

    fn finish<E>(fields: Fields, start: Start, tail: Tail) -> Result<Parts, ParseError<E>> {
        let coding = if fields.coding == super::Coding::Gzip && fields.framing.has_other() {
            super::Coding::Unsupported
        } else {
            fields.coding
        };
        let framing = fields
            .framing
            .finish()
            .try_version(start.version.as_bytes())
            .map_err(|cause| ParseError::Semantic(Semantic::Framing(cause)))?;
        Ok(Parts {
            coding,
            framing,
            connection: fields.connection,
            keep_alive: KeepAlive::new(fields.timeout()),
            upgrade: fields.upgrade,
            version: start.version,
            status: start.status,
            tail,
        })
    }
}
