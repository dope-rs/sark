//! Request-head semantics bound to the scanned input.

pub mod offers;
mod parts;
mod plan;
pub mod pool;
mod route;
pub mod scan;
pub mod trace;

pub use parts::Parts;
pub(in crate::h1::head::request) use plan::Kind;
pub use plan::Plan;
pub use route::Route;

use crate::{
    field::head::Name,
    h1::{
        field::{self, Connection, Transfer, expect},
        framing,
        head::{Semantic, line},
        switch, target,
    },
    payload::response::Method,
    request::{self, span},
};

#[derive(Clone, Copy, Default)]
pub struct Fields {
    pub message: Message,
    pub routing: Routing,
}

impl Fields {
    fn apply(mut self, field: &field::Entry<'_>, version: line::Version) -> Result<Self, Semantic> {
        let value = field.value();
        match field.kind() {
            Name::ContentLength => {
                self.message.framing = self
                    .message
                    .framing
                    .try_length(value)
                    .map_err(Semantic::Field)?;
            }
            Name::TransferEncoding => {
                self.message = self.message.try_coding(value).map_err(Semantic::Field)?;
            }
            Name::Host => {
                self.routing.authority = self
                    .routing
                    .authority
                    .try_host(value)
                    .map_err(Semantic::Host)?;
                self.routing.host = Some(field.value_span());
            }
            Name::Expect => {
                self.message = self.message.with_expect(value);
            }
            Name::Connection => {
                self.routing.connection = self
                    .routing
                    .connection
                    .try_push(value)
                    .map_err(Semantic::Connection)?;
            }
            Name::Upgrade => {
                self.routing = self
                    .routing
                    .try_upgrade(value, version)
                    .map_err(Semantic::Upgrade)?;
            }
            Name::Invalid
            | Name::Method
            | Name::Scheme
            | Name::Path
            | Name::Status
            | Name::Authority
            | Name::Protocol
            | Name::AcceptEncoding
            | Name::Te
            | Name::KeepAlive
            | Name::ProxyConnection
            | Name::Regular => {}
        }
        Ok(self)
    }

    pub fn admit_target(
        self,
        target: target::Target,
        version: &[u8],
        policy: target::Policy,
    ) -> Result<framing::Admission, Semantic> {
        framing::Admission::admit(
            self.message.framing,
            self.routing.authority,
            self.message.expectation,
            target,
            version,
            policy,
        )
        .map_err(Semantic::Admission)
    }

    pub fn transition(
        self,
        method: Method,
        version: line::Version,
    ) -> Result<switch::Intent, Semantic> {
        switch::Intent::from_request(
            method,
            self.routing.connection,
            self.routing.upgrades,
            version,
        )
        .map_err(Semantic::Transition)
    }

    pub(super) fn admit_parts(
        self,
        target: target::Target,
        range: span::Span,
        version: line::Version,
    ) -> Result<(framing::Admission, request::Layout, span::Span), Semantic> {
        let admit = self.admit_target(target, version.as_bytes(), target::Policy::Require)?;
        let auth = admit
            .authority()
            .span(target, self.routing.host)
            .map_err(Semantic::Authority)?;
        let request = target.layout(range, auth).map_err(Semantic::Authority)?;
        Ok((admit, request, auth))
    }

    pub(super) const fn connection(self) -> Connection {
        self.routing.connection
    }

    pub(super) const fn upgrades(self) -> switch::Offers {
        self.routing.upgrades
    }
}

const _: () = assert!(core::mem::size_of::<Fields>() <= 6 * core::mem::size_of::<usize>());

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Coding {
    #[default]
    Plain,
    Gzip,
}

const _: () = assert!(core::mem::size_of::<Coding>() == core::mem::size_of::<bool>());

#[derive(Clone, Copy, Default)]
pub struct Message {
    framing: framing::Fields,
    expectation: expect::State,
    coding: Coding,
}

impl Message {
    pub fn try_length(mut self, length: usize) -> Result<Self, field::Error> {
        self.framing = self.framing.try_set(length)?;
        Ok(self)
    }

    pub fn try_coding(mut self, value: &[u8]) -> Result<Self, field::Error> {
        self.framing = self.framing.try_coding(value)?;
        Ok(self)
    }

    pub fn try_append(mut self, value: Transfer) -> Result<Self, field::Error> {
        self.framing = self.framing.try_append(value)?;
        Ok(self)
    }

    pub fn with_expect(mut self, value: &[u8]) -> Self {
        self.expectation = self.expectation.push(value);
        self
    }

    pub const fn length(self) -> Option<usize> {
        self.framing.length()
    }

    pub const fn has_coding(self) -> bool {
        self.framing.has_coding()
    }

    pub const fn is_chunked(self) -> bool {
        self.framing.is_chunked()
    }

    /// Whether transfer codings remain after removing the final chunked framing.
    pub const fn has_other_transfer_coding(self) -> bool {
        self.framing.has_other()
    }

    pub(crate) fn set_coding(&mut self, coding: Coding) {
        self.coding = coding;
    }

    pub const fn coding(self) -> Coding {
        self.coding
    }
}

#[derive(Clone, Copy, Default)]
pub struct Routing {
    pub(crate) authority: target::Request,
    pub(crate) connection: Connection,
    pub(in crate::h1::head::request) host: Option<span::Span>,
    pub(in crate::h1::head::request) upgrades: switch::Offers,
}

impl Routing {
    pub const fn connection(self) -> Connection {
        self.connection
    }

    pub fn try_host(mut self, span: span::Span) -> Result<Self, target::Invalid> {
        if !self.authority.has_host() || self.host.is_some() {
            return Err(target::Invalid);
        }
        self.host = Some(span);
        Ok(self)
    }

    pub const fn host_span(self) -> Option<span::Span> {
        self.host
    }

    pub fn try_upgrade(
        mut self,
        value: &[u8],
        version: line::Version,
    ) -> Result<Self, switch::UpgradeError> {
        if version == line::Version::Ten {
            return Ok(self);
        }
        let next = self.upgrades.try_push(value)?;
        self.upgrades = next;
        Ok(self)
    }
}
