use core::num::NonZero;

use crate::{
    field,
    field::{
        head,
        metadata::{block, map::record},
    },
    payload::response,
    uri::{self, path},
};

const METHOD: u32 = 1 << 0;
const SCHEME: u32 = 1 << 1;
const PATH: u32 = 1 << 2;
const STATUS: u32 = 1 << 3;
const AUTHORITY: u32 = 1 << 4;
const HOST: u32 = 1 << 5;
const PROTOCOL: u32 = 1 << 6;
const SAW_REGULAR: u32 = 1 << 7;
const KIND_SHIFT: u32 = 8;
const VALIDATION_SHIFT: u32 = 10;
const VALIDATION_MASK: u32 = 0b11 << VALIDATION_SHIFT;
const METHOD_SHIFT: u32 = 12;
const METHOD_MASK: u32 = 0b11 << METHOD_SHIFT;
const SCHEME_SHIFT: u32 = 14;
const SCHEME_MASK: u32 = 0b11 << SCHEME_SHIFT;
const EXTENDED_ENABLED: u32 = 1 << 16;
const PATH_ASTERISK: u32 = 1 << 17;
const REQUIRE_SCHEME_AUTHORITY: u32 = 1 << 18;
const RESPONSE_SHIFT: u32 = 19;
const RESPONSE_MASK: u32 = 0b11 << RESPONSE_SHIFT;
const AUTHORITY_PORT: u32 = 1 << 21;
const CONTENT_LENGTH: u32 = 1 << 22;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Method {
    Other,
    Options,
    Connect,
    Head,
}

impl Method {
    const fn response(self) -> response::Method {
        match self {
            Self::Other | Self::Options => response::Method::Other,
            Self::Connect => response::Method::Connect,
            Self::Head => response::Method::Head,
        }
    }
}

#[derive(Clone, Copy)]
enum Dibit {
    Zero,
    One,
    Two,
    Three,
}

impl Dibit {
    const fn read(word: u32, shift: u32) -> Self {
        match (word & (1 << shift) != 0, word & (2 << shift) != 0) {
            (false, false) => Self::Zero,
            (true, false) => Self::One,
            (false, true) => Self::Two,
            (true, true) => Self::Three,
        }
    }
}

/// Field-order-independent compressed-head admission with exact framing values.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    word: u32,
    length: u64,
    status: Option<NonZero<u16>>,
}

impl Gate {
    pub const fn new(kind: block::Kind, connect: block::Connect, version: block::Version) -> Self {
        let mut word = (kind as u32) << KIND_SHIFT;
        match connect {
            block::Connect::Disabled => {}
            block::Connect::Enabled => word |= EXTENDED_ENABLED,
        }
        match version {
            block::Version::Two => {}
            block::Version::Three => word |= REQUIRE_SCHEME_AUTHORITY,
        }
        Self {
            word,
            length: 0,
            status: None,
        }
    }

    pub fn kind(self) -> block::Kind {
        match Dibit::read(self.word, KIND_SHIFT) {
            Dibit::Zero => block::Kind::Request,
            Dibit::One => block::Kind::Response,
            Dibit::Two => block::Kind::Trailers,
            Dibit::Three => block::Kind::PushPromise,
        }
    }

    pub fn validation(self) -> block::Validation {
        match Dibit::read(self.word, VALIDATION_SHIFT) {
            Dibit::Zero => block::Validation::Valid,
            Dibit::One | Dibit::Three => block::Validation::Malformed,
            Dibit::Two => block::Validation::RejectPush,
        }
    }

    pub fn scheme(self) -> uri::Scheme {
        match Dibit::read(self.word, SCHEME_SHIFT) {
            Dibit::Zero => uri::Scheme::None,
            Dibit::One => uri::Scheme::Http,
            Dibit::Two => uri::Scheme::Https,
            Dibit::Three => uri::Scheme::Other,
        }
    }

    pub(crate) const fn framing(self) -> block::Framing {
        let method = if self.word & METHOD != 0 {
            Some(self.method().response())
        } else {
            None
        };
        block::Framing::new(
            self.length,
            self.status,
            method,
            self.word & CONTENT_LENGTH != 0,
        )
    }

    const fn method(self) -> Method {
        match Dibit::read(self.word, METHOD_SHIFT) {
            Dibit::Zero => Method::Other,
            Dibit::One => Method::Options,
            Dibit::Two => Method::Connect,
            Dibit::Three => Method::Head,
        }
    }

    pub fn reject(&mut self) {
        self.reject_with(block::Validation::Malformed);
    }

    pub fn reject_push(&mut self) {
        self.reject_with(block::Validation::RejectPush);
    }

    fn reject_with(&mut self, status: block::Validation) -> block::Validation {
        if self.validation() == block::Validation::Valid {
            self.word = (self.word & !VALIDATION_MASK) | ((status as u32) << VALIDATION_SHIFT);
        }
        self.validation()
    }

    fn mark(&mut self, bit: u32) -> block::Validation {
        if self.word & bit != 0 {
            self.reject();
            return self.validation();
        }
        self.word |= bit;
        block::Validation::Valid
    }

    pub fn accept_name(&mut self, name: head::Name) -> block::Validation {
        let check = self.validation();
        if check != block::Validation::Valid {
            return check;
        }
        if block::Name::from_head(name).is_forbidden() {
            return self.reject_with(block::Validation::Malformed);
        }
        let pseudo = matches!(
            name,
            head::Name::Method
                | head::Name::Scheme
                | head::Name::Path
                | head::Name::Status
                | head::Name::Authority
                | head::Name::Protocol
        );
        if pseudo && (self.word & SAW_REGULAR != 0 || self.kind() == block::Kind::Trailers) {
            return self.reject_with(block::Validation::Malformed);
        }
        match name {
            head::Name::ContentLength if self.kind() == block::Kind::Trailers => {
                self.reject_with(block::Validation::Malformed)
            }
            head::Name::Te
                if !matches!(self.kind(), block::Kind::Request | block::Kind::PushPromise) =>
            {
                self.reject_with(block::Validation::Malformed)
            }
            head::Name::Regular | head::Name::ContentLength | head::Name::Te => {
                self.word |= SAW_REGULAR;
                block::Validation::Valid
            }
            head::Name::Host => {
                self.word |= SAW_REGULAR;
                self.mark(HOST)
            }
            head::Name::Method
                if matches!(self.kind(), block::Kind::Request | block::Kind::PushPromise) =>
            {
                self.mark(METHOD)
            }
            head::Name::Scheme
                if matches!(self.kind(), block::Kind::Request | block::Kind::PushPromise) =>
            {
                self.mark(SCHEME)
            }
            head::Name::Path
                if matches!(self.kind(), block::Kind::Request | block::Kind::PushPromise) =>
            {
                self.mark(PATH)
            }
            head::Name::Authority
                if matches!(self.kind(), block::Kind::Request | block::Kind::PushPromise) =>
            {
                self.mark(AUTHORITY)
            }
            head::Name::Protocol if self.kind() == block::Kind::Request => self.mark(PROTOCOL),
            head::Name::Status if self.kind() == block::Kind::Response => self.mark(STATUS),
            _ => self.reject_with(block::Validation::Malformed),
        }
    }

    pub fn accept_value(&mut self, name: head::Name, value: record::Span<'_>) -> block::Validation {
        if self.validation() != block::Validation::Valid {
            return self.validation();
        }
        match name {
            head::Name::Method => self.check_method(value),
            head::Name::Status => self.check_status(value),
            head::Name::ContentLength => match self.check_length(value) {
                Ok(()) => block::Validation::Valid,
                Err(check) => self.reject_with(check),
            },
            head::Name::Scheme => match uri::Scheme::parse_span(value) {
                Ok(scheme) => {
                    self.word = (self.word & !SCHEME_MASK) | ((scheme as u32) << SCHEME_SHIFT);
                    block::Validation::Valid
                }
                Err(
                    uri::Error::InvalidScheme
                    | uri::Error::Empty
                    | uri::Error::InvalidHost
                    | uri::Error::InvalidPort,
                ) => self.reject_with(block::Validation::Malformed),
            },
            head::Name::Path => {
                if value.is_equal(b"*") {
                    self.word |= PATH_ASTERISK;
                } else if value.get(0) != Some(b'/') || path::complete_span(value).is_err() {
                    return self.reject_with(block::Validation::Malformed);
                }
                block::Validation::Valid
            }
            head::Name::Authority | head::Name::Host => match uri::Parsed::parse(value) {
                Ok(parsed) => {
                    if name == head::Name::Authority {
                        self.word = (self.word & !AUTHORITY_PORT)
                            | (u32::from(parsed.has_port()) * AUTHORITY_PORT);
                    }
                    block::Validation::Valid
                }
                Err(
                    uri::Error::Empty
                    | uri::Error::InvalidHost
                    | uri::Error::InvalidPort
                    | uri::Error::InvalidScheme,
                ) => {
                    if self.kind() == block::Kind::PushPromise && name == head::Name::Authority {
                        self.reject_with(block::Validation::RejectPush)
                    } else {
                        self.reject_with(block::Validation::Malformed)
                    }
                }
            },
            head::Name::Protocol => {
                if !value.is_empty() && value.iter().copied().all(field::Error::is_name_byte) {
                    block::Validation::Valid
                } else {
                    self.reject_with(block::Validation::Malformed)
                }
            }
            head::Name::Invalid
            | head::Name::Connection
            | head::Name::TransferEncoding
            | head::Name::Expect
            | head::Name::AcceptEncoding
            | head::Name::Te
            | head::Name::KeepAlive
            | head::Name::ProxyConnection
            | head::Name::Upgrade
            | head::Name::Regular => block::Validation::Valid,
        }
    }

    pub fn finish(self) -> block::Outcome {
        if self.validation() != block::Validation::Valid {
            return block::Outcome::Rejected(self.validation());
        }
        if self.word & CONTENT_LENGTH != 0 && self.status.is_some_and(|status| status.get() < 200) {
            return block::Outcome::Rejected(block::Validation::Malformed);
        }
        if self.word & PATH_ASTERISK != 0
            && self.word & METHOD_MASK != (Method::Options as u32) << METHOD_SHIFT
        {
            return block::Outcome::Rejected(block::Validation::Malformed);
        }
        match self.kind() {
            block::Kind::Response if self.word & STATUS == 0 => {
                block::Outcome::Rejected(block::Validation::Malformed)
            }
            block::Kind::Trailers => block::Outcome::Other,
            block::Kind::PushPromise => {
                let required = METHOD | SCHEME | PATH | AUTHORITY;
                if self.word & required == required && self.word & PROTOCOL == 0 {
                    block::Outcome::Other
                } else if self.word & (METHOD | SCHEME | PATH) != METHOD | SCHEME | PATH {
                    block::Outcome::Rejected(block::Validation::Malformed)
                } else {
                    block::Outcome::Rejected(block::Validation::RejectPush)
                }
            }
            block::Kind::Request => self.plan_request(),
            block::Kind::Response => match Dibit::read(self.word, RESPONSE_SHIFT) {
                Dibit::One => block::Outcome::Response(block::Response::Informational),
                Dibit::Two => block::Outcome::Response(block::Response::Final),
                Dibit::Zero | Dibit::Three => {
                    block::Outcome::Rejected(block::Validation::Malformed)
                }
            },
        }
    }

    fn check_status(&mut self, value: record::Span<'_>) -> block::Validation {
        let mut bytes = value.iter().copied();
        let (status, class) = match (bytes.next(), bytes.next(), bytes.next(), bytes.next()) {
            (Some(b'1'), Some(b'0'), Some(b'1'), None) => {
                return self.reject_with(block::Validation::Malformed);
            }
            (
                Some(first @ b'1'..=b'5'),
                Some(second @ b'0'..=b'9'),
                Some(third @ b'0'..=b'9'),
                None,
            ) => (
                u16::from(first - b'0') * 100
                    + u16::from(second - b'0') * 10
                    + u16::from(third - b'0'),
                if first == b'1' { 1 } else { 2 },
            ),
            _ => return self.reject_with(block::Validation::Malformed),
        };
        self.status = Some(NonZero::<u16>::MIN.saturating_add(status - 1));
        self.word = (self.word & !RESPONSE_MASK) | (class << RESPONSE_SHIFT);
        block::Validation::Valid
    }

    fn check_length(&mut self, value: record::Span<'_>) -> Result<(), block::Validation> {
        let invalid = if self.kind() == block::Kind::PushPromise {
            block::Validation::RejectPush
        } else {
            block::Validation::Malformed
        };
        let mut length = 0u64;
        let mut digits = false;
        let mut trailing = false;
        for byte in value.iter().copied().chain(core::iter::once(b',')) {
            match byte {
                b'0'..=b'9' if !trailing => {
                    length = length
                        .checked_mul(10)
                        .and_then(|length| length.checked_add(u64::from(byte - b'0')))
                        .ok_or(invalid)?;
                    digits = true;
                }
                b' ' | b'\t' => trailing = digits,
                b',' if digits => {
                    if self.word & CONTENT_LENGTH != 0 && self.length != length {
                        return Err(invalid);
                    }
                    self.word |= CONTENT_LENGTH;
                    self.length = length;
                    length = 0;
                    digits = false;
                    trailing = false;
                }
                _ => return Err(invalid),
            }
        }
        Ok(())
    }

    fn check_method(&mut self, value: record::Span<'_>) -> block::Validation {
        if value.is_empty() || !value.iter().copied().all(field::Error::is_name_byte) {
            return self.reject_with(block::Validation::Malformed);
        }
        if self.kind() == block::Kind::PushPromise
            && !value.is_equal(b"GET")
            && !value.is_equal(b"HEAD")
        {
            return self.reject_with(block::Validation::RejectPush);
        }
        let method = if value.is_equal(b"CONNECT") {
            Method::Connect
        } else if value.is_equal(b"OPTIONS") {
            Method::Options
        } else if value.is_equal(b"HEAD") {
            Method::Head
        } else {
            Method::Other
        };
        self.word = (self.word & !METHOD_MASK) | ((method as u32) << METHOD_SHIFT);
        block::Validation::Valid
    }

    fn plan_request(self) -> block::Outcome {
        if self.word & METHOD == 0 {
            return block::Outcome::Rejected(block::Validation::Malformed);
        }
        let has = |bit| self.word & bit != 0;
        let scheme = self.scheme();
        let method = self.method();
        let source = if has(AUTHORITY) {
            block::Authority::Pseudo
        } else if has(HOST) {
            block::Authority::Host
        } else {
            block::Authority::None
        };
        let allowed = |source| {
            self.word & REQUIRE_SCHEME_AUTHORITY == 0
                || !matches!(scheme, uri::Scheme::Http | uri::Scheme::Https)
                || source != block::Authority::None
        };

        if method == Method::Connect && !has(PROTOCOL) {
            if has(SCHEME) || has(PATH) || !has(AUTHORITY) || !has(AUTHORITY_PORT) {
                return block::Outcome::Rejected(block::Validation::Malformed);
            }
            return block::Outcome::Request(block::Plan::new(
                block::Form::Connect,
                uri::Scheme::None,
                block::Authority::Target,
                method.response(),
            ));
        }

        if method == Method::Connect {
            if self.word & EXTENDED_ENABLED == 0 || !has(SCHEME) || !has(PATH) || !allowed(source) {
                return block::Outcome::Rejected(block::Validation::Malformed);
            }
            return block::Outcome::Request(block::Plan::new(
                block::Form::ExtendedConnect,
                scheme,
                source,
                method.response(),
            ));
        }

        if has(PROTOCOL) || !has(SCHEME) || !has(PATH) || !allowed(source) {
            return block::Outcome::Rejected(block::Validation::Malformed);
        }
        let form = if self.word & PATH_ASTERISK == 0 {
            block::Form::Path
        } else {
            block::Form::Asterisk
        };
        block::Outcome::Request(block::Plan::new(form, scheme, source, method.response()))
    }
}

const _: () = {
    assert!(core::mem::size_of::<Dibit>() == 1);
    assert!(core::mem::size_of::<Gate>() == 16);
};
