use crate::field::{head, metadata::block};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Start,
    Regular,
    Invalid,
    Pseudo,
    PseudoP,
    PseudoS,
    C,
    T,
    Method,
    Scheme,
    Path,
    Status,
    Authority,
    Protocol,
    Host,
    Length,
    Te,
    Connection,
    Keep,
    Proxy,
    Transfer,
    Upgrade,
}

/// Borrow-free streaming scan of one compressed field name.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    kind: Kind,
    matched: u8,
}

impl Default for Scan {
    fn default() -> Self {
        Self::new()
    }
}

impl Scan {
    pub const fn new() -> Self {
        Self {
            kind: Kind::Start,
            matched: 0,
        }
    }

    pub const fn push(&mut self, byte: u8) {
        if matches!(self.kind, Kind::Invalid) {
            return;
        }
        if byte.is_ascii_uppercase()
            || (!crate::field::Error::is_name_byte(byte)
                && !(byte == b':' && matches!(self.kind, Kind::Start)))
        {
            self.kind = Kind::Invalid;
            return;
        }
        match self.kind {
            Kind::Start => {
                self.matched = 1;
                self.kind = Kind::after_start(byte);
            }
            Kind::Regular => {}
            Kind::Pseudo => {
                self.matched = 2;
                self.kind = Kind::after_pseudo(byte);
            }
            Kind::PseudoP => {
                self.matched = 3;
                self.kind = Kind::after_p(byte);
            }
            Kind::PseudoS => {
                self.matched = 3;
                self.kind = Kind::after_s(byte);
            }
            Kind::C => {
                if self.matched == 1 && byte == b'o' {
                    self.matched = 2;
                } else if self.matched == 2 && byte == b'n' {
                    self.matched = 3;
                } else if self.matched == 3 && byte == b'n' {
                    self.kind = Kind::Connection;
                    self.matched = 4;
                } else if self.matched == 3 && byte == b't' {
                    self.kind = Kind::Length;
                    self.matched = 4;
                } else {
                    self.kind = Kind::Regular;
                }
            }
            Kind::T => {
                self.matched = 2;
                self.kind = Kind::after_t(byte);
            }
            kind => {
                let name = kind.name();
                let expected = name.wire();
                let index = self.matched as usize;
                if index < expected.len() && expected[index] == byte {
                    self.matched += 1;
                } else if !expected.is_empty() && expected[0] == b':' {
                    self.kind = Kind::Invalid;
                } else {
                    self.kind = Kind::Regular;
                }
            }
        }
    }

    pub const fn finish(self) -> block::Name {
        let name = self.kind.name();
        let expected = name.wire();
        if !expected.is_empty() && self.matched as usize == expected.len() {
            let name = block::Name::from_head(name);
            return if name.is_forbidden() {
                block::Name::INVALID
            } else {
                name
            };
        }
        match self.kind {
            Kind::Start
            | Kind::Invalid
            | Kind::Pseudo
            | Kind::PseudoP
            | Kind::PseudoS
            | Kind::Method
            | Kind::Scheme
            | Kind::Path
            | Kind::Status
            | Kind::Authority
            | Kind::Protocol => block::Name::INVALID,
            Kind::Regular
            | Kind::C
            | Kind::T
            | Kind::Host
            | Kind::Length
            | Kind::Te
            | Kind::Connection
            | Kind::Keep
            | Kind::Proxy
            | Kind::Transfer
            | Kind::Upgrade => block::Name::REGULAR,
        }
    }
}

impl Kind {
    const fn after_start(byte: u8) -> Self {
        if byte == b':' {
            Self::Pseudo
        } else if byte == b'c' {
            Self::C
        } else if byte == b'h' {
            Self::Host
        } else if byte == b'k' {
            Self::Keep
        } else if byte == b'p' {
            Self::Proxy
        } else if byte == b't' {
            Self::T
        } else if byte == b'u' {
            Self::Upgrade
        } else {
            Self::Regular
        }
    }

    const fn after_pseudo(byte: u8) -> Self {
        if byte == b'a' {
            Self::Authority
        } else if byte == b'm' {
            Self::Method
        } else if byte == b'p' {
            Self::PseudoP
        } else if byte == b's' {
            Self::PseudoS
        } else {
            Self::Invalid
        }
    }

    const fn after_p(byte: u8) -> Self {
        if byte == b'a' {
            Self::Path
        } else if byte == b'r' {
            Self::Protocol
        } else {
            Self::Invalid
        }
    }

    const fn after_s(byte: u8) -> Self {
        if byte == b'c' {
            Self::Scheme
        } else if byte == b't' {
            Self::Status
        } else {
            Self::Invalid
        }
    }

    const fn after_t(byte: u8) -> Self {
        if byte == b'e' {
            Self::Te
        } else if byte == b'r' {
            Self::Transfer
        } else {
            Self::Regular
        }
    }

    const fn name(self) -> head::Name {
        match self {
            Self::Method => head::Name::Method,
            Self::Scheme => head::Name::Scheme,
            Self::Path => head::Name::Path,
            Self::Status => head::Name::Status,
            Self::Authority => head::Name::Authority,
            Self::Protocol => head::Name::Protocol,
            Self::Host => head::Name::Host,
            Self::Length => head::Name::ContentLength,
            Self::Te => head::Name::Te,
            Self::Connection => head::Name::Connection,
            Self::Keep => head::Name::KeepAlive,
            Self::Proxy => head::Name::ProxyConnection,
            Self::Transfer => head::Name::TransferEncoding,
            Self::Upgrade => head::Name::Upgrade,
            Self::Invalid | Self::Pseudo | Self::PseudoP | Self::PseudoS => head::Name::Invalid,
            Self::Start | Self::Regular | Self::C | Self::T => head::Name::Regular,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Scan>() == 2);
