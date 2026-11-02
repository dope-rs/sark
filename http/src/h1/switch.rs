//! HTTP/1 protocol-transition admission.

use core::mem;

use crate::{
    field::{self, scan::Match},
    h1::{
        field::Connection,
        head::{line::Version, request::offers},
    },
    payload::response::Method,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    IncompleteUpgradeRequest,
    UnexpectedSwitch,
    MissingConnectionUpgrade,
    MissingSelection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradeError {
    Empty,
    InvalidProtocol,
    MultipleSelections,
    NotOffered,
    Overflow { value: usize, added: usize },
    TooLarge { actual: usize, maximum: usize },
}

/// Borrow-free transition requested by an admitted HTTP/1 exchange.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Intent {
    #[default]
    None,
    Connect,
    Upgrade,
}

impl Intent {
    /// Derives the only transition intent which may cross the request-head
    /// boundary. HTTP/1.0 ignores Upgrade; HTTP/1.1 rejects a partial handshake.
    pub(in crate::h1) const fn from_request(
        request: Method,
        conn: Connection,
        offers: Offers,
        version: Version,
    ) -> Result<Self, Error> {
        if matches!(request, Method::Connect) {
            return Ok(Self::Connect);
        }
        match (conn.is_upgrade(), offers.is_present()) {
            (false, false) => Ok(Self::None),
            _ if matches!(version, Version::Ten) => Ok(Self::None),
            (true, true) => Ok(Self::Upgrade),
            (true, false) | (false, true) => Err(Error::IncompleteUpgradeRequest),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Connect,
    Upgrade,
    ExtendedConnect,
}

/// Proof that one response Upgrade selection is syntactically valid and was
/// present in the request's offer set. Construction performs exact
/// case-insensitive byte comparison; no collision-prone digest is retained.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Selection {
    private: (),
}

impl Selection {
    pub(in crate::h1) fn from_offers<B>(
        offered: offers::View<'_, B>,
        selected: &[u8],
    ) -> Result<Self, UpgradeError> {
        let selected = single_upgrade_protocol(selected)?;
        for value in offered.values() {
            for protocol in value.split(|byte| *byte == b',') {
                if field::Error::trim_ows(protocol).eq_ignore_ascii_case(selected) {
                    return Ok(Self { private: () });
                }
            }
        }
        Err(UpgradeError::NotOffered)
    }

    pub fn select<'a>(
        offered: impl IntoIterator<Item = &'a [u8]>,
        selected: &[u8],
    ) -> Result<Self, UpgradeError> {
        let selected = single_upgrade_protocol(selected)?;
        let mut matched = false;
        for value in offered {
            for protocol in protocols(value) {
                let protocol = protocol?;
                if protocol.eq_ignore_ascii_case(selected) {
                    matched = true;
                }
            }
            if matched {
                return Ok(Self { private: () });
            }
        }
        Err(UpgradeError::NotOffered)
    }
}

/// Exact, borrow-free retention plan accumulated during the head scan.
#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Offers(u32);

impl Offers {
    /// Admits one `Upgrade` field and accounts for the NUL separator used by a
    /// retained offer set. Validation and exact-length planning therefore
    /// happen in the canonical request-head walk.
    pub(in crate::h1) fn try_push(self, value: &[u8]) -> Result<Self, UpgradeError> {
        Self::validate(value)?;
        let sep = usize::from(self.is_present());
        let current = self.compact_len();
        let length = current.checked_add(sep).ok_or(UpgradeError::Overflow {
            value: current,
            added: sep,
        })?;
        let actual = length
            .checked_add(value.len())
            .ok_or(UpgradeError::Overflow {
                value: length,
                added: value.len(),
            })?;
        let maximum = u32::MAX as usize;
        if actual > maximum {
            return Err(UpgradeError::TooLarge { actual, maximum });
        }
        Ok(Self(actual as u32))
    }

    pub const fn is_present(self) -> bool {
        self.0 != 0
    }

    pub const fn compact_len(self) -> usize {
        self.0 as usize
    }

    pub fn validate(value: &[u8]) -> Result<(), UpgradeError> {
        for raw in value.split(|byte| *byte == b',') {
            check(field::Error::trim_ows(raw))?;
        }
        Ok(())
    }
}

fn protocols(value: &[u8]) -> impl Iterator<Item = Result<&[u8], UpgradeError>> {
    value.split(|byte| *byte == b',').map(|raw| {
        let protocol = field::Error::trim_ows(raw);
        check(protocol)?;
        Ok(protocol)
    })
}

fn check(protocol: &[u8]) -> Result<(), UpgradeError> {
    if protocol.is_empty() {
        Err(UpgradeError::Empty)
    } else if is_protocol(protocol) {
        Ok(())
    } else {
        Err(UpgradeError::InvalidProtocol)
    }
}

fn single_upgrade_protocol(value: &[u8]) -> Result<&[u8], UpgradeError> {
    let value = field::Error::trim_ows(value);
    if value.is_empty() {
        return Err(UpgradeError::Empty);
    }
    if value.contains(&b',') {
        return Err(UpgradeError::MultipleSelections);
    }
    if !is_protocol(value) {
        return Err(UpgradeError::InvalidProtocol);
    }
    Ok(value)
}

fn is_protocol(value: &[u8]) -> bool {
    let Some(slash) = Match::find(b'/', value) else {
        return is_token(value);
    };
    is_token(slash.prefix()) && is_token(slash.suffix())
}

fn is_token(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().copied().all(field::Error::is_name_byte)
}

const _: () = assert!(mem::size_of::<UpgradeError>() <= 3 * mem::size_of::<usize>());
const _: () = assert!(!mem::needs_drop::<UpgradeError>());
const _: () = assert!(core::mem::size_of::<Intent>() == 1);
const _: () = assert!(core::mem::size_of::<Offers>() == 4);
