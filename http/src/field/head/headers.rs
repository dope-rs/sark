use core::mem;

use crate::{
    field::{
        head::{Error, Flags, Name, parse},
        text::Ascii,
    },
    h1::{
        field,
        head::{line::Version, request},
    },
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Known {
    AcceptEncoding,
    ContentLength,
    TransferEncoding,
    Expect,
    Host,
    Connection,
    Upgrade,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    tail: usize,
    start: usize,
    end: usize,
}

impl Bounds {
    pub(super) const fn new(tail: usize, start: usize, end: usize) -> Self {
        Self { tail, start, end }
    }

    pub const fn tail(self) -> usize {
        self.tail
    }

    pub const fn start(self) -> usize {
        self.start
    }

    pub const fn end(self) -> usize {
        self.end
    }

    pub(super) fn value(self, raw: &[u8]) -> &[u8] {
        // SAFETY: private parser continuations use the same immutable raw that
        // just produced these bounds. No caller substitutes another source.
        unsafe { raw.get_unchecked(self.start..self.end) }
    }
}

const _: () = {
    assert!(mem::size_of::<Bounds>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Bounds>>() == 4 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Bounds>());
};

fn parse_connection(raw: &[u8]) -> Result<field::Connection, Error> {
    field::Connection::default()
        .try_push(raw)
        .map_err(Error::Connection)
}

fn apply_connection(scan: &mut request::Fields, flags: &mut Flags, value: field::Connection) {
    scan.routing.connection.append(value);
    if value.is_close() {
        flags.set(Flags::CONNECTION_CLOSE);
    }
    if value.is_keep() {
        flags.set(Flags::CONNECTION_KEEP_ALIVE);
    }
}

impl Known {
    const PREFIX_INITIALS: u32 = {
        let mut initials = 0;
        let mut index = 0;
        while index < Name::H1.len() {
            let first = Name::H1[index].wire()[0];
            assert!(first.is_ascii_lowercase(), "initial");
            initials |= 1 << (first - b'a');
            index += 1;
        }
        initials
    };

    pub fn from_name(name: &[u8]) -> Option<Self> {
        Self::from_kind(Name::from_h1(name)?)
    }

    /// Recognizes a complete canonical field name and its following colon.
    /// The exact literal match also establishes the name's token grammar.
    pub fn from_prefix(raw: &[u8]) -> Option<(Self, usize)> {
        let first = raw.first()?.to_ascii_lowercase();
        let initial = 1u32.checked_shl(u32::from(first.wrapping_sub(b'a')))?;
        if Self::PREFIX_INITIALS & initial == 0 {
            return None;
        }
        for name in Name::H1 {
            let wire = name.wire();
            if wire.first() == Some(&first)
                && raw.get(wire.len()) == Some(&b':')
                && raw[..wire.len()].eq_ignore_ascii_case(wire)
            {
                return Self::from_kind(name).map(|known| (known, wire.len()));
            }
        }
        None
    }

    fn from_kind(name: Name) -> Option<Self> {
        match name {
            Name::AcceptEncoding => Some(Self::AcceptEncoding),
            Name::ContentLength => Some(Self::ContentLength),
            Name::TransferEncoding => Some(Self::TransferEncoding),
            Name::Expect => Some(Self::Expect),
            Name::Host => Some(Self::Host),
            Name::Connection => Some(Self::Connection),
            Name::Upgrade => Some(Self::Upgrade),
            Name::Invalid
            | Name::Method
            | Name::Scheme
            | Name::Path
            | Name::Status
            | Name::Authority
            | Name::Protocol
            | Name::Te
            | Name::KeepAlive
            | Name::ProxyConnection
            | Name::Regular => None,
        }
    }

    pub fn apply(
        self,
        scan: &mut request::Fields,
        flags: &mut Flags,
        value: &[u8],
        version: Version,
    ) -> Result<(), Error> {
        match self {
            Self::AcceptEncoding => parse::ae_value(scan, flags, value),
            Self::ContentLength => {
                let len = Ascii::new(crate::field::Error::trim_ows(value))
                    .parse_usize()
                    .map_err(Error::ContentLength)?;
                scan.message = scan.message.try_length(len).map_err(Error::Message)?;
                Ok(())
            }
            Self::TransferEncoding => {
                let parsed = field::Transfer::default()
                    .parse(value)
                    .map_err(Error::TransferEncoding)?;
                scan.message = scan.message.try_append(parsed).map_err(Error::Message)?;
                Ok(())
            }
            Self::Expect => {
                scan.message = scan.message.with_expect(value);
                Ok(())
            }
            Self::Host => {
                scan.routing.authority = scan
                    .routing
                    .authority
                    .try_host(value)
                    .map_err(Error::Host)?;
                Ok(())
            }
            Self::Connection => {
                let value = parse_connection(value)?;
                apply_connection(scan, flags, value);
                Ok(())
            }
            Self::Upgrade => {
                scan.routing = scan
                    .routing
                    .try_upgrade(value, version)
                    .map_err(Error::Upgrade)?;
                Ok(())
            }
        }
    }

    pub fn scan_line(
        self,
        scan: &mut request::Fields,
        flags: &mut Flags,
        raw: &[u8],
        version: Version,
    ) -> Result<Option<Bounds>, Error> {
        match self {
            Self::AcceptEncoding => parse::ae_line(scan, flags, raw),
            Self::ContentLength => parse::clen_line(scan, raw),
            Self::TransferEncoding => parse::te_line(scan, raw),
            Self::Expect => expect_line(scan, raw),
            Self::Host => host_line(scan, raw),
            Self::Connection => conn_line(scan, flags, raw),
            Self::Upgrade => upgrade_line(scan, raw, version),
        }
    }
}

fn host_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let Some(bounds) = parse::trim_line(raw)? else {
        return Ok(None);
    };
    scan.routing.authority = scan
        .routing
        .authority
        .try_host(bounds.value(raw))
        .map_err(Error::Host)?;
    Ok(Some(bounds))
}

fn upgrade_line(
    scan: &mut request::Fields,
    raw: &[u8],
    version: Version,
) -> Result<Option<Bounds>, Error> {
    let Some(bounds) = parse::trim_line(raw)? else {
        return Ok(None);
    };
    scan.routing = scan
        .routing
        .try_upgrade(bounds.value(raw), version)
        .map_err(Error::Upgrade)?;
    Ok(Some(bounds))
}

fn expect_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let (bounds, value) = match parse::try_token_line_lit(raw, b"100-continue") {
        Some(bounds) => (bounds, b"100-continue".as_slice()),
        None => {
            let Some(bounds) = parse::trim_line(raw)? else {
                return Ok(None);
            };
            (bounds, bounds.value(raw))
        }
    };
    scan.message = scan.message.with_expect(value);
    Ok(Some(bounds))
}

fn conn_line(
    scan: &mut request::Fields,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<Bounds>, Error> {
    let (value, bounds) = if let Some(bounds) = parse::try_token_line_lit(raw, b"keep-alive") {
        (field::Connection::KEEP_ALIVE, bounds)
    } else if let Some(bounds) = parse::try_token_line_lit(raw, b"close") {
        (field::Connection::CLOSE, bounds)
    } else {
        let Some(bounds) = parse::trim_line(raw)? else {
            return Ok(None);
        };
        let value = parse_connection(bounds.value(raw))?;
        (value, bounds)
    };
    apply_connection(scan, flags, value);
    Ok(Some(bounds))
}
