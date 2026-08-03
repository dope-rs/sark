#![no_std]

/// Protocol-neutral identity for names with request-head semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum KnownRequestHeadName {
    Invalid,
    Method,
    Scheme,
    Path,
    Status,
    Authority,
    Protocol,
    Host,
    Connection,
    ContentLength,
    TransferEncoding,
    Expect,
    AcceptEncoding,
    Te,
    KeepAlive,
    ProxyConnection,
    Upgrade,
    Regular,
}

/// A compile-time request-head name and its protocol metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct RequestHeadSemantic {
    known: KnownRequestHeadName,
    wire_name: &'static [u8],
    http1_mandatory: bool,
}

impl RequestHeadSemantic {
    const fn new(
        known: KnownRequestHeadName,
        wire_name: &'static [u8],
        http1_mandatory: bool,
    ) -> Self {
        Self {
            known,
            wire_name,
            http1_mandatory,
        }
    }

    pub const METHOD: Self = Self::new(KnownRequestHeadName::Method, b":method", false);
    pub const SCHEME: Self = Self::new(KnownRequestHeadName::Scheme, b":scheme", false);
    pub const PATH: Self = Self::new(KnownRequestHeadName::Path, b":path", false);
    pub const STATUS: Self = Self::new(KnownRequestHeadName::Status, b":status", false);
    pub const AUTHORITY: Self = Self::new(KnownRequestHeadName::Authority, b":authority", false);
    pub const PROTOCOL: Self = Self::new(KnownRequestHeadName::Protocol, b":protocol", false);
    pub const HOST: Self = Self::new(KnownRequestHeadName::Host, b"host", true);
    pub const CONNECTION: Self = Self::new(KnownRequestHeadName::Connection, b"connection", true);
    pub const CONTENT_LENGTH: Self =
        Self::new(KnownRequestHeadName::ContentLength, b"content-length", true);
    pub const TRANSFER_ENCODING: Self = Self::new(
        KnownRequestHeadName::TransferEncoding,
        b"transfer-encoding",
        true,
    );
    pub const EXPECT: Self = Self::new(KnownRequestHeadName::Expect, b"expect", false);
    pub const ACCEPT_ENCODING: Self = Self::new(
        KnownRequestHeadName::AcceptEncoding,
        b"accept-encoding",
        false,
    );
    pub const TE: Self = Self::new(KnownRequestHeadName::Te, b"te", false);
    pub const KEEP_ALIVE: Self = Self::new(KnownRequestHeadName::KeepAlive, b"keep-alive", false);
    pub const PROXY_CONNECTION: Self = Self::new(
        KnownRequestHeadName::ProxyConnection,
        b"proxy-connection",
        false,
    );
    pub const UPGRADE: Self = Self::new(KnownRequestHeadName::Upgrade, b"upgrade", false);

    /// Names for which HTTP/1 generates specialized value semantics.
    pub const HTTP1: [Self; 6] = [
        Self::HOST,
        Self::CONNECTION,
        Self::CONTENT_LENGTH,
        Self::TRANSFER_ENCODING,
        Self::EXPECT,
        Self::ACCEPT_ENCODING,
    ];

    pub const fn known(self) -> KnownRequestHeadName {
        self.known
    }

    pub const fn wire_name(self) -> &'static [u8] {
        self.wire_name
    }

    pub const fn http1_mandatory(self) -> bool {
        self.http1_mandatory
    }
}

impl KnownRequestHeadName {
    pub fn classify_http1(name: &[u8]) -> Option<Self> {
        match name.len() {
            4 if ascii_eq_ignore_case(name, RequestHeadSemantic::HOST.wire_name()) => {
                Some(Self::Host)
            }
            6 if ascii_eq_ignore_case(name, RequestHeadSemantic::EXPECT.wire_name()) => {
                Some(Self::Expect)
            }
            10 if ascii_eq_ignore_case(name, RequestHeadSemantic::CONNECTION.wire_name()) => {
                Some(Self::Connection)
            }
            14 if ascii_eq_ignore_case(name, RequestHeadSemantic::CONTENT_LENGTH.wire_name()) => {
                Some(Self::ContentLength)
            }
            15 if ascii_eq_ignore_case(name, RequestHeadSemantic::ACCEPT_ENCODING.wire_name()) => {
                Some(Self::AcceptEncoding)
            }
            17 if ascii_eq_ignore_case(
                name,
                RequestHeadSemantic::TRANSFER_ENCODING.wire_name(),
            ) =>
            {
                Some(Self::TransferEncoding)
            }
            _ => None,
        }
    }

    /// Classifies only names that alter HTTP/2 and HTTP/3 handling. Ordinary
    /// names deliberately collapse to `Regular`, preserving the minimal hot
    /// path of the protocol-specific parser.
    pub fn classify_compressed(name: &[u8]) -> Self {
        match name {
            b":method" => Self::Method,
            b":scheme" => Self::Scheme,
            b":path" => Self::Path,
            b":status" => Self::Status,
            b":authority" => Self::Authority,
            b":protocol" => Self::Protocol,
            b"content-length" => Self::ContentLength,
            b"te" => Self::Te,
            b"" | b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" => Self::Invalid,
            _ if name[0] == b':' || name.iter().any(u8::is_ascii_uppercase) => Self::Invalid,
            _ => Self::Regular,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseHeaderNameError {
    Empty,
    InvalidByte { index: usize, byte: u8 },
    Managed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderValueError {
    pub index: usize,
}

pub const fn validate_response_header_name(name: &str) -> Result<(), ResponseHeaderNameError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(ResponseHeaderNameError::Empty);
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !is_header_name_byte(byte) {
            return Err(ResponseHeaderNameError::InvalidByte { index, byte });
        }
        index += 1;
    }

    if ascii_eq_ignore_case(bytes, b"date")
        || ascii_eq_ignore_case(bytes, b"server")
        || ascii_eq_ignore_case(bytes, RequestHeadSemantic::CONTENT_LENGTH.wire_name())
        || ascii_eq_ignore_case(bytes, RequestHeadSemantic::CONNECTION.wire_name())
        || ascii_eq_ignore_case(bytes, RequestHeadSemantic::TRANSFER_ENCODING.wire_name())
    {
        return Err(ResponseHeaderNameError::Managed);
    }

    Ok(())
}

pub const fn validate_header_value(value: &[u8]) -> Result<(), HeaderValueError> {
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\r' || value[index] == b'\n' {
            return Err(HeaderValueError { index });
        }
        index += 1;
    }
    Ok(())
}

pub const fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

const fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index].to_ascii_lowercase() != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_rfc_token_names() {
        for name in ["content-type", "x-request_id", "x!#$%&'*+-.^_`|~"] {
            assert_eq!(validate_response_header_name(name), Ok(()));
        }
    }

    #[test]
    fn rejects_empty_and_non_token_names() {
        assert_eq!(
            validate_response_header_name(""),
            Err(ResponseHeaderNameError::Empty)
        );
        assert_eq!(
            validate_response_header_name("x bad"),
            Err(ResponseHeaderNameError::InvalidByte {
                index: 1,
                byte: b' '
            })
        );
        assert_eq!(
            validate_response_header_name("x:bad"),
            Err(ResponseHeaderNameError::InvalidByte {
                index: 1,
                byte: b':'
            })
        );
    }

    #[test]
    fn rejects_managed_names_case_insensitively() {
        for name in [
            "date",
            "Server",
            "CONTENT-LENGTH",
            "Connection",
            "Transfer-Encoding",
        ] {
            assert_eq!(
                validate_response_header_name(name),
                Err(ResponseHeaderNameError::Managed)
            );
        }
    }

    #[test]
    fn rejects_only_line_breaks_in_values() {
        assert_eq!(validate_header_value(b"a\tb"), Ok(()));
        assert_eq!(
            validate_header_value(b"a\nb"),
            Err(HeaderValueError { index: 1 })
        );
        assert_eq!(
            validate_header_value(b"a\rb"),
            Err(HeaderValueError { index: 1 })
        );
    }

    #[test]
    fn request_head_vocabulary_drives_both_protocol_families() {
        for semantic in RequestHeadSemantic::HTTP1 {
            let known = semantic.known();
            let wire = semantic.wire_name();
            assert_eq!(KnownRequestHeadName::classify_http1(wire), Some(known));

            let mut upper = wire.to_vec();
            upper.make_ascii_uppercase();
            assert_eq!(KnownRequestHeadName::classify_http1(&upper), Some(known));
        }

        for (wire, expected) in [
            (b":method".as_slice(), KnownRequestHeadName::Method),
            (b":path".as_slice(), KnownRequestHeadName::Path),
            (
                b"content-length".as_slice(),
                KnownRequestHeadName::ContentLength,
            ),
            (b"te".as_slice(), KnownRequestHeadName::Te),
            (b"x-route".as_slice(), KnownRequestHeadName::Regular),
        ] {
            assert_eq!(KnownRequestHeadName::classify_compressed(wire), expected);
        }

        for forbidden in [
            b"connection".as_slice(),
            b"keep-alive".as_slice(),
            b"proxy-connection".as_slice(),
            b"transfer-encoding".as_slice(),
            b"upgrade".as_slice(),
            b"X-UPPER".as_slice(),
        ] {
            assert_eq!(
                KnownRequestHeadName::classify_compressed(forbidden),
                KnownRequestHeadName::Invalid,
            );
        }
    }
}
