use crate::metadata::Metadata;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => return None,
        })
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn parse_ascii(input: &[u8]) -> Option<Self> {
        if input.is_empty() || input.len() > 2 {
            return None;
        }
        let mut value = 0u8;
        for &b in input {
            if !b.is_ascii_digit() {
                return None;
            }
            value = value.checked_mul(10)?.checked_add(b - b'0')?;
        }
        Self::from_u8(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    code: Code,
    message: String,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    pub fn code(&self) -> Code {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn from_trailers(trailers: &Metadata) -> Result<Self, Status> {
        let mut codes = trailers.get_all(b"grpc-status");
        let Some(raw_code) = codes.next() else {
            return Err(Self::new(Code::Internal, "missing grpc-status"));
        };
        let Some(code) = Code::parse_ascii(raw_code) else {
            return Err(Self::new(Code::Internal, "invalid grpc-status"));
        };
        let raw_message = trailers
            .get_all(b"grpc-message")
            .next()
            .map(Self::decode_message)
            .unwrap_or_default();
        Ok(Self::new(code, raw_message))
    }

    pub fn write_grpc_status_value(&self, out: &mut Vec<u8>) {
        let code = self.code.as_u8();
        if code >= 10 {
            out.push(b'1');
            out.push(b'0' + (code - 10));
        } else {
            out.push(b'0' + code);
        }
    }

    pub fn encode_message(message: &str, out: &mut Vec<u8>) {
        for &b in message.as_bytes() {
            match b {
                b' '..=b'~' if b != b'%' => out.push(b),
                _ => {
                    out.push(b'%');
                    out.push(Self::hex(b >> 4));
                    out.push(Self::hex(b & 0x0f));
                }
            }
        }
    }

    pub fn decode_message(input: &[u8]) -> String {
        let mut out = Vec::with_capacity(input.len());
        let mut i = 0;
        while i < input.len() {
            if input[i] == b'%'
                && i + 2 < input.len()
                && let (Some(hi), Some(lo)) = (Self::unhex(input[i + 1]), Self::unhex(input[i + 2]))
            {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
            out.push(input[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn hex(n: u8) -> u8 {
        match n {
            0..=9 => b'0' + n,
            _ => b'A' + (n - 10),
        }
    }

    fn unhex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(10 + b - b'a'),
            b'A'..=b'F' => Some(10 + b - b'A'),
            _ => None,
        }
    }
}
