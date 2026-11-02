#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Known {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Connect,
}

impl Known {
    pub const ALL: u8 = u8::MAX;

    #[doc(hidden)]
    pub const fn bit(self) -> u8 {
        1 << self as u8
    }

    pub const fn as_str(&self) -> &str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Connect => "CONNECT",
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"GET" => Some(Self::Get),
            b"POST" => Some(Self::Post),
            b"PUT" => Some(Self::Put),
            b"PATCH" => Some(Self::Patch),
            b"DELETE" => Some(Self::Delete),
            b"HEAD" => Some(Self::Head),
            b"OPTIONS" => Some(Self::Options),
            b"CONNECT" => Some(Self::Connect),
            _ => None,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Known>() == 1);
const _: () = assert!(core::mem::size_of::<Option<Known>>() == 1);
