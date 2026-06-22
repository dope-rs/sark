#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Other,
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
}

impl Key {
    pub fn from_method(method: &http::Method) -> Self {
        Self::from_bytes(method.as_str().as_bytes())
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes.len() {
            3 => {
                const GE: u16 = u16::from_le_bytes(*b"GE");
                const PU: u16 = u16::from_le_bytes(*b"PU");
                if bytes[2] != b'T' {
                    return Self::Other;
                }
                match u16::from_le_bytes(bytes[..2].try_into().unwrap()) {
                    GE => Self::Get,
                    PU => Self::Put,
                    _ => Self::Other,
                }
            }
            4 => {
                const POST: u32 = u32::from_le_bytes(*b"POST");
                const HEAD: u32 = u32::from_le_bytes(*b"HEAD");
                match u32::from_le_bytes(bytes[..4].try_into().unwrap()) {
                    POST => Self::Post,
                    HEAD => Self::Head,
                    _ => Self::Other,
                }
            }
            5 => {
                const PATC: u32 = u32::from_le_bytes(*b"PATC");
                if u32::from_le_bytes(bytes[..4].try_into().unwrap()) == PATC && bytes[4] == b'H' {
                    Self::Patch
                } else {
                    Self::Other
                }
            }
            6 => {
                const DELE: u32 = u32::from_le_bytes(*b"DELE");
                const TE: u16 = u16::from_le_bytes(*b"TE");
                if u32::from_le_bytes(bytes[..4].try_into().unwrap()) == DELE
                    && u16::from_le_bytes(bytes[4..6].try_into().unwrap()) == TE
                {
                    Self::Delete
                } else {
                    Self::Other
                }
            }
            7 => {
                const OPTI: u32 = u32::from_le_bytes(*b"OPTI");
                const IONS: u32 = u32::from_le_bytes(*b"IONS");
                if u32::from_le_bytes(bytes[..4].try_into().unwrap()) == OPTI
                    && u32::from_le_bytes(bytes[3..7].try_into().unwrap()) == IONS
                {
                    Self::Options
                } else {
                    Self::Other
                }
            }
            _ => Self::Other,
        }
    }
}
