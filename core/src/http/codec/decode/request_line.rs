use std::ops::Range;

const ALL_METHODS: u8 = (1 << 7) - 1;
const SPACE_WORD: u64 = u64::from_le_bytes(*b"        ");
const CR_WORD: u64 = u64::from_le_bytes(*b"\r\r\r\r\r\r\r\r");
const BYTE_LO: u64 = 0x0101_0101_0101_0101;
const BYTE_HI: u64 = 0x8080_8080_8080_8080;
const VERSION_BASE: u64 = u64::from_ne_bytes(*b"HTTP/1.0");
const VERSION_MASK: u64 = u64::from_ne_bytes(*b"\xff\xff\xff\xff\xff\xff\xff\xfe");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MethodKey {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
}

impl MethodKey {
    #[doc(hidden)]
    pub const fn bit(self) -> u8 {
        1 << self as u8
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RequestLine<'a> {
    pub method: &'a [u8],
    pub target: &'a [u8],
    pub version: &'a [u8],
    pub headers_start: usize,
}

impl RequestLine<'_> {
    pub fn parse(buf: &[u8]) -> Result<Option<RequestLine<'_>>, ()> {
        let mut method = None;
        Self::parse_for::<ALL_METHODS>(buf, &mut method)
    }

    #[doc(hidden)]
    pub fn parse_for<'a, const METHODS: u8>(
        buf: &'a [u8],
        method_slot: &mut Option<MethodKey>,
    ) -> Result<Option<RequestLine<'a>>, ()> {
        *method_slot = None;
        let parsed = 'parse: {
            let (method_end, method_key) = match known_method::<METHODS>(buf) {
                Some(known) => (known.0, Some(known.1)),
                None => {
                    let Some(end) = buf.iter().position(|&byte| byte == b' ' || byte == b'\r')
                    else {
                        break 'parse None;
                    };
                    if buf[end] == b'\r' {
                        break 'parse None;
                    }
                    (end, None)
                }
            };
            if method_end == 0 {
                break 'parse None;
            }

            let target_start = method_end + 1;
            let Some(target_bytes) = buf.get(target_start..) else {
                break 'parse None;
            };
            let target_len = if target_bytes.first() == Some(&b'\r') {
                break 'parse None;
            } else if target_bytes.get(1) == Some(&b' ') {
                1
            } else if let Some(word) = target_bytes.get(..8) {
                let word = u64::from_le_bytes(word.try_into().expect("eight-byte slice"));
                let space_x = word ^ SPACE_WORD;
                let spaces = space_x.wrapping_sub(BYTE_LO) & !space_x & BYTE_HI;
                let cr_x = word ^ CR_WORD;
                let crs = cr_x.wrapping_sub(BYTE_LO) & !cr_x & BYTE_HI;
                let separators = spaces | crs;
                if separators != 0 {
                    let end = separators.trailing_zeros() as usize / 8;
                    if target_bytes[end] == b'\r' {
                        break 'parse None;
                    }
                    end
                } else {
                    let Some(rest) = memchr::memchr2(b' ', b'\r', &target_bytes[8..]) else {
                        break 'parse None;
                    };
                    if target_bytes[rest + 8] == b'\r' {
                        break 'parse None;
                    }
                    rest + 8
                }
            } else {
                let Some(end) = target_bytes
                    .iter()
                    .position(|&byte| byte == b' ' || byte == b'\r')
                else {
                    break 'parse None;
                };
                if target_bytes[end] == b'\r' {
                    break 'parse None;
                }
                end
            };
            if target_len == 0 {
                break 'parse None;
            }

            let version_start = target_start + target_len + 1;
            let Some(suffix) = buf.get(version_start..version_start + 10) else {
                break 'parse None;
            };
            let version: &[u8; 8] = suffix[..8].try_into().expect("eight-byte version");
            let version_word = u64::from_ne_bytes(*version);
            if ((version_word ^ VERSION_BASE) & VERSION_MASK) != 0
                || suffix[8] != b'\r'
                || suffix[9] != b'\n'
            {
                break 'parse None;
            }

            *method_slot = method_key;
            Some(RequestLine {
                method: &buf[..method_end],
                target: &buf[target_start..target_start + target_len],
                version,
                headers_start: version_start + 10,
            })
        };

        match parsed {
            Some(request) => Ok(Some(request)),
            None => reject(buf),
        }
    }
}

fn known_method<const METHODS: u8>(buf: &[u8]) -> Option<(usize, MethodKey)> {
    if METHODS & MethodKey::Get.bit() != 0 && buf.starts_with(b"GET ") {
        Some((3, MethodKey::Get))
    } else if METHODS & MethodKey::Post.bit() != 0 && buf.starts_with(b"POST ") {
        Some((4, MethodKey::Post))
    } else if METHODS & MethodKey::Put.bit() != 0 && buf.starts_with(b"PUT ") {
        Some((3, MethodKey::Put))
    } else if METHODS & MethodKey::Patch.bit() != 0 && buf.starts_with(b"PATCH ") {
        Some((5, MethodKey::Patch))
    } else if METHODS & MethodKey::Delete.bit() != 0 && buf.starts_with(b"DELETE ") {
        Some((6, MethodKey::Delete))
    } else if METHODS & MethodKey::Head.bit() != 0 && buf.starts_with(b"HEAD ") {
        Some((4, MethodKey::Head))
    } else if METHODS & MethodKey::Options.bit() != 0 && buf.starts_with(b"OPTIONS ") {
        Some((7, MethodKey::Options))
    } else {
        None
    }
}

fn reject<T>(buf: &[u8]) -> Result<Option<T>, ()> {
    match memchr::memchr(b'\r', buf) {
        None => Ok(None),
        Some(end) if end + 1 == buf.len() => Ok(None),
        Some(_) => Err(()),
    }
}

pub fn request_head_end(bytes: &[u8]) -> Option<Range<usize>> {
    memchr::memmem::find(bytes, b"\r\n\r\n").map(|s| s..s + 4)
}
