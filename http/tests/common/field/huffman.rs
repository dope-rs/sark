use sark_http::field::metadata::huffman::{CODES, decoder::Error};

pub const AUTHORITY: &[u8] = b"www.example.com";
pub const AUTHORITY_ENCODED: &[u8] = &[
    0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
];
pub const PREFIX: &[u8] = b"prefix";
pub const SHORT: &[u8] = b"abc";
pub const RETAINED: &[u8] = b"a retained huffman value with a stable final owner";
pub const LONG: &[u8] = b"output longer than the reservation";
pub const INVALID: &[u8] = &[0x1c, 0x64, 0xff, 0xff, 0xff, 0xfc];
pub const BAD_TAILS: &[&[u8]] = &[
    &[0xff],
    &[0x00],
    &[0xff, 0xff, 0xff, 0xfc],
    &[0xff, 0xff, 0xff, 0xff],
];

pub trait Reference {
    fn try_decode(&self) -> Result<Vec<u8>, Error>;
}

impl Reference for [u8] {
    fn try_decode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        let mut prefix = 0u32;
        let mut width = 0u8;
        for &byte in self {
            let mut left = 8u8;
            while left != 0 {
                left -= 1;
                prefix = (prefix << 1) | u32::from((byte >> left) & 1);
                width += 1;

                let mut viable = false;
                let mut decoded = None;
                for (symbol, &(code, code_len)) in CODES.iter().enumerate() {
                    if code_len == width && code == prefix {
                        decoded = Some(symbol);
                        break;
                    }
                    if code_len > width && code >> (code_len - width) == prefix {
                        viable = true;
                    }
                }
                if let Some(symbol) = decoded {
                    if symbol == 256 {
                        return Err(Error);
                    }
                    out.push(symbol as u8);
                    prefix = 0;
                    width = 0;
                } else if !viable {
                    return Err(Error);
                }
            }
        }
        if width == 0 || (width <= 7 && prefix == (1u32 << width) - 1) {
            Ok(out)
        } else {
            Err(Error)
        }
    }
}
