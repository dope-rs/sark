//! Fused request-line parsing and method classification.

use crate::service::Key;
use sark_core::http::codec::ParsedRequestHead;

pub struct FusedHead<'buf> {
    pub head: ParsedRequestHead<'buf>,
    pub method_key: Key,
}

impl FusedHead<'_> {
    pub fn parse(buf: &[u8]) -> Option<FusedHead<'_>> {
        let cr = memchr::memchr(b'\r', buf)?;
        if cr + 1 >= buf.len() || buf[cr + 1] != b'\n' {
            return None;
        }
        let line = &buf[..cr];
        if line.len() < 9 {
            return None;
        }
        let sp2 = line.len() - 9;
        if line[sp2] != b' ' {
            return None;
        }
        let (method_key, sp1) = method_word(line);
        if sp1 >= sp2 {
            return None;
        }
        let method = &line[..sp1];
        if method.is_empty() {
            return None;
        }
        let target = &line[sp1 + 1..sp2];
        if target.is_empty() {
            return None;
        }
        if !sark_core::simd::request_target_is_valid(target) {
            return None;
        }
        let version = &line[sp2 + 1..];
        if !version_ok(version) {
            return None;
        }
        Some(FusedHead {
            head: ParsedRequestHead {
                method,
                target,
                version,
                headers_start: cr + 2,
            },
            method_key,
        })
    }
}

fn method_word(line: &[u8]) -> (Key, usize) {
    let w4 = u32::from_le_bytes([line[0], line[1], line[2], line[3]]);
    const GET_SP: u32 = u32::from_le_bytes(*b"GET ");
    const PUT_SP: u32 = u32::from_le_bytes(*b"PUT ");
    const POST: u32 = u32::from_le_bytes(*b"POST");
    const HEAD: u32 = u32::from_le_bytes(*b"HEAD");
    const PATC: u32 = u32::from_le_bytes(*b"PATC");
    const DELE: u32 = u32::from_le_bytes(*b"DELE");
    const OPTI: u32 = u32::from_le_bytes(*b"OPTI");
    match w4 {
        GET_SP => return (Key::Get, 3),
        PUT_SP => return (Key::Put, 3),
        POST if line[4] == b' ' => return (Key::Post, 4),
        HEAD if line[4] == b' ' => return (Key::Head, 4),
        PATC if line[5] == b' ' && line[4] == b'H' => return (Key::Patch, 5),
        DELE if line[6] == b' '
            && u16::from_le_bytes([line[4], line[5]]) == u16::from_le_bytes(*b"TE") =>
        {
            return (Key::Delete, 6);
        }
        OPTI if line[7] == b' '
            && u32::from_le_bytes([line[3], line[4], line[5], line[6]])
                == u32::from_le_bytes(*b"IONS") =>
        {
            return (Key::Options, 7);
        }
        _ => {}
    }
    match memchr::memchr(b' ', line) {
        Some(sp1) => (Key::from_bytes(&line[..sp1]), sp1),
        None => (Key::Other, line.len()),
    }
}

fn version_ok(version: &[u8]) -> bool {
    if version.len() != 8 {
        return false;
    }
    let w = u64::from_le_bytes([
        version[0], version[1], version[2], version[3], version[4], version[5], version[6],
        version[7],
    ]);
    const PREFIX: u64 = u64::from_le_bytes(*b"HTTP/1.0");
    const MASK: u64 = 0x00ff_ffff_ffff_ffff;
    if w & MASK != PREFIX & MASK {
        return false;
    }
    let last = version[7];
    last == b'0' || last == b'1'
}
