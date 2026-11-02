use sark_http::payload::crc32::Hasher;

// Independent gzip members for "decoded", "x", empty input, and 4096 'x' bytes.
pub const DECODED: &[u8] = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x4b\x49\x4d\xce\x4f\x49\x4d\x01\x00\xf6\x9a\xf0\x1a\x07\x00\x00\x00";
pub const X: &[u8] =
    b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xab\x00\x00\x83\x16\xdc\x8c\x01\x00\x00\x00";
pub const EMPTY: &[u8] =
    b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00";
pub const REPEATED: &[u8] = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xed\xc1\x01\x0d\x00\x00\x00\xc2\xa0\xda\x8f\x6f\x0f\x07\x14\x00\x00\x00\xf0\x6e\xc1\x77\x10\x3e\x00\x10\x00\x00";

/// A fixed gzip member expanding to one MiB of 'x' bytes.
pub fn bomb() -> Vec<u8> {
    let mut bytes = vec![0; 1051];
    bytes[..25].copy_from_slice(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xed\xc1\x31\x01\x00\x00\x00\xc2\xa0\xda\x8b\x6f\x08\x5f\xa0");
    bytes[1041..].copy_from_slice(b"\x3e\x03\x32\x9c\x3b\x15\x00\x00\x10\x00");
    bytes
}

/// Stored DEFLATE blocks keep the wire large enough to cross receive segments.
pub fn fragmented() -> (Vec<u8>, Vec<u8>) {
    let payload: Vec<_> = (0..512 * 1024).map(|index| index as u8).collect();
    let mut bytes = DECODED[..10].to_vec();
    let mut blocks = payload.chunks(u16::MAX as usize).peekable();
    while let Some(block) = blocks.next() {
        bytes.push(u8::from(blocks.peek().is_none()));
        let length = block.len() as u16;
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&(!length).to_le_bytes());
        bytes.extend_from_slice(block);
    }
    bytes.extend_from_slice(&Hasher::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    (payload, bytes)
}
