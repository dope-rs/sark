use miniz_oxide::deflate;
use sark_http::payload::crc32::Hasher;

pub const HEADER_LEN: usize = 10 + 5 + b"fixture.bin\0segmented\0".len();
pub const BACKREF: &[u8] = &[0x03, 0x02, 0x00];
pub const EMPTY_LARGE_HINT: (&[u8], usize) = (
    &[
        0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 3, 0, 0, 0, 0, 0, 0, 0, 0x10, 0,
    ],
    1024 * 1024,
);

pub trait Encode {
    fn gzip(&self) -> Vec<u8>;
    fn wrap(&self, checksum: bool) -> Vec<u8>;
}

impl Encode for [u8] {
    fn gzip(&self) -> Vec<u8> {
        let mut bytes = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
        bytes.extend_from_slice(&deflate::compress_to_vec(self, 3));
        bytes.extend_from_slice(&Hasher::hash(self).to_le_bytes());
        bytes.extend_from_slice(&(self.len() as u32).to_le_bytes());
        bytes
    }

    fn wrap(&self, checksum: bool) -> Vec<u8> {
        let mut wrapped = Vec::with_capacity(self.len() + HEADER_LEN);
        wrapped.extend_from_slice(&self[..3]);
        wrapped.push(0x04 | 0x08 | 0x10 | if checksum { 0x02 } else { 0 });
        wrapped.extend_from_slice(&self[4..10]);
        wrapped.extend_from_slice(&[3, 0, b'x', b't', b'r']);
        wrapped.extend_from_slice(b"fixture.bin\0segmented\0");
        if checksum {
            let check = Hasher::hash(&wrapped) as u16;
            wrapped.extend_from_slice(&check.to_le_bytes());
        }
        wrapped.extend_from_slice(&self[10..]);
        wrapped
    }
}
