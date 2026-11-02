use sark_http::field::{
    Encode as _,
    base64::{Decoder, Encoder, Error},
};

pub const VECTORS: &[(&[u8], &[u8], &[u8])] = &[
    (b"", b"", b""),
    (b"f", b"Zg", b"Zg=="),
    (b"fo", b"Zm8", b"Zm8="),
    (b"foo", b"Zm9v", b"Zm9v"),
    (b"foobar", b"Zm9vYmFy", b"Zm9vYmFy"),
];

pub fn encode(bytes: &[u8], padded: bool) -> Vec<u8> {
    let encoder = if padded {
        Encoder::padded(bytes)
    } else {
        Encoder::new(bytes)
    };
    let mut out = Vec::with_capacity(encoder.encoded_len());
    encoder.write(|block| out.extend_from_slice(block));
    out
}

pub fn try_decode(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = bytes.to_vec();
    let end = out.len();
    let len = Decoder::new(&mut out).decode(0..end, 0)?;
    out.truncate(len);
    Ok(out)
}
