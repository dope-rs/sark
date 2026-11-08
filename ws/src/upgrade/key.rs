use sark_http::field::base64::{Encoded, Encoder};

use crate::upgrade::KeyError;

const GUID: &[u8; 36] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Key([u8; 24]);

impl Key {
    pub fn from_nonce(nonce: [u8; 16]) -> Self {
        let mut bytes = [0; 24];
        let mut offset = 0;
        Encoder::padded(&nonce).write(|block| {
            bytes[offset..offset + block.len()].copy_from_slice(block);
            offset += block.len();
        });
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 24] {
        &self.0
    }

    pub fn parse(value: &[u8]) -> Result<Self, KeyError> {
        let Ok(value) = <&[u8; 24]>::try_from(value) else {
            return Err(KeyError::Length {
                actual: value.len(),
            });
        };
        let len = Encoded::new(value).validate().map_err(KeyError::Base64)?;
        if len != 16 {
            return Err(KeyError::Decoded { actual: len });
        }
        Ok(Self(*value))
    }

    pub fn accept(&self) -> [u8; 28] {
        let digest = sha1_accept(&self.0);
        let mut accept = [0; 28];
        let mut offset = 0;
        Encoder::padded(&digest).write(|block| {
            accept[offset..offset + block.len()].copy_from_slice(block);
            offset += block.len();
        });
        accept
    }
}

fn sha1_accept(key: &[u8; 24]) -> [u8; 20] {
    let mut state = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut first = [0; 64];
    first[..24].copy_from_slice(key);
    first[24..60].copy_from_slice(GUID);
    first[60] = 0x80;
    sha1_compress(&mut state, &first);

    let mut second = [0; 64];
    second[56..].copy_from_slice(&(60_u64 * 8).to_be_bytes());
    sha1_compress(&mut state, &second);

    let mut digest = [0; 20];
    for (chunk, value) in digest.as_chunks_mut::<4>().0.iter_mut().zip(state) {
        chunk.copy_from_slice(&value.to_be_bytes());
    }
    digest
}

fn sha1_compress(state: &mut [u32; 5], block: &[u8; 64]) {
    let mut words = [0; 80];
    for (word, bytes) in words[..16].iter_mut().zip(block.as_chunks::<4>().0.iter()) {
        *word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    }
    for index in 16..80 {
        words[index] =
            (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                .rotate_left(1);
    }

    let mut a = state[0];
    let mut b = state[1];
    let mut c = state[2];
    let mut d = state[3];
    let mut e = state[4];
    for (index, word) in words.iter().enumerate() {
        let (mix, add) = match index {
            0..=19 => (((b & c) | ((!b) & d)), 0x5A82_7999),
            20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
            40..=59 => (((b & c) | (b & d) | (c & d)), 0x8F1B_BCDC),
            _ => (b ^ c ^ d, 0xCA62_C1D6),
        };
        let next = a
            .rotate_left(5)
            .wrapping_add(mix)
            .wrapping_add(e)
            .wrapping_add(add)
            .wrapping_add(*word);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = next;
    }
    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
}
