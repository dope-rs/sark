const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub struct Crypto;

impl Crypto {
    pub fn sha1_digest(data: &[u8]) -> [u8; 20] {
        let mut h0: u32 = 0x6745_2301;
        let mut h1: u32 = 0xEFCD_AB89;
        let mut h2: u32 = 0x98BA_DCFE;
        let mut h3: u32 = 0x1032_5476;
        let mut h4: u32 = 0xC3D2_E1F0;

        let bit_len = (data.len() as u64) * 8;
        let mut msg = Vec::with_capacity(((data.len() + 9).div_ceil(64)) * 64);
        msg.extend_from_slice(data);
        msg.push(0x80);
        while (msg.len() % 64) != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bit_len.to_be_bytes());

        for chunk in msg.chunks_exact(64) {
            let mut w = [0u32; 80];
            for (wi, c) in w[..16].iter_mut().zip(chunk.chunks_exact(4)) {
                *wi = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
            }
            for i in 16..80 {
                w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
            }

            let mut a = h0;
            let mut b = h1;
            let mut c = h2;
            let mut d = h3;
            let mut e = h4;

            for (i, wi) in w.iter().enumerate() {
                let (f, k) = match i {
                    0..=19 => (((b & c) | ((!b) & d)), 0x5A82_7999),
                    20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                    40..=59 => (((b & c) | (b & d) | (c & d)), 0x8F1B_BCDC),
                    _ => (b ^ c ^ d, 0xCA62_C1D6),
                };
                let temp = a
                    .rotate_left(5)
                    .wrapping_add(f)
                    .wrapping_add(e)
                    .wrapping_add(k)
                    .wrapping_add(*wi);
                e = d;
                d = c;
                c = b.rotate_left(30);
                b = a;
                a = temp;
            }

            h0 = h0.wrapping_add(a);
            h1 = h1.wrapping_add(b);
            h2 = h2.wrapping_add(c);
            h3 = h3.wrapping_add(d);
            h4 = h4.wrapping_add(e);
        }

        let mut out = [0u8; 20];
        out[0..4].copy_from_slice(&h0.to_be_bytes());
        out[4..8].copy_from_slice(&h1.to_be_bytes());
        out[8..12].copy_from_slice(&h2.to_be_bytes());
        out[12..16].copy_from_slice(&h3.to_be_bytes());
        out[16..20].copy_from_slice(&h4.to_be_bytes());
        out
    }

    pub fn base64_encode(data: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        let mut i = 0;
        while i + 3 <= data.len() {
            let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
            out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
            out.push(TABLE[(n & 0x3f) as usize] as char);
            i += 3;
        }
        let rem = data.len() - i;
        if rem == 1 {
            let n = (data[i] as u32) << 16;
            out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
            out.push('=');
            out.push('=');
        } else if rem == 2 {
            let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
            out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
            out.push('=');
        }
        out
    }

    pub fn expected_accept(key: &str) -> String {
        let mut input = String::with_capacity(key.len() + WS_GUID.len());
        input.push_str(key);
        input.push_str(WS_GUID);
        let digest = Self::sha1_digest(input.as_bytes());
        Self::base64_encode(&digest)
    }

    pub fn ws_accept(head: &str) -> Option<[u8; 28]> {
        let needle = "sec-websocket-accept:";
        for line in head.lines() {
            let t = line.trim_end();
            if t.len() <= needle.len() {
                continue;
            }
            let matches = t
                .get(..needle.len())
                .map(|p| p.eq_ignore_ascii_case(needle))
                .unwrap_or(false);
            if !matches {
                continue;
            }
            let value = t[needle.len()..].trim().as_bytes();
            if value.len() == 28 {
                let mut accept = [0u8; 28];
                accept.copy_from_slice(value);
                return Some(accept);
            }
        }
        None
    }
}
