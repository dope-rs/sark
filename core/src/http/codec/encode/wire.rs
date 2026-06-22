use o3::buffer::{Owned, Shared};

use crate::http::Request;

pub struct Wire;

impl Wire {
    pub fn dec_len(mut n: usize) -> usize {
        if n == 0 {
            return 1;
        }
        let mut digits = 0;
        while n > 0 {
            digits += 1;
            n /= 10;
        }
        digits
    }

    pub fn write_dec(mut n: usize, buf: &mut [u8; 20]) -> usize {
        if n == 0 {
            buf[0] = b'0';
            return 1;
        }
        let mut i = 20;
        while n > 0 {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        let len = 20 - i;
        buf.copy_within(i..20, 0);
        len
    }

    pub fn write_hex(n: usize, buf: &mut [u8; 16]) -> usize {
        if n == 0 {
            buf[0] = b'0';
            return 1;
        }
        let mut i = 16;
        let mut val = n;
        while val > 0 {
            i -= 1;
            buf[i] = b"0123456789abcdef"[val & 0xf];
            val >>= 4;
        }
        buf.copy_within(i..16, 0);
        16 - i
    }

    pub fn chunk_prefix(size: usize) -> ([u8; 18], usize) {
        let mut hex = [0u8; 16];
        let hex_len = Self::write_hex(size, &mut hex);
        let mut out = [0u8; 18];
        out[..hex_len].copy_from_slice(&hex[..hex_len]);
        out[hex_len] = b'\r';
        out[hex_len + 1] = b'\n';
        (out, hex_len + 2)
    }

    pub fn chunk_frame(body: Shared) -> Shared {
        let (prefix, prefix_len) = Self::chunk_prefix(body.len());
        let mut framed = Owned::with_capacity(prefix_len + body.len() + 2);
        framed.extend_from_slice(&prefix[..prefix_len]);
        framed.extend_from_slice(&body);
        framed.extend_from_slice(b"\r\n");
        framed.freeze()
    }

    pub fn request(req: &Request) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::estimate_request_capacity(req));
        Self::write_request_head(&mut buf, req);

        let body = req.body();
        if !body.is_empty() && !req.headers().contains_key("content-length") {
            let mut dec = [0u8; 20];
            let dec_len = Self::write_dec(body.len(), &mut dec);
            buf.extend_from_slice(b"Content-Length: ");
            buf.extend_from_slice(&dec[..dec_len]);
            buf.extend_from_slice(b"\r\n");
        }

        buf.extend_from_slice(b"\r\n");

        if !body.is_empty() {
            buf.extend_from_slice(body.as_ref());
        }

        buf
    }

    pub fn request_head(req: &Request) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            Self::estimate_request_capacity(req).saturating_sub(req.body().len()),
        );
        Self::write_request_head(&mut buf, req);
        buf.extend_from_slice(b"\r\n");
        buf
    }

    fn header_map_len(headers: &http::HeaderMap) -> usize {
        headers
            .iter()
            .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
            .sum()
    }

    fn request_path(req: &Request) -> &str {
        req.uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
    }

    fn host_value(req: &Request) -> Option<&str> {
        if req.headers().contains_key("host") {
            return None;
        }
        let authority = req.uri().authority()?;
        let scheme = req.uri().scheme_str().unwrap_or("http");
        let default_port = if scheme == "https" { 443 } else { 80 };
        if req.uri().port_u16() == Some(default_port) {
            Some(authority.host())
        } else {
            Some(authority.as_str())
        }
    }

    fn estimate_request_capacity(req: &Request) -> usize {
        let mut cap = req.method().as_str().len()
            + 1
            + Self::request_path(req).len()
            + b" HTTP/1.1\r\n".len();
        cap += Self::header_map_len(req.headers());

        if let Some(host) = Self::host_value(req) {
            cap += b"Host: ".len() + host.len() + 2;
        }

        if !req.headers().contains_key("user-agent") {
            cap += b"User-Agent: sark/0.1\r\n".len();
        }
        if !req.headers().contains_key("accept-encoding") {
            cap += b"Accept-Encoding: gzip\r\n".len();
        }

        let body = req.body();
        if !body.is_empty() && !req.headers().contains_key("content-length") {
            cap += b"Content-Length: ".len() + Self::dec_len(body.len()) + 2;
        }

        cap + 2 + body.len()
    }

    fn write_request_head(buf: &mut Vec<u8>, req: &Request) {
        buf.extend_from_slice(req.method().as_str().as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(Self::request_path(req).as_bytes());
        buf.extend_from_slice(b" HTTP/1.1\r\n");

        if let Some(host) = Self::host_value(req) {
            buf.extend_from_slice(b"Host: ");
            buf.extend_from_slice(host.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        if !req.headers().contains_key("user-agent") {
            buf.extend_from_slice(b"User-Agent: sark/0.1\r\n");
        }

        if !req.headers().contains_key("accept-encoding") {
            buf.extend_from_slice(b"Accept-Encoding: gzip\r\n");
        }

        for (name, value) in req.headers().iter() {
            buf.extend_from_slice(name.as_str().as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(value.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }
}
