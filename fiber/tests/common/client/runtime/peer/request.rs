use std::str;

use crate::common::client::{pool, routes, runtime::peer::Error};

/// Borrowed view of one fully framed HTTP/1 request.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Request<'a> {
    wire: &'a [u8],
    head: &'a str,
    method_end: u32,
    target_end: u32,
}

impl<'a> Request<'a> {
    pub(super) fn parse<E>(
        wire: &'a [u8],
        head_end: usize,
        method: u32,
        target: u32,
    ) -> Result<Self, Error<E>> {
        let head = wire.get(..head_end).ok_or(Error::Boundary {
            wire: wire.len(),
            head: head_end,
        })?;
        let text = str::from_utf8(head).map_err(Error::Utf8)?;
        Ok(Self {
            wire,
            head: text,
            method_end: method,
            target_end: target,
        })
    }

    /// Request method as received on the wire.
    #[must_use]
    pub fn method(self) -> &'a str {
        &self.head[..self.method_end as usize]
    }

    /// Request target, including any query component.
    #[must_use]
    pub fn target(self) -> &'a str {
        &self.head[self.method_end as usize + 1..self.target_end as usize]
    }

    /// Fully framed request body.
    #[must_use]
    pub fn body(self) -> &'a [u8] {
        &self.wire[self.head.len()..]
    }

    /// Exact framed request bytes.
    #[must_use]
    pub const fn wire(self) -> &'a [u8] {
        self.wire
    }

    pub fn is_effect(&self) -> bool {
        self.method() == "POST" && self.target() == "/effect" && self.body() == b"effect"
    }

    pub fn try_line<E>(self) -> Result<(), Error<E>> {
        let expected = format!("GET {} HTTP/1.1\r\n", pool::SATURATION_PATH);
        if self.wire().starts_with(expected.as_bytes()) {
            Ok(())
        } else {
            Err(Error::Line {
                actual: self.head.to_owned(),
                expected,
            })
        }
    }

    pub fn origin(request: Request<'_>) -> Vec<u8> {
        if request.target() == "/start" {
            b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else {
            b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\narrived"
                .to_vec()
        }
    }

    pub fn chain(request: Request<'_>) -> Vec<u8> {
        match request.target() {
            "/a" => b"HTTP/1.1 302 Found\r\nLocation: /b\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
            "/b" => b"HTTP/1.1 301 Moved Permanently\r\nLocation: /c\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
            _ => b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone".to_vec(),
        }
    }

    pub fn relative(request: Request<'_>) -> Vec<u8> {
        if request.target() == "/dir/start" {
            b"HTTP/1.1 302 Found\r\nLocation: next\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else if request.target() == "/dir/next" {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok".to_vec()
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                .to_vec()
        }
    }

    pub fn query_path(request: Request<'_>) -> Vec<u8> {
        if request.target() == "/item?step=1" {
            b"HTTP/1.1 302 Found\r\nLocation: ?step=2\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else if request.target() == "/item?step=2" {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok".to_vec()
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                .to_vec()
        }
    }

    pub fn looping(request: Request<'_>) -> Vec<u8> {
        match request.target() {
            "/a" => b"HTTP/1.1 302 Found\r\nLocation: /b\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
            _ => b"HTTP/1.1 302 Found\r\nLocation: /a\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
        }
    }

    pub fn policy_reply(self, status: u16) -> Vec<u8> {
        if self.target() == "/start" {
            format!("HTTP/1.1 {status} Redirect\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").into_bytes()
        } else {
            routes::OK.to_vec()
        }
    }
}
