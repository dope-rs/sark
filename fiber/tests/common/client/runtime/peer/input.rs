use sark_http::h1::{codec::decode::request::Head, framing::RequestBody};

use crate::common::client::runtime::peer::{Error, Request, Socket};

const MAX_HEAD: usize = 64 * 1024;

const MAX_REQUEST: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) struct Frame {
    head_end: usize,
    request_end: usize,
    method_end: u32,
    target_end: u32,
}

pub(super) struct Input {
    bytes: Vec<u8>,
    start: usize,
}

impl Input {
    pub(super) const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            start: 0,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.bytes.len() == self.start
    }

    pub(super) fn next<S: Socket>(
        &mut self,
        stream: &mut S,
    ) -> Result<Option<Frame>, Error<S::Error>> {
        let Some(head_end) = self.read_head(stream)? else {
            return Ok(None);
        };
        let head_end = head_end - self.start;
        let head = Head::scan(&self.bytes[self.start..self.start + head_end], head_end)
            .map_err(Error::Protocol)?
            .ok_or(Error::HeadEnd { bytes: head_end })?;
        let body_len = match head.admission.body() {
            RequestBody::Empty => 0,
            RequestBody::Fixed(size) => size,
            RequestBody::Chunked => return Err(Error::Encoding),
        };
        let method = head.method.len() as u32;
        let target = method + 1 + head.target.len() as u32;
        let end = head_end
            .checked_add(body_len)
            .filter(|end| *end <= MAX_REQUEST)
            .ok_or(Error::Frame {
                start: self.start,
                head: head_end,
                body: body_len,
                max: MAX_REQUEST,
            })?;
        while self.bytes.len() - self.start < end {
            if !self.read(stream)? {
                return Err(Error::Body {
                    have: self.bytes.len() - self.start,
                    need: end,
                });
            }
        }
        Ok(Some(Frame {
            head_end,
            request_end: end,
            method_end: method,
            target_end: target,
        }))
    }

    pub(super) fn request<E>(&self, frame: Frame) -> Result<Request<'_>, Error<E>> {
        Request::parse(
            &self.bytes[self.start..self.start + frame.request_end],
            frame.head_end,
            frame.method_end,
            frame.target_end,
        )
    }

    pub(super) fn consume(&mut self, frame: Frame) {
        self.start += frame.request_end;
        if self.start == self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        }
    }

    pub(super) fn copy_into(&mut self, output: &mut [u8]) -> usize {
        let len = output.len().min(self.bytes.len() - self.start);
        output[..len].copy_from_slice(&self.bytes[self.start..self.start + len]);
        self.start += len;
        if self.start == self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        }
        len
    }

    fn read_head<S: Socket>(&mut self, stream: &mut S) -> Result<Option<usize>, Error<S::Error>> {
        loop {
            if let Some(end) = head_end(&self.bytes[self.start..]) {
                if end > MAX_HEAD {
                    return Err(Error::Head {
                        bytes: end,
                        max: MAX_HEAD,
                    });
                }
                return Ok(Some(self.start + end));
            }
            if self.bytes.len() - self.start >= MAX_HEAD {
                return Err(Error::Head {
                    bytes: self.bytes.len() - self.start,
                    max: MAX_HEAD,
                });
            }
            if !self.read(stream)? {
                return if self.bytes.len() == self.start {
                    Ok(None)
                } else {
                    Err(Error::HeadEnd {
                        bytes: self.bytes.len() - self.start,
                    })
                };
            }
        }
    }

    fn read<S: Socket>(&mut self, stream: &mut S) -> Result<bool, Error<S::Error>> {
        let mut chunk = [0; 4096];
        let read = stream.read(&mut chunk).map_err(Error::Io)?;
        if read == 0 {
            return Ok(false);
        }
        let needed = self.bytes.len().saturating_add(read);
        if (needed > self.bytes.capacity() || needed > MAX_REQUEST) && self.start != 0 {
            self.bytes.copy_within(self.start.., 0);
            self.bytes.truncate(self.bytes.len() - self.start);
            self.start = 0;
        }
        let bytes = self.bytes.len();
        if bytes.saturating_add(read) > MAX_REQUEST {
            return Err(Error::Request {
                bytes,
                incoming: read,
                max: MAX_REQUEST,
            });
        }
        self.bytes.extend_from_slice(&chunk[..read]);
        Ok(true)
    }
}

fn head_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}
