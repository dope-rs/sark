use std::{
    io::{self, Read, Write},
    net::{self, TcpStream},
    str,
    time::{Duration, Instant},
};

pub trait Endpoint {
    fn try_get(self, path: &str) -> io::Result<Vec<u8>>;
    fn try_request(self, parts: &[&[u8]]) -> io::Result<Vec<u8>>;
    fn try_exchange(self, target: &[u8]) -> io::Result<Vec<u8>>;
}

impl Endpoint for net::SocketAddr {
    fn try_get(self, path: &str) -> io::Result<Vec<u8>> {
        let req = format!("GET {path} HTTP/1.1\r\nHost: {self}\r\nConnection: close\r\n\r\n");
        self.try_request(&[req.as_bytes()])
    }

    fn try_request(self, parts: &[&[u8]]) -> io::Result<Vec<u8>> {
        let mut stream = TcpStream::connect(self)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        for part in parts {
            stream.write_all(part)?;
        }
        let mut buf = Vec::new();
        let mut end = None;
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if end.is_none()
                && let Some(body_at) = body_start(&buf)
            {
                end = Some(
                    try_length(&buf[..body_at])?
                        .map(|cl| {
                            body_at.checked_add(cl).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "response length overflow",
                                )
                            })
                        })
                        .transpose()?,
                );
            }
            if end.flatten().is_some_and(|end| buf.len() >= end) {
                break;
            }
        }
        Ok(buf)
    }

    fn try_exchange(self, target: &[u8]) -> io::Result<Vec<u8>> {
        let mut stream = TcpStream::connect(self)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut request = b"GET ".to_vec();
        request.extend_from_slice(target);
        request.extend_from_slice(b" HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n");
        stream.write_all(&request)?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        Ok(response)
    }
}

fn body_start(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn try_length(mut headers: &[u8]) -> io::Result<Option<usize>> {
    while let Some(end) = headers.windows(2).position(|bytes| bytes == b"\r\n") {
        let (line, tail) = headers.split_at(end);
        headers = &tail[2..];
        let Some((name, value)) = line.split_at_checked(b"content-length:".len()) else {
            continue;
        };
        if name.eq_ignore_ascii_case(b"content-length:") {
            let value = str::from_utf8(value)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            return value
                .trim()
                .parse()
                .map(Some)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
        }
    }
    Ok(None)
}

pub trait Response {
    fn status_line(&self) -> &[u8];
}

impl Response for [u8] {
    fn status_line(&self) -> &[u8] {
        let end = match self.windows(2).position(|w| w == b"\r\n") {
            Some(end) => end,
            None => self.len(),
        };
        &self[..end]
    }
}

pub trait Receive {
    fn try_head(&self) -> io::Result<Vec<u8>>;
    fn try_to_close(&self) -> io::Result<(Vec<u8>, Duration)>;
}

impl Receive for TcpStream {
    fn try_head(mut self: &Self) -> io::Result<Vec<u8>> {
        self.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = self.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            if out.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        Ok(out)
    }

    fn try_to_close(mut self: &Self) -> io::Result<(Vec<u8>, Duration)> {
        let start = Instant::now();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = self.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        Ok((buf, start.elapsed()))
    }
}
