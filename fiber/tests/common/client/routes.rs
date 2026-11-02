use crate::common::client::runtime::peer::Request;

pub struct Seen {
    method: String,
    target: String,
    body: Vec<u8>,
}

impl From<Request<'_>> for Seen {
    fn from(request: Request<'_>) -> Self {
        Self {
            method: request.method().to_owned(),
            target: request.target().to_owned(),
            body: request.body().to_vec(),
        }
    }
}

impl Seen {
    pub fn method(&self) -> &str {
        &self.method
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

pub const CROSS_ORIGIN: &[u8] = b"HTTP/1.1 302 Found\r\nLocation: http://other.example/x\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";

pub const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
pub const ARRIVED: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\narrived";
pub const FIRST: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nA";
pub const SECOND: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nB";
pub const TERMINAL: &[u8] = b"HTTP/1.1 304 Not Modified\r\nConnection: keep-alive\r\n\r\n";
pub const SECRETS: [(&str, &str); 3] = [
    ("Authorization", "Bearer secret"),
    ("Cookie", "session=secret"),
    ("X-Api-Key", "secret"),
];

pub const SECRET_BODY: &[u8] = b"secret";
