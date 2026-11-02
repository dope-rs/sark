use std::{
    cell::{Cell, RefCell},
    io::{self, Read, Write},
    marker::PhantomPinned,
    net::TcpStream,
    ops::ControlFlow,
    str::Utf8Error,
    time::Duration,
};

use sark_ws::{frame, server, server::app::Sink, upgrade};

pub mod borrowed;
mod closing;
pub use closing::Closing;
pub mod meter;

#[derive(Debug)]
pub enum Error {
    Text(Utf8Error),
    Missing,
    Key(upgrade::KeyError),
}

pub struct Upgrade<'a> {
    stream: &'a TcpStream,
}

impl<'a> Upgrade<'a> {
    pub fn new(stream: &'a TcpStream) -> Self {
        Self { stream }
    }
    pub fn try_head(mut self) -> io::Result<Vec<u8>> {
        self.stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        let mut head = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let read = self.stream.read(&mut chunk)?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            head.extend_from_slice(&chunk[..read]);
            if head.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(head);
            }
        }
    }

    pub fn try_respond(mut self, accept: &[u8; 28]) -> io::Result<()> {
        self.stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ")?;
        self.stream.write_all(accept)?;
        self.stream.write_all(b"\r\n\r\n")
    }
}

pub struct Head<'a> {
    bytes: &'a [u8],
}

impl<'a> Head<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub fn try_accept(self) -> Result<[u8; 28], Error> {
        let req = str::from_utf8(self.bytes).map_err(Error::Text)?;
        let value = req
            .split("\r\n")
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("sec-websocket-key")
                    .then(|| value.trim())
            })
            .ok_or(Error::Missing)?;
        Ok(upgrade::Key::parse(value.as_bytes())
            .map_err(Error::Key)?
            .accept())
    }
}

pub struct Reply<'a> {
    pub text: &'a str,
    pub _pin: PhantomPinned,
    pub failed: &'a dyn Fn(server::Error),
}

impl Sink for Reply<'_> {
    fn capacity(&self, _: usize) -> Result<usize, server::Error> {
        Ok(frame::Plan::new(0x1, true, self.text.len(), false)?.wire_len())
    }

    fn message(
        &self,
        message: server::Message<'_>,
        out: &mut server::Response<'_>,
    ) -> Result<ControlFlow<()>, server::Error> {
        if matches!(message, server::Message::Text("ping")) {
            out.try_text(self.text)?;
        }
        Ok(ControlFlow::Continue(()))
    }

    fn failed(&self, error: server::Error) {
        (self.failed)(error);
    }
}

pub mod saturate;

pub struct Authorized<'a> {
    pub openings: &'a Cell<usize>,
    pub messages: &'a Cell<usize>,
    pub failures: &'a RefCell<Vec<server::Error>>,
}

impl Sink for Authorized<'_> {
    fn opening(&self, request: server::Opening<'_>) -> Result<(), server::Error> {
        self.openings.set(self.openings.get() + 1);
        assert_eq!(request.authority, "x");
        assert_eq!(request.target, "/");
        if request.origin == Some(b"https://allowed.test")
            && request.cookie == Some(b"session=good")
        {
            Ok(())
        } else {
            Err(server::Error::Forbidden)
        }
    }

    fn capacity(&self, _: usize) -> Result<usize, server::Error> {
        Ok(0)
    }

    fn message(
        &self,
        _: server::Message<'_>,
        _: &mut server::Response<'_>,
    ) -> Result<ControlFlow<()>, server::Error> {
        self.messages.set(self.messages.get() + 1);
        Ok(ControlFlow::Break(()))
    }

    fn failed(&self, error: server::Error) {
        self.failures.borrow_mut().push(error);
    }
}
