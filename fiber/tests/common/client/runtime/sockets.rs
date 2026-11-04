use std::{
    convert::Infallible,
    io::{self, Read as _, Write as _},
    net::{Shutdown, SocketAddr, TcpStream},
    str,
    sync::Arc,
    time::Duration,
};

use dope_tls::rustls::{self, pki_types::ServerName};
use o3::base::marker::ThreadBound;
use sark_http::{
    h1::{codec::decode::response, framing::ResponseBody},
    payload::response::Method,
};

pub const UPLOAD_CHUNK: [u8; 8192] = [b'x'; 8192];

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Tls(rustls::Error),
    Truncated,
    Closed {
        buffered: usize,
    },
    Handshake {
        exchanges: usize,
    },
    Stalled,
    Utf8(str::Utf8Error),
    Request {
        found: Option<String>,
        expected: String,
    },
    Protocol(response::Error<Infallible>),
    MissingLength,
    MissingHead,
    Overflow {
        head: usize,
        body: usize,
    },
}

pub struct Tls(Session);

pub(super) struct Session {
    stream: TcpStream,
    state: rustls::Connection,
    pub(super) plain: Vec<u8>,
    peer_closed: bool,
    _bound: ThreadBound,
}

pub struct Plain {
    stream: TcpStream,
    buffered: Vec<u8>,
}

pub struct Response<'a> {
    pub status: u16,
    pub body: &'a [u8],
}

impl<'a> Response<'a> {
    pub fn try_parse(bytes: &'a [u8]) -> Result<Self, Error> {
        let head = response::Decoder::new(Method::Other)
            .scan(bytes, bytes.len())
            .map_err(Error::Protocol)?
            .ok_or(Error::MissingHead)?;
        Ok(Self {
            status: head.status.as_u16(),
            body: &bytes[head.header_len.get() as usize..],
        })
    }
}

impl Plain {
    pub fn connect(addr: SocketAddr) -> Result<Self, Error> {
        Ok(Self {
            stream: connect(addr).map_err(Error::Io)?,
            buffered: Vec::new(),
        })
    }

    pub fn request(&mut self, path: &str, addr: SocketAddr) -> Result<(), Error> {
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n");
        self.stream.write_all(request.as_bytes()).map_err(Error::Io)
    }

    pub fn response(&mut self) -> Result<Vec<u8>, Error> {
        loop {
            if let Some(end) = response_end(&self.buffered)? {
                return Ok(self.buffered.drain(..end).collect());
            }
            let mut chunk = [0; 8192];
            let read = self.stream.read(&mut chunk).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed {
                    buffered: self.buffered.len(),
                });
            }
            self.buffered.extend_from_slice(&chunk[..read]);
        }
    }
}

impl Tls {
    pub fn connect(
        addr: SocketAddr,
        config: Arc<rustls::ClientConfig>,
        name: ServerName<'static>,
    ) -> Result<Self, Error> {
        let state = rustls::ClientConnection::new(config, name).map_err(Error::Tls)?;
        let mut client = Session::new(connect(addr).map_err(Error::Io)?, state.into());
        client.handshake()?;
        Ok(Self(client))
    }

    pub fn request(&mut self, path: &str, addr: SocketAddr) -> Result<(), Error> {
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n");
        self.write(request.as_bytes())
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.0.write(bytes)
    }

    pub fn close(mut self) -> Result<Vec<u8>, Error> {
        self.0.state.send_close_notify();
        self.0.flush()?;
        self.finish()
    }

    pub fn close_after(mut self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.state.writer().write_all(bytes).map_err(Error::Io)?;
        self.0.state.send_close_notify();
        let mut records = Vec::new();
        while self.0.state.wants_write() {
            if self.0.state.write_tls(&mut records).map_err(Error::Io)? == 0 {
                return Err(Error::Stalled);
            }
        }
        self.0.stream.write_all(&records).map_err(Error::Io)?;
        self.finish()
    }

    fn finish(mut self) -> Result<Vec<u8>, Error> {
        self.0.stream.shutdown(Shutdown::Write).map_err(Error::Io)?;
        while self.0.read()? != 0 {}
        if self.0.peer_closed {
            Ok(self.0.plain)
        } else {
            Err(Error::Truncated)
        }
    }

    pub fn response(&mut self) -> Result<Vec<u8>, Error> {
        loop {
            if let Some(end) = response_end(&self.0.plain)? {
                return Ok(self.0.plain.drain(..end).collect());
            }
            if self.0.read()? == 0 {
                return Err(Error::Closed {
                    buffered: self.0.plain.len(),
                });
            }
        }
    }
}

impl Session {
    pub(super) fn new(stream: TcpStream, state: rustls::Connection) -> Self {
        Self {
            stream,
            state,
            plain: Vec::new(),
            peer_closed: false,
            _bound: ThreadBound::NEW,
        }
    }

    pub(super) fn handshake(&mut self) -> Result<(), Error> {
        for _ in 0..64 {
            self.flush()?;
            if !self.state.is_handshaking() {
                return Ok(());
            }
            if self.read()? == 0 {
                return Err(Error::Closed {
                    buffered: self.plain.len(),
                });
            }
        }
        Err(Error::Handshake { exchanges: 64 })
    }

    pub(super) fn read(&mut self) -> Result<usize, Error> {
        self.flush()?;
        let read = self.state.read_tls(&mut self.stream).map_err(Error::Io)?;
        if read == 0 {
            return Ok(0);
        }
        let state = self.state.process_new_packets().map_err(Error::Tls)?;
        self.peer_closed |= state.peer_has_closed();
        match self.state.reader().read_to_end(&mut self.plain) {
            Ok(_) => {}
            Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => {}
            Err(cause) => return Err(Error::Io(cause)),
        }
        self.flush()?;
        Ok(read)
    }

    pub(super) fn write(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        while !bytes.is_empty() {
            self.flush()?;
            let written = self.state.writer().write(bytes).map_err(Error::Io)?;
            if written == 0 {
                return Err(Error::Stalled);
            }
            bytes = &bytes[written..];
        }
        self.flush()
    }

    fn flush(&mut self) -> Result<(), Error> {
        while self.state.wants_write() {
            if self.state.write_tls(&mut self.stream).map_err(Error::Io)? == 0 {
                return Err(Error::Stalled);
            }
        }
        Ok(())
    }
}

fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    Ok(stream)
}

fn response_end(bytes: &[u8]) -> Result<Option<usize>, Error> {
    let Some(head) = response::Decoder::new(Method::Other)
        .scan(bytes, bytes.len())
        .map_err(Error::Protocol)?
    else {
        return Ok(None);
    };
    let ResponseBody::Fixed(body) = head.payload else {
        return Err(Error::MissingLength);
    };
    let head = head.header_len.get() as usize;
    let end = head
        .checked_add(body)
        .ok_or(Error::Overflow { head, body })?;
    Ok((bytes.len() >= end).then_some(end))
}
