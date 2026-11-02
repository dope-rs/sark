use std::{
    io::{self, Read as _, Write as _},
    net::{SocketAddr, TcpStream},
    num,
    os::fd::AsFd as _,
    str, string,
    time::Duration,
};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Utf8(str::Utf8Error),
    Body(string::FromUtf8Error),
    Status(num::ParseIntError),
    MissingStatus,
    Length(num::ParseIntError),
    Overflow { head: usize, body: usize },
    Closed { buffered: usize },
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct Peer<'a> {
    socket: &'a mut TcpStream,
    buffered: &'a mut Vec<u8>,
}

impl<'a> Peer<'a> {
    pub fn new(socket: &'a mut TcpStream, buffered: &'a mut Vec<u8>) -> Self {
        Self { socket, buffered }
    }

    pub fn try_connect(bind: SocketAddr) -> io::Result<TcpStream> {
        let socket = TcpStream::connect(bind)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        Ok(socket)
    }

    pub fn try_bounded(bind: SocketAddr) -> io::Result<TcpStream> {
        use dope::core::{Backend, dope_sys::io::socket::option};
        let socket = Self::try_connect(bind)?;
        for kind in [option::Kind::RecvBuffer, option::Kind::SendBuffer] {
            <Backend as option::Source>::set(socket.as_fd(), kind, &option::Value::new(16 * 1024))?;
        }
        Ok(socket)
    }

    pub fn try_text(self) -> Result<(u16, String), Error> {
        let (status, body) = self.try_response()?;
        Ok((status, String::from_utf8(body).map_err(Error::Body)?))
    }

    pub fn try_exchange(self, wire: &[u8]) -> Result<(u16, Vec<u8>), Error> {
        self.socket.write_all(wire)?;
        self.try_response()
    }

    pub fn try_response(mut self) -> Result<(u16, Vec<u8>), Error> {
        let mut incoming = [0u8; 4096];
        let mut start = 0;
        let end = loop {
            if let Some(end) = self.buffered[start..]
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
            {
                break start + end;
            }
            start = self.buffered.len().saturating_sub(3);
            self.try_read(&mut incoming)?;
        };
        let body = end + 4;
        let head = str::from_utf8(&self.buffered[..end]).map_err(Error::Utf8)?;
        let status = head
            .split_whitespace()
            .nth(1)
            .ok_or(Error::MissingStatus)?
            .parse()
            .map_err(Error::Status)?;
        let len = head
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| name.eq_ignore_ascii_case("content-length").then_some(value))
            .map(|value| value.trim().parse::<usize>())
            .transpose()
            .map_err(Error::Length)?
            .unwrap_or_default();
        let total = body.checked_add(len).ok_or(Error::Overflow {
            head: body,
            body: len,
        })?;
        while self.buffered.len() < total {
            self.try_read(&mut incoming)?;
        }
        let content = self.buffered[body..total].to_vec();
        self.buffered.drain(..total);
        Ok((status, content))
    }

    fn try_read(&mut self, incoming: &mut [u8]) -> Result<(), Error> {
        let received = self.socket.read(incoming).map_err(Error::Io)?;
        if received == 0 {
            return Err(Error::Closed {
                buffered: self.buffered.len(),
            });
        }
        self.buffered.extend_from_slice(&incoming[..received]);
        Ok(())
    }
}
