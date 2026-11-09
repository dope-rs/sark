//! Shared harness for ws integration tests: spin up the real echo server and
//! talk to it over a socket with masked client frames.

use std::{
    io::{self, ErrorKind, Read as _, Write as _},
    net::{SocketAddr, TcpStream},
    task::Poll,
    time::Duration,
};

use dope_fiber::{BlockOn, abi};
use dope_test::fibers;
use sark_ws::{frame, mask};

use crate::common::{echo::OPENING, upgrade::client::ACCEPT};

#[derive(Debug)]
pub enum Read {
    Io(io::Error),
    Drive(fibers::Run),
    Frame(frame::Error),
    Head(Vec<u8>),
    Unmasked,
}

pub fn try_poll<'d, T>(
    app: &mut impl BlockOn<'d>,
    run: &mut impl FnMut() -> Result<T, Read>,
) -> Result<T, Read> {
    fibers::TEST
        .try_run(
            app,
            abi::poll_fn(|cx| match run() {
                Err(Read::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) =>
                {
                    cx.wake();
                    Poll::Pending
                }
                result => Poll::Ready(result),
            }),
        )
        .map_err(Read::Drive)?
}

pub fn try_write<'d>(
    app: &mut impl BlockOn<'d>,
    mut socket: &TcpStream,
    mut bytes: &[u8],
) -> Result<(), Read> {
    try_poll(app, &mut || {
        while !bytes.is_empty() {
            let written = socket.write(bytes).map_err(Read::Io)?;
            if written == 0 {
                return Err(Read::Io(ErrorKind::WriteZero.into()));
            }
            bytes = &bytes[written..];
        }
        Ok(())
    })
}

pub trait Peer: Sized {
    fn try_socket(bind: SocketAddr) -> io::Result<Self>;
    fn try_connect(bind: SocketAddr) -> Result<(Self, Vec<u8>), Read>;
    fn try_upgrade(&self, buf: &mut Vec<u8>) -> Result<Vec<u8>, Read>;
    fn try_head(&self, buf: &mut Vec<u8>) -> io::Result<(Vec<u8>, Vec<u8>)>;
    fn try_next(&self, buf: &mut Vec<u8>) -> Result<(u8, Vec<u8>), Read>;
    fn try_client(&self, buf: &mut Vec<u8>) -> Result<(bool, u8, Vec<u8>), Read>;
}

impl Peer for TcpStream {
    fn try_socket(bind: SocketAddr) -> io::Result<Self> {
        let sock = Self::connect(bind)?;
        sock.set_nodelay(true)?;
        sock.set_read_timeout(Some(Duration::from_secs(5)))?;
        Ok(sock)
    }

    fn try_connect(bind: SocketAddr) -> Result<(Self, Vec<u8>), Read> {
        let mut sock = Self::try_socket(bind).map_err(Read::Io)?;
        sock.write_all(OPENING).map_err(Read::Io)?;
        let tail = sock.try_upgrade(&mut Vec::new())?;
        Ok((sock, tail))
    }

    fn try_upgrade(&self, buf: &mut Vec<u8>) -> Result<Vec<u8>, Read> {
        let (head, tail) = self.try_head(buf).map_err(Read::Io)?;
        if !head.starts_with(b"HTTP/1.1 101")
            || !head.windows(ACCEPT.len()).any(|part| part == ACCEPT)
        {
            return Err(Read::Head(head));
        }
        Ok(tail)
    }

    fn try_head(&self, buf: &mut Vec<u8>) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let mut sock = self;
        let mut tmp = [0u8; 1024];
        loop {
            if let Some(end) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
                let tail = buf.split_off(end + 4);
                return Ok((std::mem::take(buf), tail));
            }
            let read = sock.read(&mut tmp)?;
            if read == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "HTTP response head",
                ));
            }
            buf.extend_from_slice(&tmp[..read]);
        }
    }

    fn try_next(&self, buf: &mut Vec<u8>) -> Result<(u8, Vec<u8>), Read> {
        let (head, payload) = read_frame::<4096>(self, buf)?;
        Ok((head.opcode, payload))
    }

    fn try_client(&self, buf: &mut Vec<u8>) -> Result<(bool, u8, Vec<u8>), Read> {
        let (head, mut payload) = read_frame::<{ 64 * 1024 }>(self, buf)?;
        let key = head.mask.ok_or(Read::Unmasked)?;
        mask::Key::new(key).apply(&mut payload);
        Ok((head.fin, head.opcode, payload))
    }
}

fn read_frame<const CAPACITY: usize>(
    mut sock: &TcpStream,
    buf: &mut Vec<u8>,
) -> Result<(frame::Head, Vec<u8>), Read> {
    let mut tmp = [0; CAPACITY];
    loop {
        if let Some(head) = frame::Head::parse(buf, 0, usize::MAX).map_err(Read::Frame)?
            && buf.len() >= head.payload_end().get()
        {
            let payload = buf[head.start().get()..head.payload_end().get()].to_vec();
            buf.drain(..head.payload_end().get());
            return Ok((head, payload));
        }
        let read = sock.read(&mut tmp).map_err(Read::Io)?;
        if read == 0 {
            return Err(Read::Io(io::Error::new(
                ErrorKind::UnexpectedEof,
                "WebSocket frame",
            )));
        }
        buf.extend_from_slice(&tmp[..read]);
    }
}
