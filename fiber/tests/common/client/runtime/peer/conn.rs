use std::{io, io::Write as _};

use sark_client::protocol::codec::output;

use crate::common::client::{
    pool,
    runtime::{
        calls,
        peer::{Error, Ready, Request, Response as _, input},
    },
};

/// One accepted HTTP connection with owned request framing state.
pub struct Conn<S> {
    stream: S,
    input: input::Input,
    frame: Option<input::Frame>,
}

impl<S: io::Read + io::Write> Conn<S> {
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            input: input::Input::new(),
            frame: None,
        }
    }

    /// Reads the next fully framed request.
    pub fn try_request(&mut self) -> Result<Option<Request<'_>>, Error> {
        self.discard();
        let Some(frame) = self.input.next(&mut self.stream)? else {
            return Ok(None);
        };
        self.frame = Some(frame);
        self.input.request(frame).map(Some)
    }

    /// Attempts one immediate raw read after the current request.
    pub fn try_read(&mut self, output: &mut [u8]) -> io::Result<usize>
    where
        S: Ready,
    {
        self.discard();
        let copied = self.input.copy_into(output);
        if copied != 0 || output.is_empty() {
            return Ok(copied);
        }
        self.stream.try_read(output)
    }

    pub fn try_fill(&mut self) -> Result<(), Error> {
        for _ in 0..pool::SATURATION_INFLIGHT {
            self.try_request()?
                .ok_or(Error::Io(io::ErrorKind::UnexpectedEof.into()))?
                .try_line()?;
        }
        Ok(())
    }

    pub fn try_probe(&mut self) -> io::Result<bool>
    where
        S: Ready,
    {
        let mut byte = [0];
        match self.try_read(&mut byte) {
            Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn try_release(&mut self) -> Result<(), Error> {
        for _ in 0..pool::SATURATION_INFLIGHT {
            self.write_all(pool::SATURATION_RESPONSE)?;
        }
        Ok(())
    }

    pub fn try_tail(&mut self) -> Result<bool, Error> {
        match self.try_request() {
            Ok(Some(request)) => {
                request.try_line()?;
                self.write_all(pool::SATURATION_LAST_RESPONSE)?;
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(Error::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub fn try_final(&mut self, request: usize) -> Result<(), Error> {
        let headers = if request == 2 {
            &[("Connection", "close"), ("Content-Length", "2")][..]
        } else {
            &[("Content-Length", "2")][..]
        };
        self.write_all(&"HTTP/1.1 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    pub fn try_http10(&mut self, request: usize, persist: bool) -> Result<(), Error> {
        let headers = if request == 1 && persist {
            &[("Connection", "keep-alive"), ("Content-Length", "2")][..]
        } else if request == 1 {
            &[("Content-Length", "2")][..]
        } else {
            &[("Connection", "close"), ("Content-Length", "2")][..]
        };
        self.write_all(&"HTTP/1.0 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    pub fn try_bounded(&mut self, request: usize) -> Result<(), Error> {
        let headers = if request == 1 {
            &[
                ("Keep-Alive", "timeout=18446744073709551615"),
                ("Content-Length", "2"),
            ][..]
        } else {
            &[("Connection", "close"), ("Content-Length", "2")][..]
        };
        self.write_all(&"HTTP/1.1 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    pub fn try_reset(&mut self, request: usize) -> Result<(), Error> {
        let headers = match request {
            1 => &[("Keep-Alive", "timeout=2"), ("Content-Length", "2")][..],
            2 => &[("Content-Length", "2")][..],
            _ => &[("Connection", "close"), ("Content-Length", "2")][..],
        };
        self.write_all(&"HTTP/1.1 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    /// Sends a chunked response followed by an empty pipelined response.
    pub fn pipeline(&mut self) -> Result<(), Error> {
        for _ in 0..2 {
            if self.try_request()?.is_none() {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
        }
        let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for _ in 0..calls::CHUNKS {
            write!(wire, "{:x}\r\n", output::DATA_LIMIT)?;
            wire.resize(wire.len() + output::DATA_LIMIT, b'x');
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n");
        self.write_all(&wire).map_err(Into::into)
    }

    fn discard(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.input.consume(frame);
        }
    }
}

impl<S: io::Read + io::Write> io::Read for Conn<S> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.discard();
        let copied = self.input.copy_into(output);
        if copied != 0 || output.is_empty() {
            return Ok(copied);
        }
        io::Read::read(&mut self.stream, output)
    }
}

impl<S: io::Read + io::Write> io::Write for Conn<S> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.discard();
        io::Write::write(&mut self.stream, input)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.discard();
        io::Write::flush(&mut self.stream)
    }
}
