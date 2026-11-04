use sark_client::protocol::codec::output;

use crate::common::client::{
    pool,
    runtime::{
        calls,
        peer::{Error, Request, Response as _, Socket, input},
    },
};

/// One accepted HTTP connection with owned request framing state.
pub struct Conn<S> {
    stream: S,
    input: input::Input,
    frame: Option<input::Frame>,
}

impl<S: Socket> Conn<S> {
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            input: input::Input::new(),
            frame: None,
        }
    }

    /// Reads the next fully framed request.
    pub fn try_request(&mut self) -> Result<Option<Request<'_>>, Error<S::Error>> {
        self.discard();
        let Some(frame) = self.input.next(&mut self.stream)? else {
            return Ok(None);
        };
        self.frame = Some(frame);
        self.input.request(frame).map(Some)
    }

    /// Attempts one immediate raw read after the current request.
    pub fn try_read(&mut self, output: &mut [u8]) -> Result<Option<usize>, S::Error> {
        self.discard();
        let copied = self.input.copy_into(output);
        if copied != 0 || output.is_empty() {
            return Ok(Some(copied));
        }
        self.stream.try_read(output)
    }

    pub fn try_fill(&mut self) -> Result<(), Error<S::Error>> {
        for _ in 0..pool::SATURATION_INFLIGHT {
            self.try_request()?
                .ok_or(Error::UnexpectedEof)?
                .try_line()?;
        }
        Ok(())
    }

    pub fn try_probe(&mut self) -> Result<bool, Error<S::Error>> {
        match self.try_read(&mut [0])? {
            Some(0) => Err(Error::UnexpectedEof),
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn try_release(&mut self) -> Result<(), Error<S::Error>> {
        for _ in 0..pool::SATURATION_INFLIGHT {
            self.write_all(pool::SATURATION_RESPONSE)?;
        }
        Ok(())
    }

    pub fn try_final(&mut self, request: usize) -> Result<(), Error<S::Error>> {
        let headers = if request == 2 {
            &[("Connection", "close"), ("Content-Length", "2")][..]
        } else {
            &[("Content-Length", "2")][..]
        };
        self.write_all(&"HTTP/1.1 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    pub fn try_excess_response(&mut self, request: usize) -> Result<(), Error<S::Error>> {
        if request == 2 {
            return self
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .map_err(Error::Io);
        }
        let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        // The head, each chunk and the terminator each consume one decode permit.
        for _ in 0..dope::core::driver::schedule::LIMIT - 2 {
            wire.extend_from_slice(b"1\r\nx\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\nHTTP/1.1 299 Unsolicited\r\nContent-Length: 0\r\n\r\n");
        self.write_all(&wire).map_err(Error::Io)
    }

    pub fn try_http10(&mut self, request: usize, persist: bool) -> Result<(), Error<S::Error>> {
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

    pub fn try_bounded(&mut self, request: usize) -> Result<(), Error<S::Error>> {
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

    pub fn try_reset(&mut self, request: usize) -> Result<(), Error<S::Error>> {
        let headers = match request {
            1 => &[("Keep-Alive", "timeout=2"), ("Content-Length", "2")][..],
            2 => &[("Content-Length", "2")][..],
            _ => &[("Connection", "close"), ("Content-Length", "2")][..],
        };
        self.write_all(&"HTTP/1.1 200 OK".response(headers, b"ok"))
            .map_err(Error::Io)
    }

    /// Sends a chunked response followed by an empty pipelined response.
    pub fn pipeline(&mut self) -> Result<(), Error<S::Error>> {
        for _ in 0..2 {
            if self.try_request()?.is_none() {
                return Err(Error::UnexpectedEof);
            }
        }
        let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        let head = format!("{:x}\r\n", output::DATA_LIMIT);
        for _ in 0..calls::CHUNKS {
            wire.extend_from_slice(head.as_bytes());
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

    pub fn read(&mut self, output: &mut [u8]) -> Result<usize, S::Error> {
        self.discard();
        let copied = self.input.copy_into(output);
        if copied != 0 || output.is_empty() {
            return Ok(copied);
        }
        self.stream.read(output)
    }

    pub fn write_all(&mut self, input: &[u8]) -> Result<(), S::Error> {
        self.discard();
        self.stream.write_all(input)
    }
}
