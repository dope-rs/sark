use sark_client::protocol::codec::output;

use crate::common::client::{
    pool,
    runtime::{
        calls,
        peer::{Error, Request, Socket, input},
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
