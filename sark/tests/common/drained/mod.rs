use sark_core::decode::Decode;
use sark_h2::{
    conn::event::Write,
    frame::{
        self,
        controls::RstStream,
        layout::Header,
        numbers::{ErrorCode, ParseError, Type},
    },
    server::sync,
    stream::Id,
};

mod chunk;
mod error;
mod segment;
pub use chunk::Chunk;
pub use error::Error;
pub use segment::Segment;

#[derive(Debug, Default)]
pub struct Drained {
    pub wire: Vec<u8>,
    pub retained: Vec<Chunk>,
}

impl Drained {
    pub fn try_for<R: Decode>(server: &mut sync::Conn<'_, '_, R>) -> Result<Self, Error> {
        let mut prefix = [0_u8; 64 << 10];
        let mut drained = Self::default();
        while !server.output().is_empty() {
            let write = server
                .transfer()
                .take_write(&mut prefix)
                .map_err(Error::Transfer)?;
            match write {
                Write::Buffered(written) => {
                    if written == 0 {
                        return Err(Error::EmptyWrite);
                    }
                    drained.wire.extend_from_slice(&prefix[..written]);
                }
                Write::Owned {
                    prefix_len: len,
                    body,
                    stream_id: _,
                } => {
                    drained.wire.extend_from_slice(&prefix[..len]);
                    drained.wire.extend_from_slice(body.as_slice());
                }
                Write::Split {
                    write,
                    prefix_len: len,
                    body,
                } => {
                    let body_at = drained.wire.len() + len;
                    drained.wire.extend_from_slice(&prefix[..len]);
                    drained.wire.extend_from_slice(body.as_slice());
                    drained.retained.push(Chunk {
                        stream: write.stream_id,
                        body_at,
                        pointer: body.as_slice().as_ptr().addr(),
                        len: body.len(),
                    });
                }
            }
        }
        Ok(drained)
    }

    fn try_frames(
        &self,
        mut visit: impl FnMut(Header, &[u8], usize) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut offset = 0;
        while offset != self.wire.len() {
            let header = Header::parse(&self.wire[offset..]).map_err(Error::Parse)?;
            let body = Error::add(offset, frame::HEADER_LEN)?;
            let end = Error::add(body, header.length.into_usize())?;
            let payload = self
                .wire
                .get(body..end)
                .ok_or(Error::Parse(ParseError::Need {
                    actual: self.wire.len() - offset,
                    required: end - offset,
                }))?;
            visit(header, payload, body)?;
            offset = end;
        }
        Ok(())
    }

    pub fn try_visit(
        &self,
        mut visit: impl FnMut(Segment<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.try_frames(|header, payload, body_at| {
            if header.kind != Type::Data {
                return Ok(());
            }
            let owner = self.retained.iter().find(|chunk| chunk.body_at == body_at);
            if let Some(chunk) = owner
                && (chunk.stream.id() != header.stream_id || chunk.len != payload.len())
            {
                return Err(Error::Owner {
                    stream: header.stream_id,
                    owner: chunk.stream,
                    expected: payload.len(),
                    actual: chunk.len,
                });
            }
            visit(Segment {
                stream: header.stream_id,
                payload,
                owner,
            })
        })
    }

    pub fn try_data(&self, stream: Id) -> Result<Vec<u8>, Error> {
        let mut data = Vec::new();
        self.try_visit(|part| {
            if part.stream == stream {
                data.extend_from_slice(part.payload);
            }
            Ok(())
        })?;
        Ok(data)
    }

    pub fn try_account(&self, stream: Id, owner: usize, sent: &mut usize) -> Result<usize, Error> {
        let mut held = 0;
        self.try_visit(|part| {
            if part.stream != stream {
                return Ok(());
            }
            if let Some(chunk) = part.owner {
                let expected = Error::add(owner, *sent)?;
                if chunk.pointer != expected {
                    return Err(Error::Pointer {
                        expected,
                        actual: chunk.pointer,
                    });
                }
                held = Error::add(held, chunk.len)?;
            }
            *sent = Error::add(*sent, part.payload.len())?;
            Ok(())
        })?;
        Ok(held)
    }

    pub fn retained_len(&self) -> usize {
        self.retained.iter().map(|chunk| chunk.len).sum()
    }

    pub fn try_all(&self) -> Result<Vec<u8>, Error> {
        let mut data = Vec::new();
        self.try_visit(|part| {
            data.extend_from_slice(part.payload);
            Ok(())
        })?;
        Ok(data)
    }

    pub fn try_reset(&self) -> Result<Option<ErrorCode>, Error> {
        let mut reset = None;
        self.try_frames(|header, payload, _| {
            if header.kind == Type::RstStream {
                reset = Some(
                    RstStream::parse(header, payload)
                        .map_err(Error::Parse)?
                        .error(),
                );
            }
            Ok(())
        })?;
        Ok(reset)
    }
}
