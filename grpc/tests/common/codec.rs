use o3::buffer;
use prost::bytes::Buf as _;
use sark_grpc::{Codec, Status, encode, message};

/// Byte-vector codec for gRPC protocol tests.
pub struct Bytes;

impl Bytes {
    fn read<C: buffer::raw::Segment>(input: message::View<'_, C>) -> Vec<u8> {
        let mut cursor = input.cursor();
        let mut bytes = Vec::with_capacity(cursor.remaining());
        while cursor.remaining() != 0 {
            let chunk = cursor.chunk();
            bytes.extend_from_slice(chunk);
            let len = chunk.len();
            cursor.advance(len);
        }
        bytes
    }
}

impl Codec for Bytes {
    type Encode = Vec<u8>;
    type Decode<'a, C: buffer::raw::Segment>
        = Vec<u8>
    where
        C: 'a;
    type Owned<C: buffer::raw::Segment> = Vec<u8>;

    fn encode<S: encode::Sink>(
        &mut self,
        message: &Self::Encode,
        sink: S,
    ) -> Result<S::Output, Status> {
        sink.try_slices([message.as_slice()])
    }

    fn decode<'a, C: buffer::raw::Segment + 'a>(
        &mut self,
        input: message::View<'a, C>,
    ) -> Result<Self::Decode<'a, C>, Status> {
        Ok(Self::read(input))
    }

    fn decode_frame<C: buffer::raw::Segment>(
        &mut self,
        frame: message::Frame<C>,
    ) -> Result<Self::Owned<C>, Status> {
        Ok(Self::read(frame.view()))
    }
}

#[derive(Debug)]
pub enum Parse {
    Header { remaining: usize },
    Compression(u8),
    Payload { expected: usize, remaining: usize },
}

impl Bytes {
    pub fn try_frame(
        mode: message::Compression,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), message::Error> {
        out.extend_from_slice(&message::Frame::try_header(mode, payload.len())?);
        out.extend_from_slice(payload);
        Ok(())
    }

    pub fn try_messages(mut bytes: &[u8]) -> Result<Vec<Vec<u8>>, Parse> {
        let mut msgs = Vec::new();
        while !bytes.is_empty() {
            let (header, tail) = bytes.split_first_chunk::<5>().ok_or(Parse::Header {
                remaining: bytes.len(),
            })?;
            if header[0] != 0 {
                return Err(Parse::Compression(header[0]));
            }
            let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let (payload, tail) = tail.split_at_checked(len).ok_or(Parse::Payload {
                expected: len,
                remaining: tail.len(),
            })?;
            msgs.push(payload.to_vec());
            bytes = tail;
        }
        Ok(msgs)
    }

    pub fn try_frames(
        count: usize,
        size: usize,
        mode: message::Compression,
        byte: u8,
    ) -> Result<Vec<u8>, message::Error> {
        let mut out = Vec::new();
        let payload = vec![byte; size];
        for _ in 0..count {
            Self::try_frame(mode, &payload, &mut out)?;
        }
        Ok(out)
    }
}
