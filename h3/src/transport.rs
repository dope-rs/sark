use o3::buffer::{Bytes, InlineBytes, Retained};

use crate::conn::{Conn, ConnError, WritePayload, WritePrefix};
use crate::stream::StreamId;

pub trait StreamTransport {
    type SendError;

    fn recv_stream(&mut self, stream_id: u64) -> Option<Vec<u8>>;
    fn recv_stream_finished(&self, stream_id: u64) -> bool;
    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) -> Result<(), Self::SendError>;
    fn send_stream_owned(&mut self, stream_id: u64, bytes: Vec<u8>) -> Result<(), Self::SendError> {
        self.send_stream(stream_id, &bytes)
    }
    fn send_stream_inline(
        &mut self,
        stream_id: u64,
        bytes: InlineBytes,
    ) -> Result<(), Self::SendError> {
        self.send_stream(stream_id, bytes.as_slice())
    }
    fn send_stream_retained(
        &mut self,
        stream_id: u64,
        bytes: Bytes<Retained>,
    ) -> Result<(), Self::SendError> {
        self.send_stream(stream_id, bytes.as_slice())
    }
    fn finish_stream(&mut self, stream_id: u64) -> Result<(), Self::SendError>;
}

pub fn pump_stream_event<T: StreamTransport>(
    conn: &mut Conn,
    transport: &mut T,
    stream_id: u64,
) -> Result<(), ConnError> {
    let bytes = transport.recv_stream(stream_id);
    let fin = transport.recv_stream_finished(stream_id);
    match bytes {
        Some(bytes) => conn.ingest_stream_owned(StreamId::new(stream_id), bytes, fin),
        None if fin => conn.ingest_stream(StreamId::new(stream_id), &[], true),
        None => Ok(()),
    }
}

pub fn pump_writes<T: StreamTransport>(
    conn: &mut Conn,
    transport: &mut T,
) -> Result<(), T::SendError> {
    while let Some(write) = conn.poll_write() {
        let stream_id = write.stream_id.0;
        match write.prefix {
            WritePrefix::Inline(prefix) => transport.send_stream_inline(stream_id, prefix)?,
            WritePrefix::Owned(bytes) => transport.send_stream_owned(stream_id, bytes)?,
        }
        match write.payload {
            Some(WritePayload::Owned(bytes)) => transport.send_stream_owned(stream_id, bytes)?,
            Some(WritePayload::Retained(bytes)) => {
                transport.send_stream_retained(stream_id, bytes)?;
            }
            None => {}
        }
        if write.fin {
            transport.finish_stream(stream_id)?;
        }
    }
    Ok(())
}
