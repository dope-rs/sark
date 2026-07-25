use crate::conn::{Conn, ConnError};
use crate::stream::StreamId;

pub trait StreamTransport {
    type SendError;

    fn recv_stream(&mut self, stream_id: u64) -> Option<Vec<u8>>;
    fn recv_stream_finished(&self, stream_id: u64) -> bool;
    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) -> Result<(), Self::SendError>;
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
        transport.send_stream(write.stream_id.0, &write.bytes)?;
        if write.fin {
            transport.finish_stream(write.stream_id.0)?;
        }
    }
    Ok(())
}
