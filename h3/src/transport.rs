use crate::conn::{Conn, ConnError};
use crate::stream::StreamId;

pub trait StreamTransport {
    type SendError;

    fn recv_stream(&mut self, stream_id: u64, out: &mut Vec<u8>) -> usize;
    fn recv_stream_finished(&self, stream_id: u64) -> bool;
    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) -> Result<(), Self::SendError>;
    fn finish_stream(&mut self, stream_id: u64) -> Result<(), Self::SendError>;
}

pub fn pump_stream_event<T: StreamTransport>(
    conn: &mut Conn,
    transport: &mut T,
    stream_id: u64,
) -> Result<(), ConnError> {
    let mut bytes = Vec::new();
    transport.recv_stream(stream_id, &mut bytes);
    let fin = transport.recv_stream_finished(stream_id);
    if bytes.is_empty() && !fin {
        return Ok(());
    }
    conn.ingest_stream(StreamId::new(stream_id), &bytes, fin)
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
