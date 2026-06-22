use crate::conn::{Conn, ConnError};
use crate::stream::StreamId;

pub trait StreamTransport {
    fn recv_stream(&mut self, stream_id: u64, out: &mut Vec<u8>) -> usize;
    fn recv_stream_finished(&self, stream_id: u64) -> bool;
    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]);
    fn finish_stream(&mut self, stream_id: u64);
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

pub fn pump_writes<T: StreamTransport>(conn: &mut Conn, transport: &mut T) {
    while let Some(write) = conn.poll_write() {
        transport.send_stream(write.stream_id.0, &write.bytes);
        if write.fin {
            transport.finish_stream(write.stream_id.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use sark_core::http::Field;

    use super::*;
    use crate::conn::Event;

    #[derive(Default)]
    struct FakeTransport {
        recv: BTreeMap<u64, Vec<u8>>,
        recv_fin: BTreeSet<u64>,
        sent: BTreeMap<u64, Vec<u8>>,
        sent_fin: BTreeSet<u64>,
    }

    impl StreamTransport for FakeTransport {
        fn recv_stream(&mut self, stream_id: u64, out: &mut Vec<u8>) -> usize {
            let bytes = self.recv.remove(&stream_id).unwrap_or_default();
            let n = bytes.len();
            out.extend_from_slice(&bytes);
            n
        }

        fn recv_stream_finished(&self, stream_id: u64) -> bool {
            self.recv_fin.contains(&stream_id)
        }

        fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) {
            self.sent
                .entry(stream_id)
                .or_default()
                .extend_from_slice(bytes);
        }

        fn finish_stream(&mut self, stream_id: u64) {
            self.sent_fin.insert(stream_id);
        }
    }

    #[test]
    fn pumps_h3_writes_into_transport_and_events_back() {
        let mut client_h3 = Conn::new();
        client_h3
            .send_headers(StreamId::new(0), [Field::new(b":method", b"GET")], false)
            .unwrap();
        client_h3.send_data(StreamId::new(0), b"abc", true).unwrap();

        let mut wire = FakeTransport::default();
        pump_writes(&mut client_h3, &mut wire);
        assert!(wire.sent_fin.contains(&0));

        let mut server_h3 = Conn::new();
        wire.recv.insert(0, wire.sent.remove(&0).unwrap());
        wire.recv_fin.insert(0);
        pump_stream_event(&mut server_h3, &mut wire, 0).unwrap();

        assert!(matches!(
            server_h3.poll_event(),
            Some(Event::Headers { .. })
        ));
        assert_eq!(
            server_h3.poll_event(),
            Some(Event::Data {
                stream_id: StreamId::new(0),
                data: b"abc".to_vec()
            })
        );
        assert_eq!(
            server_h3.poll_event(),
            Some(Event::Finished {
                stream_id: StreamId::new(0)
            })
        );
    }
}
