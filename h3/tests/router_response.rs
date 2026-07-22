use std::collections::{BTreeMap, BTreeSet};

use sark::dispatch::Decode;
use sark_core::http::{Field, OwnedField};
use sark_h3::dope::H3Encoder;
use sark_h3::{Conn, Event, Role, StreamId, StreamTransport, pump_stream_event, pump_writes};

#[sark_gen::response(raw)]
struct Reply {
    status: http::StatusCode,
    body: &'static [u8],
}

#[sark_gen::request]
struct JsonReq {}

#[sark_gen::handler]
fn json_h(_req: JsonReq, _state: &sark::EmptyState) -> Reply {
    Reply {
        status: http::StatusCode::OK,
        body: b"hello-h3",
    }
}

sark_gen::define_route! {
    H3App: sark::EmptyState => {
        GET "/json" => json_h,
    }
}

#[derive(Default)]
struct FakeTransport {
    recv: BTreeMap<u64, Vec<u8>>,
    recv_fin: BTreeSet<u64>,
    sent: BTreeMap<u64, Vec<u8>>,
    sent_fin: BTreeSet<u64>,
}

impl StreamTransport for FakeTransport {
    type SendError = std::convert::Infallible;

    fn recv_stream(&mut self, stream_id: u64, out: &mut Vec<u8>) -> usize {
        let bytes = self.recv.remove(&stream_id).unwrap_or_default();
        let n = bytes.len();
        out.extend_from_slice(&bytes);
        n
    }

    fn recv_stream_finished(&self, stream_id: u64) -> bool {
        self.recv_fin.contains(&stream_id)
    }

    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) -> Result<(), Self::SendError> {
        self.sent
            .entry(stream_id)
            .or_default()
            .extend_from_slice(bytes);
        Ok(())
    }

    fn finish_stream(&mut self, stream_id: u64) -> Result<(), Self::SendError> {
        self.sent_fin.insert(stream_id);
        Ok(())
    }
}

#[test]
fn h3_request_routes_and_responds() {
    let app = H3App::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState,
        sark::app::Config {
            timer_capacity: 0,
            task_capacity: 0,
        },
    );

    let mut client = Conn::with_role(Role::Client);
    client
        .send_headers(
            StreamId::new(0),
            [
                Field::new(b":method", b"GET"),
                Field::new(b":scheme", b"https"),
                Field::new(b":authority", b"x"),
                Field::new(b":path", b"/json"),
            ],
            true,
        )
        .unwrap();

    let mut wire = FakeTransport::default();
    pump_writes(&mut client, &mut wire).unwrap();

    let mut server = Conn::with_role(Role::Server);
    wire.recv.insert(0, wire.sent.remove(&0).unwrap());
    wire.recv_fin.insert(0);
    pump_stream_event(&mut server, &mut wire, 0).unwrap();

    let mut pending: Option<(StreamId, Vec<OwnedField>)> = None;
    let mut routed = false;
    while let Some(ev) = server.poll_event() {
        match ev {
            Event::Headers {
                stream_id, fields, ..
            } => pending = Some((stream_id, fields)),
            Event::Finished { stream_id } => {
                let (_sid, fields) = pending.take().expect("headers before finish");
                let mut head = Vec::new();
                let mut pairs: Vec<(&[u8], std::ops::Range<usize>)> = Vec::new();
                for f in &fields {
                    if f.name.first() == Some(&b':') {
                        continue;
                    }
                    let start = head.len();
                    head.extend_from_slice(&f.value);
                    pairs.push((f.name.as_slice(), start..head.len()));
                }
                let mut enc = H3Encoder::new(&mut server, stream_id);
                let out =
                    app.dispatch_decoded(http::Method::GET, b"/json", &pairs, &head, &[], &mut enc);
                assert_eq!(out, sark::dispatch::Decoded::Emitted);
                assert!(enc.ok());
                routed = true;
            }
            _ => {}
        }
    }
    assert!(routed);

    let mut back = FakeTransport::default();
    pump_writes(&mut server, &mut back).unwrap();
    back.recv.insert(0, back.sent.remove(&0).unwrap());
    if back.sent_fin.contains(&0) {
        back.recv_fin.insert(0);
    }
    pump_stream_event(&mut client, &mut back, 0).unwrap();

    let mut status_ok = false;
    let mut body_ok = false;
    while let Some(ev) = client.poll_event() {
        match ev {
            Event::Headers { fields, .. } => {
                assert!(
                    fields
                        .iter()
                        .any(|f| f.name == b":status" && f.value == b"200")
                );
                status_ok = true;
            }
            Event::Data { data, .. } => {
                assert_eq!(data, b"hello-h3");
                body_ok = true;
            }
            _ => {}
        }
    }
    assert!(status_ok && body_ok);
}
