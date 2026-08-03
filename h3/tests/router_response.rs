use std::collections::{BTreeMap, BTreeSet};

use sark::dispatch::Decode;
use sark_core::http::{Field, PlannedHead};
use sark_h3::dope::H3Encoder;
use sark_h3::qpack::Encoder;
use sark_h3::{
    Conn, Event, Frame, Role, StreamId, StreamTransport, TYPE_HEADERS, pump_stream_event,
    pump_writes,
};

fn routed_conn<R: Decode>(_: &R) -> Conn<R::Plan> {
    Conn::from_config_with_plan(Role::Server, sark_h3::ValidatedConfig::default())
}

#[sark_gen::response(raw)]
#[header("content-type", "text/plain")]
struct Reply {
    status: http::StatusCode,
    body: &'static [u8],
}

#[sark_gen::request]
struct JsonReq {
    #[header("x-name", default = "none")]
    name: sark_core::http::Bytes<sark_core::http::Retained>,
}

#[sark_gen::handler]
fn json_h(req: JsonReq, _state: &sark::EmptyState) -> Reply {
    Reply {
        status: http::StatusCode::OK,
        body: if req.name.as_slice() == b"alice" {
            b"hello-h3"
        } else {
            b"missing-header"
        },
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

    fn recv_stream(&mut self, stream_id: u64) -> Option<Vec<u8>> {
        self.recv
            .remove(&stream_id)
            .filter(|bytes| !bytes.is_empty())
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
    let timer = sark::Timer::new();
    let app = H3App::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState::REF,
        &timer,
        sark::app::Config { task_capacity: 0 },
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
                Field::new(b"x-ignored", b"discard-me"),
                Field::new(b"x-name", b"alice"),
            ],
            true,
        )
        .unwrap();

    let mut wire = FakeTransport::default();
    pump_writes(&mut client, &mut wire).unwrap();

    let mut server = routed_conn(&app);
    wire.recv.insert(0, wire.sent.remove(&0).unwrap());
    wire.recv_fin.insert(0);
    pump_stream_event(&mut server, &mut wire, 0).unwrap();

    let mut pending = None;
    let mut routed = false;
    while let Some(ev) = server.poll_event() {
        match ev {
            Event::Headers {
                stream_id,
                fields,
                selection,
                ..
            } => {
                pending = Some((stream_id, PlannedHead::new(fields, selection)));
            }
            Event::Finished { stream_id } => {
                let (_sid, head) = pending.take().expect("headers before finish");
                let prepared = app.prepare_planned_head(head).expect("known route");
                let mut enc = H3Encoder::new(&mut server, stream_id);
                let out = app.dispatch_prepared(prepared, &[][..], &mut enc);
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
                assert!(
                    fields
                        .iter()
                        .any(|f| f.name == b"content-type" && f.value == b"text/plain")
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

#[test]
fn planned_huffman_values_decode_directly_into_their_final_sink() {
    let timer = sark::Timer::new();
    let app = H3App::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState::REF,
        &timer,
        sark::app::Config { task_capacity: 0 },
    );
    let mut qpack = Encoder::new();
    qpack.set_huffman(true);
    let mut block = Vec::new();
    qpack.encode(
        [
            Field::new(b":path", b"/json"),
            Field::new(b":scheme", b"https"),
            Field::new(b":authority", b"x"),
            Field::new(b":method", b"GET"),
            Field::new(b"x-ignored", b"discarded huffman value"),
            Field::new(b"x-name", b"alice"),
        ],
        &mut block,
    );
    let mut wire = Vec::new();
    Frame::encode(TYPE_HEADERS, &block, &mut wire).unwrap();

    let mut server = routed_conn(&app);
    server
        .ingest_stream_owned(StreamId::new(0), wire, true)
        .unwrap();
    let mut planned = None;
    while let Some(event) = server.poll_event() {
        if let Event::Headers {
            fields, selection, ..
        } = event
        {
            planned = Some(PlannedHead::new(fields, selection));
        }
    }
    let prepared = app
        .prepare_planned_head(planned.expect("decoded request head"))
        .expect("known route");
    let mut response = H3Encoder::new(&mut server, StreamId::new(0));
    assert_eq!(
        app.dispatch_prepared(prepared, &[][..], &mut response),
        sark::dispatch::Decoded::Emitted,
    );
    assert!(response.ok());

    let mut sent = FakeTransport::default();
    pump_writes(&mut server, &mut sent).unwrap();
    assert!(
        sent.sent
            .get(&0)
            .is_some_and(|bytes| bytes.windows(b"hello-h3".len()).any(|w| w == b"hello-h3"))
    );
}
