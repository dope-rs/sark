use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{CLIENT_PREFACE, event::Event, machine::Connection},
    frame::{
        Ack, Block,
        layout::Headers,
        payload::{Continuation, Ping},
    },
    hpack::encode::Encoder,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn reads_split_headers() {
    let encoder = Encoder::try_new(4096);
    let mut encoder = encoder.expect("test operation must succeed");
    let mut block = Vec::new();
    encoder
        .try_encode(
            [
                Entry::new(b":method", b"GET"),
                Entry::new(b":scheme", b"http"),
                Entry::new(b":authority", b"x"),
                Entry::new(b":path", b"/"),
                Entry::new(b"x-fragments", b"retained"),
            ],
            &mut block,
        )
        .expect("HPACK table");
    assert!(block.len() > 2);
    let mut wire = CLIENT_PREFACE.to_vec();
    wire.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
    let first = Headers::new(
        Id::FIRST_CLIENT,
        Boundary::End,
        Block::More,
        None,
        &block[..1],
    );
    let first = first.expect("test owner must be present");
    (first.encode(&mut wire))
        .as_ref()
        .expect("test operation must succeed");
    let last = block.len() - 1;
    for (part, end) in [(&block[1..last], Block::More), (&block[last..], Block::End)] {
        let next = Continuation::new(Id::FIRST_CLIENT, end, part);
        let next = next.expect("test owner must be present");
        (next.encode(&mut wire))
            .as_ref()
            .expect("test operation must succeed");
    }
    for width in [1, 2, 3, 5, 8, 9, 13, 32, 128] {
        let server = Connection::<ServerRole>::try_new();
        let mut server = server.expect("test operation must succeed");
        for part in wire.chunks(width) {
            assert!(
                server.ingress().ingest(part).is_ok(),
                "test operation must succeed"
            );
        }
        let mut seen = 0;
        while let Some(event) = server.ingress().poll_event() {
            if let Event::Headers { head, .. } = event {
                let (headers, _, _) = head.into_parts();
                assert_eq!(headers.get(b"x-fragments"), Some(b"retained".as_slice()));
                seen += 1;
            }
        }
        assert_eq!(seen, 1, "input chunk size {width}");
    }
}

#[test]
fn skips_split_extensions() {
    let mut wire = CLIENT_PREFACE.to_vec();
    wire.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
    wire.extend_from_slice(&[0, 0, 4, 254, 0, 0, 0, 0, 0, 1, 2, 3, 4]);
    (Ping {
        ack: Ack::Required,
        opaque: [7; 8],
    }
    .encode(&mut wire))
    .as_ref()
    .expect("test operation must succeed");
    for width in 1..=17 {
        let server = Connection::<ServerRole>::try_new();
        let mut server = server.expect("test operation must succeed");
        for part in wire.chunks(width) {
            assert!(
                server.ingress().ingest(part).is_ok(),
                "test operation must succeed"
            );
        }
        let mut seen = 0;
        while let Some(event) = server.ingress().poll_event() {
            if let Event::Ping { opaque, .. } = event {
                assert_eq!(opaque, [7; 8]);
                seen += 1;
            }
        }
        assert_eq!(seen, 1, "input chunk size {width}");
    }
}
