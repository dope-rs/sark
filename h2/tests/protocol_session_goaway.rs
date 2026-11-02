pub mod common;

use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        session::{Frame, Wire},
    },
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole, conn::machine::Connection, frame, frame::numbers::ErrorCode, stream::Id,
};

#[test]
fn post_goaway_refuses_head() {
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let id = Id::new(3).expect("fixture domain");
    for split in [block.len(), block.len() / 2] {
        let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        conn.control().goaway(ErrorCode::NoError);
        conn.transfer().drain(usize::MAX);

        let chain = if split == block.len() {
            frame::Block::End
        } else {
            frame::Block::More
        };
        conn.ingress()
            .ingest(
                &Frame::new(3, &block[..split])
                    .try_headers(Boundary::More, chain)
                    .expect("protocol fixture"),
            )
            .expect("refused HEADERS");
        if split != block.len() {
            conn.ingress()
                .ingest(
                    &Frame::new(3, &block[split..])
                        .try_continue(frame::Block::End)
                        .expect("protocol fixture"),
                )
                .expect("refused CONTINUATION");
        }

        assert!(!conn.status().has_stream(id));
        assert!(conn.ingress().poll_event().is_none());
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol fixture"),
            Some((id, ErrorCode::RefusedStream))
        );
        conn.transfer().drain(usize::MAX);

        let window = conn.status().recv_window().value();
        conn.ingress()
            .ingest(
                &Frame::new(3, b"x")
                    .try_data(Boundary::More)
                    .expect("protocol fixture"),
            )
            .expect("refused stream remains closed");
        assert_eq!(conn.status().recv_window().value(), window - 1);
        conn.transfer().drain(usize::MAX);

        conn.ingress()
            .ingest(
                &Frame::new(3, &block)
                    .try_headers(Boundary::More, frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("repeated HEADERS remain closed");
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol fixture"),
            Some((id, ErrorCode::StreamClosed))
        );
        assert!(conn.ingress().poll_event().is_none());

        conn.transfer().drain(usize::MAX);
        conn.ingress()
            .ingest(
                &Frame::new(1, &block)
                    .try_headers(Boundary::More, frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("implicitly closed stream keeps HPACK state");
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol fixture"),
            Some((Id::FIRST_CLIENT, ErrorCode::StreamClosed))
        );
        assert!(conn.ingress().poll_event().is_none());
    }
}
