pub mod common;

use common::{
    hpack::{Fields, GET},
    protocol::{Session, session::Frame},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{event::Event, machine::Connection},
    frame::Block,
};
use sark_http::field::Indexing;

#[test]
fn forwards_never_indexed() {
    let mut source = Connection::<ServerRole>::try_new().expect("source connection");
    Session::new(&mut source)
        .try_prime()
        .expect("source preface");
    let block = Fields::new(&GET).encode_never();
    let wire = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("request frame");
    source
        .ingress()
        .ingest(&wire)
        .expect("decode never-indexed fields");
    let held = source
        .ingress()
        .poll_event()
        .and_then(|event| match event {
            Event::Headers { head, .. } => Some(head),
            _ => None,
        })
        .expect("retained request fields")
        .try_request()
        .ok()
        .expect("validated request fields");
    drop(source);

    let mut forward = Connection::<ClientRole>::try_new().expect("forwarding connection");
    Session::new(&mut forward)
        .try_prime()
        .expect("forwarding preface");
    forward
        .streams()
        .headers()
        .request(&held, Boundary::End)
        .expect("forward retained fields");
    let mut target = Connection::<ServerRole>::try_new().expect("target connection");
    Session::new(&mut target)
        .try_prime()
        .expect("target preface");
    target
        .ingress()
        .ingest(forward.output().first())
        .expect("decode forwarded fields");
    let fields = target
        .ingress()
        .poll_event()
        .and_then(|event| match event {
            Event::Headers { head, .. } => Some(head),
            _ => None,
        })
        .expect("forwarded request fields");
    assert!(
        fields
            .fields()
            .iter()
            .all(|field| field.indexing == Indexing::Never)
    );
}
