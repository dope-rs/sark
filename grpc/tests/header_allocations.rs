use std::{hint, mem};

use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Status,
    metadata::Metadata,
    protocol::{Block, request, response, trailer},
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{event, machine::Connection},
};
use sark_http::field::metadata::{map::Owned, packed::prefix::Prefix};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plans_borrow_owners() {
    let mut metadata = Metadata::new();
    metadata
        .push(b"x-owner", b"retained")
        .expect("valid owner metadata");
    let status = Status::from_code(Code::InvalidArgument);

    let ((request, response, trailers), (allocs, bytes)) = Tracker::usage(|| {
        (
            request::Fields::new(b"/svc/Method", Some(b"localhost"), &metadata)
                .expect("valid request fields"),
            response::Fields::new(&metadata),
            trailer::Fields::new(&status, &metadata).expect("valid trailer fields"),
        )
    });

    assert_eq!((allocs, bytes), (0, 0));
    assert_eq!(
        mem::size_of_val(&request),
        mem::size_of::<(request::Scheme, &[u8], Option<&[u8]>, &Metadata)>()
    );
    assert_eq!(mem::size_of_val(&response), mem::size_of::<usize>());
    assert_eq!(mem::size_of_val(&trailers), 3 * mem::size_of::<usize>());
}

#[test]
fn request_reuse_is_zero() {
    let mut conn = Connection::<ClientRole>::try_new().expect("valid client connection");
    conn.transfer().drain(usize::MAX);
    let mut metadata = Metadata::new();
    metadata
        .push(b"x-owner", b"retained")
        .expect("valid owner metadata");

    conn.streams()
        .headers()
        .request(
            request::Fields::new(b"/svc/Method", Some(b"localhost"), &metadata)
                .expect("valid request fields"),
            Boundary::End,
        )
        .expect("first request admission");
    conn.transfer().drain(usize::MAX);

    let (allocs, bytes) = Tracker::during(|| {
        conn.streams()
            .headers()
            .request(
                request::Fields::new(b"/svc/Method", Some(b"localhost"), &metadata)
                    .expect("valid request fields"),
                Boundary::End,
            )
            .expect("reused request admission");
    });
    assert_eq!((allocs, bytes), (0, 0));
}

#[test]
fn response_reuse_is_zero() {
    let mut client = Connection::<ClientRole>::try_new().expect("valid client connection");
    client
        .streams()
        .headers()
        .request(
            request::Fields::new(b"/svc/Method", Some(b"localhost"), &Metadata::new())
                .expect("valid request fields"),
            Boundary::End,
        )
        .expect("request admission");
    let wire = client.output().first().to_vec();

    let mut server = Connection::<ServerRole>::try_new().expect("valid server connection");
    server.ingress().ingest(&wire).expect("valid request wire");
    let stream = loop {
        let event = server.ingress().poll_event().expect("request event");
        if let event::Event::Headers {
            stream_id: stream, ..
        } = event
        {
            break stream;
        }
    };
    server.transfer().drain(usize::MAX);

    let mut metadata = Metadata::new();
    metadata
        .push(b"x-owner", b"retained")
        .expect("valid owner metadata");
    let status = Status::from_code(Code::InvalidArgument);
    let trailers = trailer::Fields::new(&status, &metadata).expect("valid trailer fields");

    let (allocs, bytes) = Tracker::during(|| {
        server
            .streams()
            .headers()
            .respond_key(stream, response::Fields::new(&metadata), Boundary::More)
            .expect("response admission");
        server
            .streams()
            .headers()
            .trail_key(stream, trailers)
            .expect("trailer admission");
    });
    assert_eq!((allocs, bytes), (0, 0));
}

#[test]
fn blocks_pack_once() {
    let metadata = Metadata::new();

    let (request, (allocs, _)) = Tracker::usage(|| {
        Block::request(b"/svc/Method", Some(b"localhost"), &metadata).expect("valid request block")
    });
    assert_eq!(allocs, 1);
    assert_eq!(request.iter().count(), 6);

    let (response, (allocs, _)) =
        Tracker::usage(|| Block::response(&metadata).expect("valid response block"));
    assert_eq!(allocs, 1);
    assert_eq!(response.iter().count(), 2);

    let (trailers, (allocs, _)) =
        Tracker::usage(|| Block::trailers(&Status::ok(), &metadata).expect("valid trailer block"));
    assert_eq!(allocs, 1);
    assert_eq!(trailers.iter().count(), 1);
}

#[test]
fn decode_packs_once() {
    let metadata = Metadata::new();
    let fields =
        Block::request(b"/svc/Method", Some(b"localhost"), &metadata).expect("valid request block");
    let (head, (allocs, _)) = Tracker::usage(|| request::Head::from_fields(&fields));
    let head = head.expect("decoded request");
    assert_eq!(allocs, 1);
    assert_eq!(head.path(), b"/svc/Method");
    assert_eq!(head.authority(), Some(b"localhost".as_slice()));

    let fields = vec![
        Owned::try_new(b"grpc-status", b"3").expect("status field"),
        Owned::try_new(b"grpc-message", b"bad%20%E2%98%83%0A").expect("message field"),
    ];
    let (status, (allocs, _)) = Tracker::usage(|| Status::from_fields(&fields));
    let status = status.expect("valid status fields decode");
    assert_eq!(allocs, 1);
    assert!((status.code()) == (Code::InvalidArgument));
    assert_eq!(status.message(), Some("bad \u{2603}\n"));
    let ((first, second), (allocs, bytes)) = Tracker::usage(|| {
        (
            hint::black_box(status.message()).map(str::as_ptr),
            hint::black_box(status.message()).map(str::as_ptr),
        )
    });
    assert_eq!((allocs, bytes), (0, 0));
    assert_eq!(
        first, second,
        "the retained message borrow must stay stable"
    );
}

#[test]
fn large_meta_packs_once() {
    let mut metadata = Metadata::new();
    metadata
        .push(b"x-large", &[b'a'; 1_024])
        .expect("valid large metadata");
    let capacity = Prefix::LEN
        + b":status".len()
        + b"200".len()
        + Prefix::LEN
        + b"content-type".len()
        + b"application/grpc+proto".len()
        + Prefix::LEN
        + b"x-large".len()
        + 1_024;

    let (headers, (allocs, bytes)) =
        Tracker::usage(|| Block::response(&metadata).expect("valid response block"));

    assert_eq!(allocs, 1);
    assert_eq!(bytes, capacity);
    assert_eq!(headers.iter().count(), 3);
    assert_eq!(
        headers
            .iter()
            .find(|field| field.name == b"x-large")
            .expect("large metadata field")
            .value,
        &[b'a'; 1_024]
    );
}

#[test]
fn metadata_packs_once() {
    assert!(matches!(
        Metadata::try_with(usize::MAX),
        Err(sark_grpc::metadata::Error::Pack(
            sark_http::field::metadata::map::Error::Reserve(o3::alloc::Error::Overflow)
        ))
    ));
    const CAPACITY: usize =
        2 * Prefix::LEN + b"x-request-id".len() + b"42".len() + b"trace-bin".len() + 4;

    let (metadata, (allocs, _)) = Tracker::usage(|| {
        let mut metadata = Metadata::try_with(CAPACITY).expect("metadata capacity");
        metadata
            .push(b"x-request-id", b"42")
            .expect("valid request metadata");
        metadata
            .push(b"trace-bin", &[0, 1, 2, 255])
            .expect("valid binary metadata");
        metadata
    });

    assert_eq!(allocs, 1);
    assert_eq!(mem::size_of::<Metadata>(), mem::size_of::<Vec<u8>>());
    assert_eq!(
        metadata.values(b"x-request-id").next(),
        Some(b"42".as_slice())
    );
    assert_eq!(metadata.len(), 2);
}
