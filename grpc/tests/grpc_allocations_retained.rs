use core::iter;

use sark_grpc::{
    Codec as _,
    client::{
        call::unary::Session as _,
        io::{Session as _, ingress::Session as _},
    },
};

pub mod common;

use std::cell::Cell;

use dope_test::alloc::Tracker;
use prost::Message;
use sark_core::manifold::client::io::Output;
use sark_grpc::{
    Code,
    client::{
        call::{Admission, bidi},
        config,
        config::Limits,
        session::Session,
    },
    encode::Prost,
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{client, conn::event::Event, frame::numbers::ErrorCode};

use crate::common::{
    grpc::pair::Pair,
    messages::{Packet, Probe},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn source_matches_protobuf() {
    use o3::buffer::{pool, storage};

    for len in [0, 1, 127, 128, 16383, 16384, 2097151, 2097152] {
        let packet = Packet {
            data: vec![0xa5; len],
        };
        let expected = packet.encode_to_vec();
        let mut codec = Prost::<Packet>::new();
        let mut owner = storage::Shared::new();
        let (result, (count, _)) = Tracker::usage(|| codec.encode(&packet, &mut owner));
        result.expect("protobuf must encode into shared storage");
        assert_eq!(owner.as_slice(), expected);
        assert_eq!(count, usize::from(len != 0));

        let lease = pool::Span::try_new(expected.len().max(1));
        let mut lease = lease.expect("test operation must succeed");
        lease
            .try_append(&[0xff])
            .expect("prefix fits the lease")
            .commit();
        let (result, (count, _)) = Tracker::usage(|| codec.encode(&packet, lease));
        let bytes = result.expect("test operation must succeed");
        assert_eq!(bytes.as_ref(), expected);
        assert_eq!(count, 0);
    }
}

#[test]
fn rejects_short_lease() {
    use o3::buffer::pool;

    let packet = Packet { data: vec![1; 128] };
    let created = pool::Pool::try_new(1, 130);
    let pool = created.expect("test operation must succeed");
    let acquired = pool.acquire();
    let lease = acquired.expect("fresh pool must own its lease");
    let (result, (count, _)) = Tracker::usage(|| Prost::<Packet>::new().encode(&packet, lease));
    let status = result.expect_err("short lease must reject the message");
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(count, 0);
    assert_eq!(pool.available(), 1);
}

#[test]
fn encode_reuses_owner() {
    Pair::try_flow()
        .expect("flow fixture")
        .try_run(|mut session, mut server| {
            let mut stream =
                bidi::Session::open(&mut session, b"/svc/Method", None, &Metadata::new())
                    .expect("the first stream fits the call limit");
            let packet = Packet {
                data: vec![b'x'; 4096],
            };
            let mut codec = Prost::<Packet>::new();

            let (result, (count, _)) = Tracker::usage(|| {
                bidi::Session::try_send(&mut session, &mut stream, &mut codec, &packet)
                    .into_result()
            });

            result.expect("the admitted request fits its owner");
            assert_eq!(count, 0, "request encoding must reuse its admitted owner");
            assert!(
                server.ingress().ingest(session.outbound()).is_ok(),
                "the allocation-free encoding is a valid H2 request"
            );
        })
        .expect("gRPC pair scope");
}

#[test]
fn source_recycles_owner() {
    let limits = Limits::default()
        .try_bytes(8)
        .expect("eight bytes fit the message limit")
        .try_output(config::Output::try_new(1).expect("one output slot is nonzero"))
        .expect("one output slot fits the limit");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut session| {
            let mut stream = bidi::Session::open(
                &mut session,
                b"/svc/Method",
                None,
                &Metadata::new(),
            )
                .expect("the first stream fits the call limit");
            let mut codec = Prost::<Probe, Packet>::new();
            let mut payload = Probe { plans: Cell::new(0), bytes: vec![0; 9] };

            let rejected =
                bidi::Session::try_send(&mut session, &mut stream, &mut codec, &payload);
            assert!(matches!(rejected, Admission::Rejected(ref status) if status.code() == Code::ResourceExhausted));
            assert_eq!(payload.plans.get(), 1);
            assert_eq!(session.owner_cap(), 1);

            payload.bytes.truncate(8);
            assert!(matches!(
                bidi::Session::try_send(&mut session, &mut stream, &mut codec, &payload),
                Admission::Ready(())
            ));
            assert_eq!(payload.plans.get(), 2);
        },
    )
    .expect("the scoped client session closes cleanly");
}

#[test]
fn cancel_recycles_slot() {
    Pair::try_flow()
        .expect("flow fixture")
        .try_run(|mut session, mut server| {
            let mut stream =
                bidi::Session::open(&mut session, b"/svc/Method", None, &Metadata::new())
                    .expect("the first stream fits the call limit");
            let payload = Probe {
                plans: Cell::new(0),
                bytes: vec![b'x'; 4096],
            };
            let mut codec = Prost::<Probe, Packet>::new();

            bidi::Session::try_send(&mut session, &mut stream, &mut codec, &payload)
                .into_result()
                .expect("the admitted request fits its owner");
            assert_eq!(payload.plans.get(), 1);
            assert_eq!(session.owner_cap(), 0);
            assert!(matches!(
                bidi::Session::try_send(&mut session, &mut stream, &mut codec, &payload),
                Admission::Pending
            ));
            assert_eq!(
                payload.plans.get(),
                1,
                "transient admission must not invoke the codec"
            );
            let outcome = bidi::Session::try_send(&mut session, &mut stream, &mut codec, &payload)
                .into_result();
            let status = outcome.expect_err("a full owner pool must reject another request");
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert_eq!(
                payload.plans.get(),
                1,
                "full owner pool must reject before encode"
            );

            let request = session.outbound().to_vec();
            session
                .drain(request.len())
                .expect("released egress resumes queued calls");
            assert!(
                server.ingress().ingest(&request).is_ok(),
                "the encoded request is valid H2"
            );
            let id = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id),
                    _ => None,
                })
                .expect("request stream");
            assert!(
                server.streams().try_cancel(id, ErrorCode::Cancel).is_ok(),
                "the observed request stream is active"
            );
            let reset = server.output().first().to_vec();
            session.ingest(&reset).expect("the peer reset is valid H2");
            let outcome = bidi::Session::try_poll(&mut session, &stream);
            let status = outcome
                .err()
                .expect("a reset stream must no longer be active");
            assert_eq!(status.code(), Code::Cancelled);
            assert_eq!(session.owner_cap(), 1);

            let mut next =
                bidi::Session::open(&mut session, b"/svc/Method", None, &Metadata::new())
                    .expect("the cancelled stream returned its call slot");
            bidi::Session::try_send(&mut session, &mut next, &mut codec, &payload)
                .into_result()
                .expect("the cancelled request returned its owner");
            assert_eq!(
                payload.plans.get(),
                2,
                "cancel must return the sole request owner"
            );
        })
        .expect("gRPC pair scope");
}

#[test]
fn prost_uses_h2_owner() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut session| {
            session
                .drain(usize::MAX)
                .expect("released egress resumes queued calls");
            let packet = Packet {
                data: vec![b'x'; 4096],
            };
            let expected = packet.encode_to_vec();
            let mut codec = Prost::<Packet>::new();

            let call = session
                .try_start(b"/svc/Method", None, &Metadata::new(), &mut codec, &packet)
                .into_result()
                .expect("the first unary call fits the limit");

            let mut prefix = vec![0; 64 * 1024];
            let result = session.take_write(&mut prefix);
            let (write, driven) =
                result.expect("the admitted request must retain its egress owner");
            driven.expect("released egress resumes queued calls");
            assert!(
                matches!(write, Output::Split { .. }),
                "typed gRPC payload must retain its owner"
            );
            if let Output::Split {
                prefix_len: len,
                body,
            } = write
            {
                assert!(len > 5, "request headers and gRPC prefix stay buffered");
                assert_eq!(body.as_slice(), expected);
            }
            session.cancel(call);
        },
    )
    .expect("the scoped client session closes cleanly");
}

#[test]
fn empty_body_is_free() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut session| {
            session
                .drain(usize::MAX)
                .expect("released egress resumes queued calls");
            let packet = Packet { data: Vec::new() };
            let mut codec = Prost::<Packet>::new();
            let call = session
                .try_start(b"/svc/Method", None, &Metadata::new(), &mut codec, &packet)
                .into_result()
                .expect("the first unary call fits the limit");

            let mut prefix = [0; 64 * 1024];
            let (result, usage) = Tracker::usage(|| session.take_write(&mut prefix));
            let (write, driven) = result.expect("the empty protobuf request retains its headers");
            driven.expect("released egress resumes queued calls");
            assert!(matches!(write, Output::Buffered(_)));
            assert_eq!(usage, (0, 0));
            session.cancel(call);
        },
    )
    .expect("the scoped client session closes cleanly");
}
