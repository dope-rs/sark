use std::{iter, num::NonZero, pin::Pin};

use o3::{
    base::cell::region,
    buffer::{pool::Pool, resident, segments, storage::Shared},
    depot,
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        Error,
        config::{Config, Validated, capacity},
        event::Event,
        machine::Connection,
    },
    frame::Ack,
};
use sark_http::field::metadata::section::rules;
pub mod common;

use common::{
    capacity::{Upload, try_start, try_wire},
    retain::{Frames, GOAWAY, Ingress, fragments},
};

#[test]
fn slices_owner() {
    let (wire, offset) = try_wire().expect("request fixture");
    let start = offset - 9;
    let pool = Pool::try_new(1, wire.len() - start).expect("receive pool");
    let mut lease = pool.acquire().expect("receive lease");
    lease
        .try_append(&wire[start..])
        .expect("wire fits lease")
        .commit();
    let pooled = lease.freeze();
    let expected = pooled.as_ref().as_ptr() as usize + 9;
    let retained = pooled.into_shared();
    let mut server = Connection::<ServerRole>::try_new().expect("server allocation");
    server
        .ingress()
        .ingest(&wire[..start])
        .expect("request head");

    server
        .ingress()
        .ingest_owned(retained)
        .expect("retained ingress");

    let data = iter::from_fn(|| server.ingress().poll_event())
        .find_map(|event| match event {
            Event::Data { data, .. } => Some(data),
            _ => None,
        })
        .expect("data event");
    assert_eq!(
        data.contiguous().expect("contiguous payload").as_ptr() as usize,
        expected
    );
    assert_eq!(data.contiguous(), Some(b"retained-payload".as_slice()));
}

/// Owner reuse under retained HTTP/2 DATA; TLS staging has its own contract tests.
#[test]
fn reuses_retained_owners() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(65 * 512, token.proof())
            .expect("body budget");
        let mut client = Connection::<ClientRole>::try_new().expect("client");
        let stream = try_start(&mut client).expect("request");
        let plan = Validated::new(Config {
            payload_capacity: capacity::Payload::new(66, 1).expect("retained bodies"),
            ..Config::default()
        })
        .expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        server.ingress().try_bind(budget).expect("bind authority");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("request head");
        while server.ingress().poll_event().is_some() {}
        client
            .ingress()
            .ingest(server.output().first())
            .expect("peer settings");
        while client.ingress().poll_event().is_some() {}
        client.transfer().drain(usize::MAX);
        server.transfer().drain(usize::MAX);

        let pool = Pool::try_new(65, 16_384).expect("record owners");
        let opaque = *b"retain65";
        let mut bodies = Vec::new();
        for index in 0..65 {
            let record = stream.record((index == 64).then_some(opaque));
            let mut lease = pool.acquire().expect("previous record owner was released");
            lease.try_append(&record).expect("record fits").commit();
            server
                .ingress()
                .ingest_owned(resident::Shared::from(lease.freeze().into_shared()))
                .expect("record ingress");
            while let Some(event) = server.ingress().poll_event() {
                if let Event::Data { data, boundary, .. } = event {
                    assert_eq!(boundary, Boundary::More);
                    assert_eq!(data.contiguous(), Some([b'x'; 512].as_slice()));
                    bodies.push(data);
                }
            }
        }
        assert_eq!(bodies.len(), 65);
        assert_eq!(budget.resident(), 65 * 512);
        assert_eq!(pool.available(), 65);
        client
            .ingress()
            .ingest(server.output().first())
            .expect("PING reply");
        assert!(iter::from_fn(|| client.ingress().poll_event()).any(|event| {
            matches!(event, Event::Ping { ack: Ack::Complete, opaque: actual } if actual == opaque)
        }));

        let mut end = [0, 0, 0, 0, 1, 0, 0, 0, 0];
        end[5..].copy_from_slice(&stream.into_u32().to_be_bytes());
        let mut lease = pool.acquire().expect("END_STREAM owner");
        lease.try_append(&end).expect("frame fits").commit();
        server
            .ingress()
            .ingest_owned(resident::Shared::from(lease.freeze().into_shared()))
            .expect("END_STREAM ingress");
        assert!(
            matches!(server.ingress().poll_event(), Some(Event::Data { data, boundary: Boundary::End, .. }) if data.is_empty())
        );
        assert_eq!(pool.available(), 65);
        drop(bodies);
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn compacts_goaway_owner() {
    let (_, mut server, _) = Upload::with_payload(capacity::Payload::default())
        .try_connect()
        .expect("server");
    let pool = Pool::try_new(1, 4096).expect("control owner");
    let mut lease = pool.acquire().expect("receive lease");
    lease
        .try_append(&GOAWAY)
        .expect("control frames fit")
        .commit();
    let shared = lease.freeze().into_shared();
    let original = shared.as_ref().as_ptr() as usize + 17;
    server
        .ingress()
        .ingest_owned(shared)
        .expect("GOAWAY ingress");
    let debug = match server.ingress().poll_event() {
        Some(Event::GoAway { debug, .. }) => Some(debug),
        _ => None,
    }
    .expect("GOAWAY event");
    let bytes = debug.contiguous().expect("contiguous debug");
    assert_eq!(bytes, b"bye!");
    assert_ne!(bytes.as_ptr() as usize, original);
    assert_eq!(pool.available(), 1);
}

#[test]
fn preserves_owners() {
    let (wire, offset) = try_wire().expect("request fixture");
    let split = offset + 4;
    let first = Shared::try_copy(&wire[..split]).expect("first retained owner");
    let second = Shared::try_copy(&wire[split..]).expect("second retained owner");
    let mut server = Connection::<ServerRole>::try_new().expect("server allocation");

    server
        .ingress()
        .ingest_owned(first)
        .expect("first retained ingress");
    server
        .ingress()
        .ingest_owned(second)
        .expect("second retained ingress");

    let data = iter::from_fn(|| server.ingress().poll_event())
        .find_map(|event| match event {
            Event::Data { data, .. } => Some(data),
            _ => None,
        })
        .expect("fragmented data event");
    assert_eq!(data.contiguous(), None);
    assert!((data.count()) == (segments::Count::TWO));
    let mut joined = Vec::new();
    data.for_each(|segment| joined.extend_from_slice(segment));
    assert_eq!(joined, b"retained-payload");
}

#[test]
fn retains_live_events() {
    let plan = capacity::Payload::new(3, 1).expect("payload capacity");
    assert_eq!(plan.segments(), 7);
    let (_, mut server, stream) = Upload::with_payload(plan)
        .try_connect()
        .expect("server fixture");
    let wire = stream.data(3);
    let (frames, tail) = wire.as_chunks::<14>();
    assert!(tail.is_empty(), "complete DATA frames");
    let mut frames = frames.iter();
    server
        .try_chunks(frames.next().expect("first frame"))
        .expect("fragmented input");
    for frame in frames {
        server.try_owned(frame).expect("retained input");
    }

    for count in [
        segments::Count::new(5),
        segments::Count::ONE,
        segments::Count::ONE,
    ] {
        let data = match server
            .ingress()
            .poll_event()
            .expect("retained event must be DATA")
        {
            Event::Data { data, .. } => Some(data),
            _ => None,
        }
        .expect("retained event must be DATA");
        assert!((data.count()) == (count));
        assert!(
            data.segments()
                .flat_map(|part| part.as_ref().iter())
                .eq(b"abcde".iter())
        );
    }
}

#[test]
fn resumes_frame() {
    let plan = capacity::Payload::new(3, 1).expect("payload capacity");
    let (_, mut server, stream) = Upload::with_payload(plan)
        .try_connect()
        .expect("server fixture");
    let wire = stream.data(2);
    let (frames, tail) = wire.as_chunks::<14>();
    assert!(tail.is_empty(), "complete DATA frames");
    let mut frames = frames.iter();
    server
        .try_chunks(frames.next().expect("first frame"))
        .expect("fragmented input");
    let second = fragments(frames.next().expect("second frame"));
    for chunk in &second[..4] {
        server.try_owned(chunk).expect("retained input");
    }
    let retained = Shared::try_copy(second[4]).expect("final fragment");
    let error = server.ingress().ingest_owned(retained);
    assert!(
        error.err()
            == Some(Error::Segments(segments::Error::SegmentCapacity(
                depot::capacity::Shortage {
                    requested: NonZero::new(second.len()).expect("frame has physical owners"),
                    available: 2
                }
            )))
    );

    {
        let data = match server
            .ingress()
            .poll_event()
            .expect("first event must be DATA")
        {
            Event::Data { data, .. } => Some(data),
            _ => None,
        }
        .expect("first event must be DATA");
        assert!((data.count()) == (segments::Count::new(5)));
        assert!(
            data.segments()
                .flat_map(|part| part.as_ref().iter())
                .eq(b"abcde".iter())
        );
    }

    server.ingress().resume().expect("resume ingress");
    let data = match server
        .ingress()
        .poll_event()
        .expect("resumed event must be DATA")
    {
        Event::Data { data, .. } => Some(data),
        _ => None,
    }
    .expect("resumed event must be DATA");
    assert!((data.count()) == (segments::Count::new(5)));
    assert!(
        data.segments()
            .flat_map(|part| part.as_ref().iter())
            .eq(b"abcde".iter())
    );
}

#[test]
fn retains_fragments() {
    let plan = capacity::Payload::new(3, 1)
        .expect("payload capacity")
        .with_arena(15)
        .expect("segment capacity");
    let (_, mut server, stream) = Upload::with_payload(plan)
        .try_connect()
        .expect("server fixture");
    let wire = stream.data(3);
    assert_eq!(wire.len(), 3 * 14);
    for frame in wire.as_chunks::<14>().0 {
        server.try_chunks(frame).expect("fragmented input");
    }

    for _ in 0..3 {
        let data = match server
            .ingress()
            .poll_event()
            .expect("retained fragment event must be DATA")
        {
            Event::Data { data, .. } => Some(data),
            _ => None,
        }
        .expect("retained fragment event must be DATA");
        assert!((data.count()) == (segments::Count::new(5)));
        assert!(
            data.segments()
                .flat_map(|part| part.as_ref().iter())
                .eq(b"abcde".iter())
        );
    }
}
