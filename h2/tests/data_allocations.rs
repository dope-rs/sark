use common::{
    capacity::{POST, Upload},
    protocol::session::Frame,
};
use dope_test::alloc::Tracker;
use o3::buffer::{segments, storage::Shared};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        DataOutcome,
        config::Config,
        event::Event,
        machine::{Connection, streams},
    },
};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn padded_is_zero_alloc() {
    let (_, mut server, id) = Upload::new(Config::default())
        .try_connect()
        .expect("upload fixture");
    let wire = Frame::new(id.into_u32(), b"abcdefghijkl")
        .try_padded(Boundary::More, 2)
        .expect("padded DATA fixture");
    let wire = Shared::try_copy(&wire).expect("padded DATA owner");
    let mut received = false;

    let (allocs, _) = Tracker::during(|| {
        server
            .ingress()
            .ingest_owned(wire)
            .expect("ingest padded DATA");
        received = matches!(
            server.ingress().poll_event(),
            Some(Event::Data { data, .. }) if data.segments().flat_map(|part| part.as_ref().iter()).eq(b"abcdefghijkl".iter())
        );
    });

    assert_eq!(allocs, 0);
    assert!(received);
}

#[test]
fn segments_are_zero_alloc() {
    let (_, mut server, id) = Upload::new(Config::default())
        .try_connect()
        .expect("upload fixture");
    let wire = Frame::new(id.into_u32(), b"abcdefghijkl")
        .try_data(Boundary::More)
        .expect("DATA fixture");
    let chunks = [
        (Shared::try_copy(&wire[..18]).expect("first DATA segment")),
        (Shared::try_copy(&wire[18..19]).expect("second DATA segment")),
        (Shared::try_copy(&wire[19..20]).expect("third DATA segment")),
        (Shared::try_copy(&wire[20..]).expect("fourth DATA segment")),
    ];
    let mut received = false;

    let (allocs, _) = Tracker::during(|| {
        for chunk in chunks {
            server
                .ingress()
                .ingest_owned(chunk)
                .expect("ingest DATA segment");
        }
        received = matches!(
            server.ingress().poll_event(),
            Some(Event::Data { data, .. })
                if data.count() == segments::Count::new(4) && data.segments().flat_map(|part| part.as_ref().iter()).eq(b"abcdefghijkl".iter())
        );
    });

    assert_eq!(allocs, 0);
    assert!(received);
}

#[test]
fn key_send_is_zero_alloc() {
    let mut client = Connection::<ClientRole>::try_new().expect("client connection");
    let key = client
        .streams()
        .headers()
        .request_key(&POST[..3], Boundary::More)
        .expect("open keyed upload");
    let (sent, allocs) = Tracker::count(|| {
        client
            .streams()
            .try_to(key, b"ab", b"cd", Boundary::End, streams::Limit::MAX)
            .expect("send keyed DATA")
    });
    assert_eq!(allocs, 0);
    assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 4));

    let mut server = Connection::<ServerRole>::try_new().expect("server connection");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest keyed upload");
    let mut payload = None;
    while let Some(event) = server.ingress().poll_event() {
        if let Event::Data {
            stream_id: id,
            data,
            boundary,
        } = event
        {
            payload = Some((id, data, boundary));
        }
    }
    let (stream, data, boundary) = payload.expect("one DATA event");
    assert!((stream.id()) == (key.id()));
    assert!(matches!(boundary, Boundary::End));
    assert!(
        data.segments()
            .flat_map(|part| part.as_ref().iter())
            .eq(b"abcd")
    );
}
