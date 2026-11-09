use common::atomic::Setup;
use dope_quic::wire::stream::Id;
use dope_test::alloc::Tracker;
use o3::buffer::{hold, pool::Pool, resident, storage};
use sark_core::{
    decode::{Encode, Lineage},
    request,
};
use sark_h3::{
    conn::{Connection, Error, output::WritePayload},
    manifold::encoder,
    stream::{Cancellation, Target},
};
use sark_http::{
    field::{Value, metadata::section::storage::Bytes},
    owner::{
        evidence::{Cause, Copy},
        handoff::{self, Project as _, input},
    },
    payload::{
        owned,
        response::{Method, dispatch::Accept, headers, status::Code},
    },
    request::body::Ref,
};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn inspect(conn: &mut Connection<'_>, check: impl FnOnce(&[u8])) -> Result<(), Error> {
    loop {
        let write = conn
            .events()
            .poll_write()?
            .expect("the committed response must contain a DATA body");
        let Some(body) = write.payload else {
            continue;
        };
        assert!(
            matches!(&body, WritePayload::Shared(_) | WritePayload::Retained(_)),
            "one request segment retains its contiguous owner"
        );
        match &body {
            WritePayload::Shared(owner) => check(owner.as_ref()),
            WritePayload::Retained(owner) => check(owner.as_slice()),
            WritePayload::Owned(_) | WritePayload::Segmented(_) => {}
        }
        return Ok(());
    }
}

#[test]
fn charges_retained_headers() -> Result<(), Error> {
    let mut conn = Setup::with_writes(2)
        .try_build::<'static>()
        .expect("H3 queue");
    let headers = [headers::Entry::new(
        headers::Name::try_bytes(b"x-long").expect("header name"),
        Value::try_bytes(&[b'a'; 64]).expect("header value"),
    )];
    let encoder = encoder::Encoder::new(&mut conn, Target::new(Id::ZERO), Method::Other);
    let charge = Accept::fixed(encoder, Code::OK, headers, Vec::new())
        .map_err(|(error, _)| error)?
        .finish()?;
    let retained = conn.retained(Id::ZERO)?;
    assert_eq!(retained.owners, 1);
    assert_eq!(charge, retained);
    Ok(())
}

#[test]
fn rejects_partial_response() {
    let stream = Id::ZERO;
    let result = Setup::with_writes(2).try_build::<'static>();
    let mut conn = result.expect("the H3 queue fixture must configure");
    let encoder = encoder::Encoder::new(&mut conn, Target::new(stream), Method::Other);
    let result = Accept::fixed(encoder, Code::OK, (), b"body".to_vec());
    assert!(result.is_ok(), "the response fixture must prepare");
    if let Ok(prepared) = result {
        assert!(matches!(
            prepared.finish(),
            Err(Error::QueueRegular {
                queued: 0,
                additional: 2,
                limit: 1,
            })
        ));
        assert!(matches!(conn.events().poll_write(), Ok(None)));
    }
}

#[test]
fn moves_request_provenance() -> Result<(), Error> {
    let stream = Id::ZERO;
    let result = Setup::with_writes(3).try_build::<'static>();
    let mut conn = result.expect("the H3 queue fixture must configure");
    let result = storage::Shared::try_from(&mut b"request-owner".to_vec());
    let request = result.expect("request fixture storage must fit its owner");
    let request = owned::Segments::from(request);
    let body = request
        .segment(0)
        .expect("the fixture must retain one request segment");
    let pointer = body.as_ptr();
    let borrowed = body;
    let encoder = encoder::Encoder::from_slot(
        &mut conn,
        Target::new(stream),
        Method::Other,
        handoff::Slot::new(Ref::from(&request)),
    );
    let result = Accept::fixed(encoder, Code::OK, (), borrowed);
    assert!(result.is_ok(), "the response fixture must prepare");
    if let Ok(prepared) = result {
        prepared.finish()?;
        inspect(&mut conn, |segment| {
            assert_eq!(segment, b"request-owner");
            assert_eq!(segment.as_ptr(), pointer);
        })?;
    }
    Ok(())
}

#[test]
fn commits_request_lineage() -> Result<(), Error> {
    let mut conn = Setup::with_writes(3)
        .try_build::<'static>()
        .expect("H3 queue");
    let request = owned::Segments::from(storage::Shared::try_copy(b"same-owner").expect("owner"));
    let expected = request.charge();
    input::Input::with(request, |input| {
        let (prepared, owner) = input.segmented((), |(), body, slot| {
            let enc =
                encoder::Encoder::from_slot(&mut conn, Target::new(Id::ZERO), Method::Other, slot);
            let borrowed = body.segment(0).expect("one segment");
            Accept::fixed(enc, Code::OK, (), borrowed)
                .map_err(|(error, _)| error)
                .and_then(|prepared| prepared.try_detach())
        });
        let prepared = prepared?;
        let (commit, allocs) = Tracker::count(|| prepared.finish_with(owner));
        let (charge, linked, copied) = commit?;
        assert_eq!(allocs, 0);
        assert!(charge == expected);
        assert!(linked == expected);
        assert!(copied.is_none());
        Ok(())
    })
}

#[test]
fn sums_linked_segments() -> Result<(), Error> {
    let stream = Id::ZERO;
    let result = Setup::with_writes(3).try_build::<'static>();
    let mut conn = result.expect("the H3 queue fixture must configure");
    let request = owned::Segments::new();
    let result = storage::Shared::try_copy(b"first");
    let first = result.expect("the first request segment must allocate");
    let result = request.try_push(owned::Segment::from(resident::Shared::from(first)));
    let request = result.expect("the first request segment must fit");
    let result = storage::Shared::try_copy(b"second");
    let second = result.expect("the second request segment must allocate");
    let result = request.try_push(owned::Segment::from(resident::Shared::from(second)));
    let request = result.expect("the second request segment must fit");
    let expected = request.charge();
    input::Input::with(request, |input| {
        let (prepared, owner) = input.segmented((), |(), body, slot| {
            let enc =
                encoder::Encoder::from_slot(&mut conn, Target::new(stream), Method::Other, slot);
            Accept::fixed(enc, Code::OK, (), body)
                .map_err(|(error, _)| error)
                .and_then(|prepared| prepared.try_detach())
        });
        let prepared = prepared?;
        let (commit, allocs) = Tracker::count(|| prepared.finish_with(owner));
        let (charge, linked, copied) = commit?;
        assert_eq!(allocs, 0);
        assert!(charge == expected);
        assert!(linked == expected);
        assert!(copied.is_none());
        Ok(())
    })
}

#[test]
fn drops_rejected_lineage() {
    let mut conn = Setup::with_writes(2)
        .try_build::<'static>()
        .expect("H3 queue");
    let request = owned::Segments::from(storage::Shared::try_copy(b"same-owner").expect("owner"));
    let result = input::Input::with(request, |input| {
        let (prepared, owner) = input.segmented((), |(), body, slot| {
            let enc =
                encoder::Encoder::from_slot(&mut conn, Target::new(Id::ZERO), Method::Other, slot);
            Accept::fixed(enc, Code::OK, (), body)
                .map_err(|(error, _)| error)
                .and_then(|prepared| prepared.try_detach())
        });
        prepared?.finish_with(owner)
    });
    assert!(matches!(
        result,
        Err(Error::QueueRegular {
            queued: 0,
            additional: 2,
            limit: 1
        })
    ));
    assert!(matches!(conn.events().poll_write(), Ok(None)));
}

#[test]
fn keeps_head_zero_copy() -> Result<(), Error> {
    let stream = Id::ZERO;
    let result = Setup::with_writes(3).try_build::<'static>();
    let mut conn = result.expect("the H3 queue fixture must configure");
    let result = Pool::try_new(1, 64);
    let pool = result.expect("the request-head pool must allocate");
    let mut lease = pool
        .acquire()
        .expect("the sole request-head entry must be available");
    lease
        .try_append(b"head-owner")
        .expect("head fits fixture lease")
        .commit();

    let encoder = encoder::Encoder::new(&mut conn, Target::new(stream), Method::Other);
    let head = Bytes::new(lease.freeze());
    let pointer = head.as_bytes().as_ptr();
    let head_len = head.as_bytes().len();
    let result = request::Head::<_>::from_path(head, 0..head_len, head_len, None);
    let head = result.expect("the fixture path must belong to its request head");
    let result = head.scope(|head| {
        let encoder = Encode::begin_head(encoder, head.owner().view());
        let borrowed = head.borrow();
        let prepared = Accept::fixed(encoder, Code::OK, (), *borrowed.owner())?;
        Ok::<_, (Error, Cancellation)>(Lineage::resolve_head(prepared, head))
    });
    assert!(result.is_ok(), "the head-backed response must prepare");
    if let Ok(prepared) = result {
        let result = prepared.try_detach();
        let detached = result?;
        let result = detached.finish_with(());
        let result = result?;
        assert!(result.1 == hold::Charge::default());
        assert!(result.2.is_none());
        assert!(pool.acquire().is_none());
        inspect(&mut conn, |segment| {
            assert_eq!(segment, b"head-owner");
            assert_eq!(segment.as_ptr(), pointer);
        })?;
        assert!(pool.acquire().is_some());
    }
    Ok(())
}

#[test]
fn reports_foreign_copy() -> Result<(), Error> {
    let stream = Id::ZERO;
    let result = Setup::with_writes(3).try_build::<'static>();
    let mut conn = result.expect("the H3 queue fixture must configure");
    let foreign = &b"foreign"[..];
    let encoder = encoder::Encoder::new(&mut conn, Target::new(stream), Method::Other);
    let result = Accept::fixed(encoder, Code::OK, (), foreign);
    assert!(result.is_ok(), "the response fixture must prepare");
    if let Ok(prepared) = result {
        let result = prepared.try_detach();
        let detached = result?;
        let result = detached.finish_with(());
        let result = result?;
        assert!(result.1 == hold::Charge::default());
        assert!(
            (result.2)
                == (Some(Copy {
                    read: 7,
                    written: 7,
                    cause: Cause::Borrowed,
                }))
        );
    }
    Ok(())
}
