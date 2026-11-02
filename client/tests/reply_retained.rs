use dope_test::alloc::Tracker;
use o3::buffer::{resident, resident::staging, segments, write::Sink as _};
use sark_client::reply::detach::{Error, Part};
use sark_http::payload::response::{Method, status};

pub mod common;
use common::{
    codec::Scope as _,
    reply::{COPIED_FIELDS, FRAGMENTED, PADDED, aliased_fields, chunked, try_sources},
    response::{Decode as _, PLAIN},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn detached_small_shapes() {
    for bytes in [&PLAIN[..0], PLAIN] {
        let response = resident::Domain::try_scope(4096, |budget| {
            let pool = segments::Pool::try_new(segments::Plan {
                segments: 1,
                chains: 0,
            })
            .expect("metadata");
            pool.try_body(budget, "", bytes)
                .expect("response")
                .try_retain()
                .expect("detach")
        })
        .expect("budget");
        assert_eq!(response.body().contiguous(), Some(bytes));
        assert_eq!(
            response.body().segments().len(),
            usize::from(!bytes.is_empty())
        );
    }
}

#[test]
fn detached_fields_reused() {
    let response = resident::Domain::try_scope(32 * 1024, |budget| {
        let pool = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("metadata");
        let response = pool
            .try_wire(budget, Method::Other, &chunked(PLAIN))
            .expect("response");
        let head = response.headers().get("x-head").expect("header").as_ptr();
        let tail = response.trailers().expect("trailers").as_bytes().as_ptr();
        let body = response.body().contiguous().expect("body").as_ptr();
        let (response, allocs) = Tracker::count(|| response.try_retain());
        let response = response.expect("detach unique fields");
        assert_eq!(allocs, 0);
        assert_eq!(
            response.headers().get("x-head").expect("header").as_ptr(),
            head
        );
        assert_eq!(
            response.trailers().expect("trailers").as_bytes().as_ptr(),
            tail
        );
        assert_eq!(response.body().contiguous().expect("body").as_ptr(), body);
        assert_eq!(budget.resident(), 0);
        response
    })
    .expect("budget");
    assert_eq!(response.status(), status::Code::CREATED);
    assert_eq!(
        response.headers().get("x-head"),
        Some(b"retained".as_slice())
    );
    assert_eq!(
        response.trailers().expect("trailers").get("x-tail"),
        Some(b"kept".as_slice())
    );
    assert_eq!(response.body().contiguous(), Some(PLAIN));
}

#[test]
fn flatten_copies_body() {
    let response = resident::Domain::try_scope(64 * 1024, |budget| {
        let pool = segments::Pool::try_new(segments::Plan {
            segments: 2,
            chains: 1,
        })
        .expect("metadata");
        let wire = chunked(FRAGMENTED);
        let response = pool
            .try_wire(budget, Method::Other, &wire)
            .expect("response");
        assert_eq!(response.body().segments().len(), 2);
        let error = Tracker::fail_after(0, || response.try_flatten())
            .err()
            .expect("body allocation");
        let (parts, bytes) = match error {
            Error::StorageExhausted {
                parts,
                requested: bytes,
                ..
            } => Some((parts, bytes)),
            _ => None,
        }
        .expect("storage failure");
        assert_eq!(bytes, FRAGMENTED.len());
        assert!(parts.contains(Part::Body));
        assert!(!parts.contains(Part::Headers));
        assert!(!parts.contains(Part::Trailers));

        let response = pool
            .try_wire(budget, Method::Other, &wire)
            .expect("response");
        let head = response.headers().get("x-head").expect("header").as_ptr();
        let tail = response.trailers().expect("trailers").as_bytes().as_ptr();
        let (response, allocs) = Tracker::count(|| response.try_flatten());
        let response = response.expect("flatten body");
        assert_eq!(allocs, 1);
        assert_eq!(
            response.headers().get("x-head").expect("header").as_ptr(),
            head
        );
        assert_eq!(
            response.trailers().expect("trailers").as_bytes().as_ptr(),
            tail
        );
        assert_eq!(budget.resident(), 0);
        response
    })
    .expect("budget");
    assert_eq!(response.body().as_slice(), FRAGMENTED);
    assert_eq!(response.status(), status::Code::CREATED);
}

#[test]
fn aliased_fields_copy_once() {
    let response = resident::Domain::try_scope(32 * 1024, |budget| {
        let wire = aliased_fields();
        let plan = staging::Plan::new(1, 8192).expect("staging plan");
        let layout = staging::Layout::new(1, plan, 0, 0).expect("staging layout");
        let staging = staging::Pool::try_new(&budget, layout).expect("staging pool");
        let mut sources = [
            staging::Buf::try_from(&staging).expect("head source"),
            staging::Buf::try_from(&staging).expect("trailer source"),
        ];
        for (source, bytes) in sources.iter_mut().zip(&wire) {
            source.try_reserve(bytes.len()).expect("source storage");
            source.write_slice(bytes).expect("source bytes");
        }
        let pool = segments::Pool::try_new(segments::Plan {
            segments: 0,
            chains: 0,
        })
        .expect("metadata");
        let response = try_sources(&pool, budget, Method::Other, &sources).expect("response");
        let resident = budget.resident();
        let error = Tracker::fail_after(0, || response.try_retain())
            .err()
            .expect("aliased fields need copies");
        let (parts, bytes) = match error {
            Error::StorageExhausted {
                parts,
                requested: bytes,
                ..
            } => Some((parts, bytes)),
            _ => None,
        }
        .expect("storage failure");
        assert_eq!(bytes, COPIED_FIELDS);
        assert!(parts.contains(Part::Headers));
        assert!(parts.contains(Part::Trailers));
        assert!(!parts.contains(Part::Body));
        assert_eq!(budget.resident(), resident);

        let response = try_sources(&pool, budget, Method::Other, &sources).expect("response");
        let head = response.headers().get("x-head").expect("header").as_ptr();
        let tail = response.trailers().expect("trailers").as_bytes().as_ptr();
        let (response, allocs) = Tracker::count(|| response.try_retain());
        let response = response.expect("copy aliased fields");
        assert_eq!(allocs, 1);
        assert_ne!(
            response.headers().get("x-head").expect("header").as_ptr(),
            head
        );
        assert_ne!(
            response.trailers().expect("trailers").as_bytes().as_ptr(),
            tail
        );
        assert_eq!(budget.resident(), resident);
        for (source, bytes) in sources.iter().zip(&wire) {
            assert_eq!(source.as_slice(), bytes);
        }
        response
    })
    .expect("budget");
    assert_eq!(response.headers().get("x-head"), Some(PADDED));
    assert_eq!(
        response.trailers().expect("trailers").get("x-tail"),
        Some(PADDED)
    );
}
