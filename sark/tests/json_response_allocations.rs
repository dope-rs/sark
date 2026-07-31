use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use http::StatusCode;
use o3::buffer::{Borrowed, ByteSink, Bytes, CapacityError, Retained, Shared, SliceWriter};
use sark::http::{EncodedBody, EncodedResponse};
use sark::json::{JsonEncode, Write};
use sark::service::manifold::NativeResponse;

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct RawBody<'req> {
    key: Bytes<Borrowed<'req>>,
    nonce: Bytes<Borrowed<'req>>,
}

impl EncodedBody for RawBody<'_> {
    type Error = CapacityError;

    fn encoded_len(&self) -> usize {
        self.key.len() + 1 + self.nonce.len()
    }

    fn encode_into(&self, out: &mut [u8]) -> Result<(), Self::Error> {
        let key = self.key.as_slice();
        let nonce = self.nonce.as_slice();
        SliceWriter::new(out).write_slices([key, b":", nonce])
    }
}

#[sark_gen::response(encoded)]
#[header("content-type", "text/plain")]
struct RawReply<'req> {
    status: StatusCode,
    body: RawBody<'req>,
    #[header("x-key")]
    key: Bytes<Borrowed<'req>>,
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
}

fn count_allocation() {
    if TRACK_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn encoded_response_writes_a_borrowed_body_without_allocating() {
    let key = Bytes::<Borrowed<'_>>::from(&b"alpha"[..]);
    let nonce = Bytes::<Borrowed<'_>>::from(&b"42"[..]);
    let response = RawReply {
        status: StatusCode::OK,
        body: RawBody { key, nonce },
        key,
    }
    .into_fixed();
    let response: EncodedResponse<'_, RawBody<'_>, 1, _> =
        NativeResponse::into_route_response(response);

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut out = [0u8; 512];
    let written = response
        .write_into_slice(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("response fits");
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    assert_eq!(after, before);
    assert!(out[..written].ends_with(b"alpha:42"));
}

#[sark_gen::json(encode)]
struct Body {
    ok: bool,
    value: u64,
    shared: Shared,
    local: Bytes<Retained>,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct Reply {
    status: StatusCode,
    body: Body,
}

#[test]
fn json_response_encodes_directly_into_write_buffer() {
    let shared = Shared::from_static(b"shared");
    let response = Reply {
        status: StatusCode::OK,
        body: Body {
            ok: true,
            value: 42,
            shared: shared.clone(),
            local: Bytes::<Retained>::from(shared),
        },
    }
    .into_fixed();
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut out = [0u8; 512];
    let written = response
        .write_into_slice(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("response fits");
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    assert_eq!(after, before);
    assert!(
        out[..written].ends_with(br#"{"ok":true,"value":42,"shared":"shared","local":"shared"}"#)
    );
}

static LENGTH_VISITS: AtomicUsize = AtomicUsize::new(0);

struct Counted;

impl JsonEncode for Counted {
    fn json_len(&self) -> usize {
        LENGTH_VISITS.fetch_add(1, Ordering::Relaxed);
        2
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        writer.put(b"{}")
    }
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct CountedReply {
    status: StatusCode,
    body: Counted,
}

#[test]
fn encoded_length_is_visited_once_per_response() {
    LENGTH_VISITS.store(0, Ordering::Relaxed);
    let response = CountedReply {
        status: StatusCode::OK,
        body: Counted,
    }
    .into_fixed();
    assert_eq!(LENGTH_VISITS.load(Ordering::Relaxed), 1);

    let mut out = [0u8; 256];
    let written = response
        .write_into_slice(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("response fits");
    assert!(out[..written].ends_with(b"{}"));
    assert_eq!(LENGTH_VISITS.load(Ordering::Relaxed), 1);

    let response = CountedReply {
        status: StatusCode::OK,
        body: Counted,
    }
    .into_fixed();
    assert_eq!(LENGTH_VISITS.load(Ordering::Relaxed), 2);
    let (_, body) = response
        .write_head_split(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("head fits");
    assert_eq!(body.as_slice(), b"{}");
    assert_eq!(LENGTH_VISITS.load(Ordering::Relaxed), 2);
}
