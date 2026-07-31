use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use o3::buffer::{Bytes, Retained, Shared};
use sark_h2::frame::{Continuation, Headers};
use sark_h2::hpack::{Encoder, Header};
use sark_h2::{CLIENT_PREFACE, Conn, ServerRole, StreamId, conn};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn allocations() -> usize {
    ALLOCATIONS.load(Ordering::Relaxed)
}

fn fragmented_request(stream_id: u32) -> (Bytes<Retained>, Bytes<Retained>) {
    let mut encoder = Encoder::new(4096);
    let mut block = Vec::new();
    encoder.encode(
        [
            Header::new(b":method", b"GET"),
            Header::new(b":scheme", b"http"),
            Header::new(b":path", b"/"),
        ],
        &mut block,
    );
    let split = 1;
    let stream_id = StreamId::new(stream_id).unwrap();
    let mut first = Vec::new();
    Headers::new(stream_id, true, false, None, &block[..split])
        .unwrap()
        .encode(&mut first);
    let mut second = Vec::new();
    Continuation::new(stream_id, true, &block[split..])
        .unwrap()
        .encode(&mut second);
    (
        Bytes::from(Shared::copy_from_slice(&first)),
        Bytes::from(Shared::copy_from_slice(&second)),
    )
}

fn consume_request(conn: &mut Conn<ServerRole>) {
    let Some(conn::Event::Headers { stream_id, .. }) = conn.poll_event() else {
        panic!("expected request headers");
    };
    conn.send_response(stream_id, [Header::new(b":status", b"204")], true)
        .unwrap();
    conn.drain_outbound(conn.outbound().len());
}

#[test]
fn reused_fragmented_header_path_allocates_nothing() {
    let mut conn = Conn::<ServerRole>::new();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    while conn.poll_event().is_some() {}

    let (first, second) = fragmented_request(1);
    conn.ingest_retained(first).unwrap();
    conn.ingest_retained(second).unwrap();
    consume_request(&mut conn);

    let (first, second) = fragmented_request(3);
    let before = allocations();
    conn.ingest_retained(first).unwrap();
    conn.ingest_retained(second).unwrap();
    consume_request(&mut conn);
    assert_eq!(allocations(), before);
}
