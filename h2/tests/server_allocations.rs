use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use sark_h2::hpack::{Header, HeaderBlock};
use sark_h2::server::{Body, Config, Request, Response, SyncApp, SyncHandler};

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

#[test]
fn repeated_body_allocates_once_and_reuse_allocates_nothing() {
    let before_repeat = allocations();
    let large = Body::repeat(b'x', 1 << 20);
    assert_eq!(allocations() - before_repeat, 1);
    assert_eq!(large.len(), 1 << 20);
    assert!(large.as_slice().iter().all(|byte| *byte == b'x'));

    let before_static = allocations();
    let static_body = Body::from_static(b"static");
    assert_eq!(allocations(), before_static);
    assert_eq!(static_body.as_slice(), b"static");

    let before_clone = allocations();
    let reusable = large.clone();
    assert_eq!(allocations(), before_clone);
    assert_eq!(large.as_slice().as_ptr(), reusable.as_slice().as_ptr());

    let handler = move |request: Request| {
        assert_eq!(request.path(), Some(&b"/large"[..]));
        Response::text(reusable.clone())
    };
    let config = Config {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 64,
        max_connections_per_ip: 64,
        listen_backlog: 128,
        max_handler_tasks: 0,
        max_request_body_bytes: 16 << 20,
        max_connection_body_bytes: 64 << 20,
        max_outbound_bytes: 64 << 10,
        socket_receive_buffer_bytes: None,
        socket_send_buffer_bytes: None,
        tcp_fast_open_backlog: None,
        receive_buffer_bytes: 64 << 10,
        receive_buffer_count: 1024,
    };
    let before_app = allocations();
    let app: SyncApp<'_, _> = SyncApp::new(&handler, config);
    assert_eq!(allocations(), before_app);
    let request = Request {
        headers: HeaderBlock::from_headers(&[Header::new(b":path", b"/large")]),
        body: Vec::new(),
    };
    let before_response = allocations();
    let response = SyncHandler::request(app.handler(), request);
    assert_eq!(allocations(), before_response);
    drop(response);
}
