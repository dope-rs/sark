use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use sark_grpc::headers::HeaderBlock;
use sark_grpc::metadata::Metadata;
use sark_grpc::status::Status;

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
fn common_grpc_header_blocks_use_one_packed_allocation() {
    let metadata = Metadata::new();

    let before = allocations();
    let request = HeaderBlock::for_request(b"/svc/Method", Some(b"localhost"), &metadata).unwrap();
    assert_eq!(allocations() - before, 1);
    assert_eq!(request.iter().count(), 6);

    let before = allocations();
    let response = HeaderBlock::for_response(&metadata).unwrap();
    assert_eq!(allocations() - before, 1);
    assert_eq!(response.iter().count(), 2);

    let before = allocations();
    let trailers = HeaderBlock::for_trailers(&Status::ok(), &metadata).unwrap();
    assert_eq!(allocations() - before, 1);
    assert_eq!(trailers.iter().count(), 1);
}
