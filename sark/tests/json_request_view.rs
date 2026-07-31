use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use http::StatusCode;
use o3::buffer::{Bytes, Retained};
use sark::json::{JsonEncode, JsonRequestDecode};
use sark::service::{RouteRequestImpl, RouteSpec};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

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

#[sark_gen::json(ordered, plain)]
struct PlainBody {
    text: Bytes<Retained>,
    #[field(raw)]
    number: Bytes<Retained>,
}

#[sark_gen::json(ordered)]
struct GeneralBody {
    text: Bytes<Retained>,
    #[field(raw)]
    number: Bytes<Retained>,
}

#[sark_gen::request(ordered)]
#[json_body(PlainBody)]
struct EchoRequest {}

#[sark_gen::response]
#[header("content-type", "application/json")]
struct EchoResponse<'req> {
    status: StatusCode,
    body: PlainBodyJsonView<'req>,
}

#[sark_gen::handler]
fn echo(request: EchoRequest, _state: &()) -> EchoResponse<'req> {
    EchoResponse {
        status: StatusCode::OK,
        body: request.body,
    }
}

fn in_input(input: &[u8], bytes: &[u8]) -> bool {
    let input_start = input.as_ptr() as usize;
    let input_end = input_start + input.len();
    let bytes_start = bytes.as_ptr() as usize;
    input_start <= bytes_start && bytes_start + bytes.len() <= input_end
}

#[test]
fn plain_request_view_borrows_without_allocating() {
    let input = br#"{"text":"alpha","number":42}"#;
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let body = <PlainBody as JsonRequestDecode>::decode_request(input).expect("decode");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    let after = ALLOCATIONS.load(Ordering::Relaxed);

    assert_eq!(after, before);
    assert_eq!(body.text.as_slice(), b"alpha");
    assert_eq!(body.number.as_slice(), b"42");
    assert!(in_input(input, body.text.as_slice()));
    assert!(in_input(input, body.number.as_slice()));
    assert_eq!(
        size_of_val(&body.text),
        size_of::<&[u8]>(),
        "plain request fields stay two-word borrowed slices",
    );
}

#[test]
fn general_request_view_borrows_unescaped_and_owns_only_escaped_fields() {
    let plain_input = br#"{"text":"alpha","number":42}"#;
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let plain =
        <GeneralBody as JsonRequestDecode>::decode_request(plain_input).expect("plain decode");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(after, before);
    assert_eq!(plain.text.as_slice(), b"alpha");
    assert!(in_input(plain_input, plain.text.as_slice()));
    assert!(in_input(plain_input, plain.number.as_slice()));
    assert_eq!(
        size_of_val(&plain.text),
        3 * size_of::<usize>(),
        "general request JSON bytes stay a three-word Cow",
    );
    let owned = plain.text.into_shared();
    assert_eq!(owned.as_slice(), b"alpha");
    assert!(!in_input(plain_input, owned.as_slice()));

    let escaped_input = br#"{"text":"a\"b","number":42}"#;
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let escaped =
        <GeneralBody as JsonRequestDecode>::decode_request(escaped_input).expect("escaped decode");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(
        after - before,
        1,
        "only the escaped field receives a decode allocation",
    );
    assert_eq!(escaped.text.as_slice(), b"a\"b");
    assert!(!in_input(escaped_input, escaped.text.as_slice()));
    assert!(in_input(escaped_input, escaped.number.as_slice()));
    assert_eq!(escaped.encode_json().as_slice(), escaped_input);

    let owned = escaped.text.into_shared();
    assert_eq!(owned.as_slice(), b"a\"b");
}

#[test]
fn generated_request_and_borrowed_json_response_are_wired() {
    fn require_route<T: RouteSpec>() {}
    require_route::<echo>();

    let body = <EchoRequest as RouteRequestImpl>::parse_body(br#"{"text":"alpha","number":42}"#)
        .expect("request body parses");
    assert_eq!(
        body.encode_json().as_slice(),
        br#"{"text":"alpha","number":42}"#,
    );
}
