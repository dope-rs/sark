use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use o3::buffer::{Bytes, Retained, Shared};
use sark_json::{JsonBytes, Parse};

struct CountingAllocator;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
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
        ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
    }
}

fn tracked<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATIONS.with(Cell::get);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let value = f();
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    let after = ALLOCATIONS.with(Cell::get);
    (value, after - before)
}

fn in_input(input: &[u8], bytes: &[u8]) -> bool {
    let input_start = input.as_ptr() as usize;
    let input_end = input_start + input.len();
    let bytes_start = bytes.as_ptr() as usize;
    input_start <= bytes_start && bytes_start + bytes.len() <= input_end
}

#[test]
fn unescaped_frames_project_without_allocating() {
    let input: &[u8] = br#""alpha","tail""#;
    let mut borrowed_idx = 0;
    let (borrowed, borrowed_allocations): (JsonBytes<'_>, _) =
        tracked(|| Parse::frame(input, &mut borrowed_idx).expect("borrowed frame"));
    assert_eq!(borrowed.as_slice(), b"alpha");
    assert_eq!(borrowed_idx, 7);
    assert_eq!(borrowed_allocations, 0);
    assert!(in_input(input, borrowed.as_slice()));

    let owner = Shared::from_static(input);
    let mut retained_idx = 0;
    let (retained, retained_allocations): (Bytes<Retained>, _) =
        tracked(|| Parse::frame(&owner, &mut retained_idx).expect("retained frame"));
    assert_eq!(retained.as_slice(), b"alpha");
    assert_eq!(retained_idx, borrowed_idx);
    assert_eq!(retained_allocations, 0);
    assert!(in_input(input, retained.as_slice()));
}

#[test]
fn escaped_frames_decode_with_one_allocation() {
    let input: &[u8] = br#""a\"b\\c\/d\b\f\n\r\t","tail""#;
    let expected = b"a\"b\\c/d\x08\x0c\n\r\t";

    let mut borrowed_idx = 0;
    let (borrowed, borrowed_allocations): (JsonBytes<'_>, _) =
        tracked(|| Parse::frame(input, &mut borrowed_idx).expect("borrowed frame"));
    assert_eq!(borrowed.as_slice(), expected);
    assert_eq!(borrowed_allocations, 1);
    assert!(!in_input(input, borrowed.as_slice()));

    let owner = Shared::from_static(input);
    let mut retained_idx = 0;
    let (retained, retained_allocations): (Bytes<Retained>, _) =
        tracked(|| Parse::frame(&owner, &mut retained_idx).expect("retained frame"));
    assert_eq!(retained.as_slice(), expected);
    assert_eq!(retained_idx, borrowed_idx);
    assert_eq!(retained_allocations, 1);
    assert!(!in_input(input, retained.as_slice()));
}

#[test]
fn generic_plain_and_raw_frames_preserve_boundaries() {
    let plain: &[u8] = br#""alpha",next"#;
    let mut plain_idx = 0;
    let plain_frame = Parse::frame_plain(plain, &mut plain_idx).expect("plain frame");
    assert_eq!(plain_frame.as_slice(), b"alpha");
    assert_eq!(plain_idx, 7);

    let raw: &[u8] = b"12345,next";
    let mut raw_idx = 0;
    let raw_frame = Parse::frame_raw(raw, &mut raw_idx).expect("raw frame");
    assert_eq!(raw_frame.as_slice(), b"12345");
    assert_eq!(raw_idx, 5);

    let mut out_of_bounds = raw.len() + 1;
    assert!(Parse::frame_raw(raw, &mut out_of_bounds).is_err());
    assert_eq!(out_of_bounds, raw.len() + 1);
}

#[test]
fn invalid_escape_and_plain_escape_keep_error_positions() {
    let invalid: &[u8] = br#""a\u0062",next"#;
    let mut invalid_idx = 0;
    assert!(Parse::frame(invalid, &mut invalid_idx).is_err());
    assert_eq!(invalid_idx, 9);

    let plain: &[u8] = br#""a\"b",next"#;
    let mut plain_idx = 0;
    assert!(Parse::frame_plain(plain, &mut plain_idx).is_err());
    assert_eq!(plain_idx, 2);
}

#[test]
fn inline_frames_scan_once_and_fail_at_capacity() {
    let plain = br#""abc",next"#;
    let mut plain_idx = 0;
    let token = Parse::inline_plain::<3>(plain, &mut plain_idx).expect("inline plain");
    assert_eq!(token.as_bytes(), b"abc");
    assert_eq!(plain_idx, 5);

    let overflow = br#""abcd",next"#;
    let mut overflow_idx = 0;
    assert!(Parse::inline_plain::<3>(overflow, &mut overflow_idx).is_err());
    assert_eq!(overflow_idx, 4);

    let raw = b"123,next";
    let mut raw_idx = 0;
    let token = Parse::inline_raw::<3>(raw, &mut raw_idx).expect("inline raw");
    assert_eq!(token.as_bytes(), b"123");
    assert_eq!(raw_idx, 3);

    let mut out_of_bounds = raw.len() + 1;
    assert!(Parse::inline_raw::<3>(raw, &mut out_of_bounds).is_err());
    assert_eq!(out_of_bounds, raw.len() + 1);
}
