use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use sark_core::http::Field;
use sark_h3::qpack::{DecodeOutcome, Decoder, DecoderError, Encoder};

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
fn qpack_decodes_literals_and_table_references_into_one_packed_allocation() {
    let literal_fields = [
        Field::new(b":method", b"GET"),
        Field::new(b"x-direct", b"huffman value"),
    ];
    let mut encoder = Encoder::new();
    encoder.set_huffman(true);
    let mut literal_block = Vec::new();
    encoder.encode(literal_fields, &mut literal_block);
    let mut decoder = Decoder::new(4096);

    let before = allocations();
    let decoded = decoder.decode(&literal_block).unwrap();
    assert_eq!(allocations() - before, 1);
    assert_eq!(decoded.iter().collect::<Vec<_>>(), literal_fields);

    let dynamic_field = Field::new(b"x-dynamic", b"retained once");
    let mut encoder = Encoder::with_dynamic_capacity(256);
    encoder.set_dynamic_capacity(256).unwrap();
    encoder.set_max_blocked_streams(1);
    let mut first_block = Vec::new();
    encoder.encode([dynamic_field], &mut first_block);
    let instructions = encoder.take_encoder_instructions();
    let mut referenced_block = Vec::new();
    encoder.encode([dynamic_field], &mut referenced_block);

    let mut decoder = Decoder::with_dynamic_capacity(4096, 256);
    let before = allocations();
    assert!(matches!(
        decoder.decode_or_blocked(&referenced_block).unwrap(),
        DecodeOutcome::Blocked { .. }
    ));
    assert_eq!(allocations() - before, 0);
    decoder.ingest_encoder(&instructions).unwrap();

    let before = allocations();
    let decoded = decoder.decode(&referenced_block).unwrap();
    assert_eq!(allocations() - before, 1);
    assert_eq!(decoded.iter().collect::<Vec<_>>(), [dynamic_field]);

    let mut limited = Decoder::new(41);
    let mut method_block = Vec::new();
    Encoder::new().encode([Field::new(b":method", b"GET")], &mut method_block);
    assert_eq!(limited.decode(&method_block), Err(DecoderError::BadLiteral));

    let oversized_value = vec![b'a'; 4096];
    let mut encoder = Encoder::new();
    encoder.set_huffman(true);
    let mut oversized_block = Vec::new();
    encoder.encode(
        [Field::new(b"x-oversized", &oversized_value)],
        &mut oversized_block,
    );
    let mut limited = Decoder::new(64);
    let before = allocations();
    assert_eq!(
        limited.decode(&oversized_block),
        Err(DecoderError::BadLiteral)
    );
    assert_eq!(
        allocations() - before,
        1,
        "Huffman expansion must stop before growing past the field-section budget"
    );
}
