use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use sark_core::http::Field;
use sark_h3::qpack::{DecodeOutcome, Decoder, DecoderError, Encoder};
use sark_h3::{Conn, Role, StreamId, WritePayload};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static LARGE_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
const LARGE_ALLOCATION_SIZE: usize = 32 * 1024;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn count_allocation(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    if size >= LARGE_ALLOCATION_SIZE {
        LARGE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

fn allocations() -> usize {
    ALLOCATIONS.load(Ordering::Relaxed)
}

#[test]
fn qpack_decodes_literals_and_table_references_into_one_packed_allocation() {
    let large_value = vec![b'x'; 64 * 1024];
    let mut sender = Conn::with_role(Role::Server);
    let before = LARGE_ALLOCATIONS.load(Ordering::Relaxed);
    sender
        .send_headers(
            StreamId::new(0),
            [Field::new(b"content-type", &large_value)],
            false,
        )
        .unwrap();
    assert_eq!(
        LARGE_ALLOCATIONS.load(Ordering::Relaxed) - before,
        1,
        "H3 must retain the QPACK field-line allocation instead of duplicating it"
    );
    assert!(matches!(
        sender.poll_write().and_then(|write| write.payload),
        Some(WritePayload::Owned(_))
    ));
    let before = allocations();
    sender
        .send_headers(StreamId::new(4), [Field::new(b":status", b"200")], false)
        .unwrap();
    assert_eq!(
        allocations() - before,
        1,
        "a typical H3 header allocates only its owned QPACK field lines"
    );
    drop(sender.poll_write());

    let wire_fields = [
        Field::new(b":method", b"GET"),
        Field::new(b"x-direct", b"segmented"),
    ];
    let mut segmented_encoder = Encoder::new();
    let section = segmented_encoder.encode_section(wire_fields);
    let field_lines_ptr = section.field_lines().as_ptr();
    let mut segmented = Vec::new();
    section.encode_prefix(&mut segmented);
    segmented.extend_from_slice(section.field_lines());
    let field_lines = section.into_field_lines();
    let mut compatible = Vec::new();
    Encoder::new().encode(wire_fields, &mut compatible);
    assert_eq!(field_lines.as_ptr(), field_lines_ptr);
    assert_eq!(segmented, compatible);

    let warm_name = [b'n'; 64];
    let warm_value = [b'v'; 256];
    let mut no_table = Encoder::new();
    let mut no_table_wire = Vec::with_capacity(512);
    no_table.encode([Field::new(&warm_name, &warm_value)], &mut no_table_wire);
    no_table_wire.clear();
    let before = allocations();
    no_table.encode([Field::new(b"x", b"y")], &mut no_table_wire);
    assert_eq!(
        allocations() - before,
        0,
        "a disabled dynamic table must not allocate field owners"
    );

    let mut dynamic = Encoder::with_dynamic_capacity(2048);
    dynamic.set_dynamic_capacity(2048).unwrap();
    let mut dynamic_wire = Vec::with_capacity(512);
    dynamic.encode([Field::new(&warm_name, &warm_value)], &mut dynamic_wire);
    drop(dynamic.take_encoder_instructions());
    dynamic_wire.clear();
    let before = allocations();
    dynamic.encode([Field::new(b"x", b"y")], &mut dynamic_wire);
    assert_eq!(
        allocations() - before,
        3,
        "dynamic insertion owns name and value once, plus its encoder-stream buffer"
    );

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
    assert_eq!(
        allocations() - before,
        0,
        "decoded fields must reuse their bounded pool"
    );
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
    assert_eq!(allocations() - before, 0);
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
        0,
        "Huffman expansion must stop inside the bounded field-section slot"
    );
}
