use data::raw::Payload as _;
use dope::net::link::egress::{self, data};
use dope_test::alloc::Tracker;
use o3::buffer::raw::Scatter as _;
use sark_h1::server::payload::Body;
use sark_http::payload::compress::{Compression as _, Gzip, Output};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn compressed_owner_returns() {
    let result = Gzip::try_new();
    let mut gzip = result.expect("gzip allocation");
    let mut held = Vec::with_capacity(Gzip::SLOTS.get());
    let (_, allocs) = Tracker::count(|| {
        for _ in 0..2 {
            for tag in 0..Gzip::SLOTS.get() {
                let output = gzip.encode(&[b'x'; 256]);
                assert!(matches!(&output, Output::Compressed(_)));
                if let Output::Compressed(owner) = output {
                    let pointer = owner.as_ref().as_ptr();
                    let len = owner.len();
                    let load = egress::Load::new(Body::Frozen { owner, tag });
                    let value = load.as_inner();
                    assert_eq!(value.len(), len);
                    assert_eq!(value.charge().resident_bytes, Gzip::CAPACITY.get());
                    assert_eq!(value.charge().owners, 1);
                    let cursor = value.cursor(data::Offset::ZERO);
                    let cursor = cursor.expect("compressed data owns its cursor");
                    let mut chunks = cursor;
                    let mut view = chunks.take_bytes(len);
                    let first = view.next();
                    let first = first.expect("nonempty compressed frame has its first segment");
                    assert_eq!(first.bytes().as_ptr(), pointer);
                    assert_eq!(first.bytes().len(), len);
                    assert!(first.bytes().starts_with(&[0x1f, 0x8b]));
                    assert!(view.next().is_none());
                    held.push(load);
                }
            }
            assert!(matches!(gzip.encode(&[b'x'; 256]), Output::Plain));
            for (tag, load) in held.drain(..).enumerate() {
                assert_eq!(load.into_inner().release(), tag);
            }
        }
    });
    assert_eq!(allocs, 0);

    let output = gzip.encode(&[b'x'; 256]);
    assert!(matches!(&output, Output::Compressed(_)));
    if let Output::Compressed(owner) = output {
        let value = Body::Frozen { owner, tag: 7 };
        drop(gzip);
        let cursor = value.cursor(data::Offset::ZERO);
        let cursor = cursor.expect("compressed data owns its cursor");
        let mut chunks = cursor;
        let first = chunks.take_bytes(value.len()).next();
        let first = first.expect("nonempty compressed frame has its first segment");
        assert!(first.bytes().starts_with(&[0x1f, 0x8b]));
        assert_eq!(value.release(), 7);
    }
}
