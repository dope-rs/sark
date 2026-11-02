use std::{array, mem};

use dope_test::alloc::Tracker;
use sark_http::payload::compress::{Compression, Gzip, Output};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plain_uses_no_storage() {
    let (_, allocs) = Tracker::usage(|| {
        <() as Compression>::try_new().expect("plain construction is infallible");
        assert!(matches!(().encode(b"response"), Output::Plain));
    });
    assert_eq!(allocs, (0, 0));
}

#[test]
fn reuses_released_output() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let (_, allocs) = Tracker::usage(|| {
        let mut held: [_; Gzip::SLOTS.get()] = array::from_fn(|_| {
            let output = gzip.encode(&[b'x'; 256]);
            assert!(matches!(&output, Output::Compressed(_)));
            output
        });
        assert!(matches!(gzip.encode(&[b'x'; 256]), Output::Plain));
        drop(mem::replace(&mut held[0], Output::Plain));
        assert!(matches!(gzip.encode(&[b'x'; 256]), Output::Compressed(_)));
    });
    assert_eq!(allocs, (0, 0));
}

#[test]
fn keeps_unprofitable_plain() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let bytes = array::from_fn::<_, 256, _>(|index| index as u8);
    let (_, allocs) = Tracker::usage(|| {
        assert!(matches!(gzip.encode(b"response"), Output::Plain));
        assert!(matches!(gzip.encode(&bytes), Output::Plain));
        let output = match gzip.encode(&[b'x'; 256]) {
            Output::Compressed(output) => Some(output),
            Output::Plain => None,
        }
        .expect("compressible body");
        assert!(output.len() < 256);
    });
    assert_eq!(allocs, (0, 0));
}
