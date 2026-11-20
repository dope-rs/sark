use std::{array, iter, mem};

use dope_test::alloc::Tracker;
use o3::buffer::{storage, write::SpareExt as _};
use sark_http::payload::compress::{Compression, Error, Gzip, Output, inflate};

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
fn fixed_capacity_boundary() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let bytes = vec![b'x'; 262_036];
    assert!(matches!(
        gzip.encode(&bytes[..262_035]),
        Output::Compressed(_)
    ));
    assert!(matches!(gzip.encode(&bytes), Output::Plain));
}

#[test]
fn required_keeps_empty_and_expanding_members() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let bytes = array::from_fn::<_, 256, _>(|index| index as u8);
    for original in [b"".as_slice(), b"response", &bytes] {
        let encoded = gzip.try_encode(original).expect("required member");
        assert!(encoded.len() > original.len());
        let mut decoded = storage::Owned::try_with(original.len()).expect("decoded storage");
        let mut spare = decoded.spare();
        spare.fill(0);
        inflate::Gzip::default()
            .decode(iter::once(encoded.as_ref()), spare)
            .as_ref()
            .expect("complete gzip member");
        assert_eq!(decoded.as_slice(), original);
    }
}

#[test]
fn required_preserves_admission_failures() {
    assert!(matches!(().try_encode(b""), Err(Error::Unavailable)));
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let held: [_; Gzip::SLOTS.get()] =
        array::from_fn(|_| gzip.try_encode(b"").expect("required member"));
    assert!(matches!(
        gzip.try_encode(&vec![b'x'; 262_036]),
        Err(Error::Input {
            actual: 262_036,
            limit: 262_035,
        })
    ));
    assert!(matches!(gzip.try_encode(b""), Err(Error::Exhausted)));
    drop(held);
    assert!(gzip.try_encode(b"").is_ok());
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
