pub mod common;

use std::{array, convert::Infallible, iter, pin::Pin};

use common::response::{
    crc32,
    gzip::{BACKREF, EMPTY_LARGE_HINT, Encode as _, HEADER_LEN},
};
use dope_test::alloc::Tracker;
use o3::{
    base::cell::region::Token,
    buffer::{resident, storage, write::SpareExt as _},
};
use sark_http::payload::{
    compress::inflate::{self, Plan},
    crc32::Hasher,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn decodes_owned() {
    let original = b"resident gunzip body".repeat(64);
    let encoded = original.gzip();
    let mut out = storage::Owned::try_with(original.len()).expect("test output must allocate");
    let mut spare = out.spare();
    spare.fill(0);
    let mut decoder = inflate::Gzip::default();
    let plan = Plan::try_segments(iter::once(encoded.as_ref()), original.len())
        .expect("valid member header");
    decoder
        .decode_plan(plan, spare)
        .expect("test operation must succeed");

    assert_eq!(out.as_slice(), original);
}

#[test]
fn reuses_decoder() {
    let mut decoder = inflate::Gzip::default();
    for original in [
        b"first gzip member".as_slice(),
        b"second gzip member".as_slice(),
    ] {
        let encoded = original.gzip();
        let mut out = storage::Owned::try_with(original.len()).expect("test output must allocate");
        let mut spare = out.spare();
        spare.fill(0);
        let plan = Plan::try_segments(iter::once(encoded.as_ref()), original.len())
            .expect("valid member header");
        decoder
            .decode_plan(plan, spare)
            .expect("each independent member must decode");
        assert_eq!(out.as_slice(), original);
    }
}

#[test]
fn decodes_members() {
    let first = b"first member".repeat(64);
    let second = b"second member".repeat(32);
    let expected = [first.as_slice(), second.as_slice()].concat();
    let encoded = [
        first.gzip(),
        b"".gzip(),
        second.gzip().wrap(true),
        b"".gzip(),
    ]
    .concat();
    for width in [1, encoded.len()] {
        let mut output = storage::Owned::try_with(expected.len()).expect("output");
        let mut spare = output.spare();
        spare.fill(0);
        let segments = encoded
            .chunks(width)
            .flat_map(|bytes| [b"".as_slice(), bytes])
            .chain(iter::once(b"".as_slice()));
        let plan = Plan::try_segments(segments, 0).expect("header");
        inflate::Gzip::default()
            .decode_plan(plan, spare)
            .expect("members");
        assert_eq!(output.as_slice(), expected);
    }
}

#[test]
fn verifies_each_member() {
    let first = b"first".gzip();
    let second = b"second".gzip();
    for index in [3, second.len() - 8, second.len() - 4] {
        let mut encoded = [first.as_slice(), second.as_slice()].concat();
        encoded[first.len() + index] ^= if index == 3 { 0x20 } else { 1 };
        let mut output = storage::Owned::try_with(11).expect("output");
        let mut spare = output.spare();
        spare.fill(0);
        let failure = inflate::Gzip::default()
            .decode(encoded.chunks(1), spare)
            .expect_err("member");
        assert_eq!(failure.written(), if index == 3 { 5 } else { 11 });
        assert!(match failure.error() {
            inflate::Error::Header {
                offset,
                cause: inflate::HeaderFault::Flags { actual: 0x20 },
            } => offset == inflate::Count::Exact(first.len() + 3),
            inflate::Error::Checksum { expected, actual } => expected == actual ^ 1,
            inflate::Error::Length {
                expected: 7,
                actual: 6,
            } => true,
            _ => false,
        });
        assert!(output.is_empty());
    }
}

#[test]
fn resets_member_history() {
    let first = b"A".gzip();
    let mut second = b"AAA".gzip();
    second.splice(10..second.len() - 8, BACKREF.iter().copied());
    let encoded = [first, second].concat();
    let mut output = storage::Owned::try_with(4).expect("output");
    let mut spare = output.spare();
    spare.fill(0);
    let failure = inflate::Gzip::default()
        .decode(encoded.chunks(2), spare)
        .expect_err("history");
    assert!(matches!(failure.error(), inflate::Error::Inflate { .. }));
    assert_eq!(failure.written(), 1);
    assert!(output.is_empty());
}

#[test]
fn grows_one_owner() {
    let mut expected = b"resident members".repeat(2);
    expected.push(b'!');
    let mut encoded = [expected.gzip(), b"".gzip()].concat();
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(expected.len(), token.proof())
            .expect("budget");
        let plan = Plan::try_segments(encoded.chunks(1), 0).expect("header");
        let output = inflate::Gzip::default()
            .decode_owned(plan, 4096, &budget)
            .expect("members");
        assert_eq!(output.as_slice(), expected);
        assert_eq!(budget.resident(), expected.len());
        drop(output);
        let trailer = encoded.len() - 8;
        encoded[trailer] ^= 1;
        let plan = Plan::try_segments(encoded.chunks(1), 0).expect("header");
        let failure = inflate::Gzip::default()
            .decode_owned(plan, 4096, &budget)
            .expect_err("checksum");
        assert!(
            matches!(failure, inflate::DecodeError::Stream(failure) if matches!(failure.error(), inflate::Error::Checksum { .. }))
        );
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn bounds_unverified_hint() {
    let (encoded, hint) = EMPTY_LARGE_HINT;
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(hint, token.proof())
            .expect("resident budget");
        let plan = Plan::try_segments(iter::once(encoded), hint).expect("member header");
        let mut peak = 0;
        let failure = inflate::Gzip::default()
            .owned_with(plan, hint, &budget, || {
                peak = peak.max(budget.resident());
                Ok::<(), Infallible>(())
            })
            .expect_err("forged member size");
        assert!(peak <= 64 * 1024);
        assert!(matches!(failure,
            inflate::Checked::Work(inflate::DecodeError::Stream(failure))
                if failure.written() == 0
                    && failure.error() == (inflate::Error::Length { expected: hint, actual: 0 })
        ));
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn checked_windows() {
    let expected = (u16::MIN..=u16::MAX)
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
        .repeat(3);
    let encoded = expected.gzip();
    assert!(expected.len() > 64 * 1024 && encoded.len() > 64 * 1024);
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(expected.len(), token.proof())
            .expect("resident budget");
        let hint = 64 * 1024 + 1;
        let plan = Plan::try_segments(iter::once(encoded.as_slice()), hint).expect("member header");
        let mut checks = 0;
        let mut decoder = inflate::Gzip::default();
        let (output, allocations) = Tracker::count(|| {
            decoder.owned_with(plan, expected.len(), &budget, || {
                if budget.resident() > hint {
                    checks += 1;
                }
                Ok::<(), Infallible>(())
            })
        });
        let output = output.expect("checked decode crosses input and output windows");
        assert_eq!(output.as_slice(), expected);
        assert!(checks > 1, "checked decoding makes checkpoint progress");
        assert!(allocations <= 4, "capacity growth stays geometric");
    });
}

#[test]
fn rejects_wrong_length() {
    let original = b"exact gunzip body".repeat(8);
    let encoded = original.gzip();
    let mut out = storage::Owned::try_with(original.len() - 1).expect("test output must allocate");
    let mut spare = out.spare();
    spare.fill(0);
    let plan = Plan::try_segments(iter::once(encoded.as_ref()), original.len())
        .expect("valid member header");
    let error = inflate::Gzip::default()
        .decode_plan(plan, spare)
        .expect_err("output length mismatch must be rejected");

    assert!(matches!(
        error.error(),
        inflate::Error::Length { expected, actual }
            if expected == original.len() && actual == original.len() - 1
    ));
    assert!(out.is_empty());
}

#[test]
fn decodes_fragmented() {
    let original = b"segmented resident gunzip body".repeat(256);
    let encoded = original.gzip();
    for checksum in [false, true] {
        let wrapped = encoded.wrap(checksum);
        let mut out = storage::Owned::try_with(original.len()).expect("test output must allocate");
        let mut spare = out.spare();
        spare.fill(0);
        inflate::Gzip::default()
            .decode(wrapped.chunks(1), spare)
            .expect("fragmented optional header must decode with or without FHCRC");
        assert_eq!(out.as_slice(), original);
    }

    let wrapped = encoded.wrap(true);
    let mut out = storage::Owned::try_with(original.len()).expect("test output must allocate");
    let mut spare = out.spare();
    spare.fill(0);
    let (result, allocs) = Tracker::usage(|| {
        let segments = wrapped.chunks(3).flat_map(|bytes| [b"".as_slice(), bytes]);
        let plan = Plan::try_segments(segments, original.len())?;
        inflate::Gzip::default().decode_plan(plan, spare)
    });
    (result).as_ref().expect("test operation must succeed");
    assert_eq!(allocs, (0, 0));
    assert_eq!(out.as_slice(), original);

    let empty = b"".gzip();
    (inflate::Gzip::default().decode(empty.chunks(1), storage::Owned::new().spare()))
        .as_ref()
        .expect("test operation must succeed");
}

#[test]
fn preserves_header_errors() {
    let encoded = b"".gzip();
    for checksum in [false, true] {
        let wrapped = encoded.wrap(checksum);
        let Err(failure) = Plan::try_segments(wrapped[..HEADER_LEN - 1].chunks(1), 0) else {
            panic!("unterminated comment must fail before output admission");
        };
        assert_eq!(
            failure.error(),
            inflate::Error::Eof {
                part: inflate::Part::Comment,
                offset: inflate::Count::Exact(HEADER_LEN - 1),
            },
        );
    }
    let mut wrapped = encoded.wrap(true);
    let actual = u16::from_le_bytes([wrapped[HEADER_LEN], wrapped[HEADER_LEN + 1]]);
    wrapped[HEADER_LEN] ^= 1;
    let Err(failure) = Plan::try_segments(wrapped.chunks(1), 0) else {
        panic!("incorrect FHCRC must fail before output admission");
    };
    assert_eq!(
        failure.error(),
        inflate::Error::Header {
            offset: inflate::Count::Exact(HEADER_LEN),
            cause: inflate::HeaderFault::Checksum {
                expected: actual ^ 1,
                actual,
            },
        },
    );
    assert_eq!(failure.written(), 0);
}

#[test]
fn preserves_errors() {
    let original = b"typed gzip failure".repeat(128);
    let encoded = original.gzip();

    let mut short =
        storage::Owned::try_with(original.len() - 1).expect("test output must allocate");
    let mut spare = short.spare();
    spare.fill(0);
    let failure = (inflate::Gzip::default().decode(encoded.chunks(3), spare))
        .expect_err("short output must be rejected");
    assert!(
        failure.error()
            == inflate::Error::Output {
                capacity: original.len() - 1,
                written: original.len() - 1,
            }
    );
    assert_eq!(failure.written(), original.len() - 1);
    assert!(short.is_empty());

    let mut checksum = encoded.clone();
    let trailer = checksum.len() - 8;
    checksum[trailer] ^= 1;
    let mut output = storage::Owned::try_with(original.len()).expect("test output must allocate");
    let mut spare = output.spare();
    spare.fill(0);
    let failure = (inflate::Gzip::default().decode(checksum.chunks(2), spare))
        .expect_err("checksum mismatch must be rejected");
    assert!(matches!(
        failure.error(),
        inflate::Error::Checksum { expected, actual } if expected != actual
    ));
    assert_eq!(failure.written(), original.len());
    assert!(output.is_empty());

    let plan = Plan::try_segments(iter::once(checksum.as_slice()), original.len())
        .expect("valid member header");
    let mut spare = output.spare();
    spare.fill(0);
    let mut decoder = inflate::Gzip::default();
    let decoded = decoder
        .decode_plan(plan, spare)
        .expect_err("checksum mismatch must retain the same leaf failure");
    assert_eq!(decoded, failure);

    let mut length = encoded.clone();
    let declared =
        u32::try_from(original.len() + 1).expect("fixture length must fit the gzip footer");
    let footer = length.len() - 4;
    length[footer..].copy_from_slice(&declared.to_le_bytes());
    let mut spare = output.spare();
    spare.fill(0);
    let failure = (inflate::Gzip::default().decode(length.chunks(5), spare))
        .expect_err("length mismatch must be rejected");
    assert!(
        failure.error()
            == inflate::Error::Length {
                expected: original.len() + 1,
                actual: original.len(),
            }
    );
    assert_eq!(failure.written(), original.len());
    assert!(output.is_empty());

    let mut trailing = encoded.clone();
    trailing.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
    let mut spare = output.spare();
    spare.fill(0);
    let failure = (inflate::Gzip::default().decode(trailing.chunks(7), spare))
        .expect_err("trailing input must be rejected");
    assert!(
        failure.error()
            == inflate::Error::Trailing {
                bytes: inflate::Count::Exact(3),
            }
    );
    assert_eq!(failure.written(), original.len());
    assert!(output.is_empty());
}

#[test]
fn streams_ieee() {
    assert_eq!(Hasher::hash(b"123456789"), 0xcbf4_3926);
    for offset in [0, 1, 15] {
        let data: [u8; 4111] = array::from_fn(|index| {
            (index.wrapping_sub(offset) as u8)
                .wrapping_mul(197)
                .wrapping_add(29)
        });
        for (len, expected) in crc32::VECTORS {
            let bytes = &data[offset..offset + len];
            assert_eq!(Hasher::hash(bytes), expected);
            for width in [1, 7, 16, 63, 64, 127, 128, 257] {
                let mut state = Hasher::new();
                for chunk in bytes.chunks(width) {
                    state.update(chunk);
                }
                state.update(&[]);
                assert_eq!(state.finalize(), expected);
            }
        }
    }
}
