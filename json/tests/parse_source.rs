use std::mem;
pub mod common;

use common::{Bytes as _, encode::Quoted};
use dope_test::alloc::Tracker;
use o3::buffer::storage::Shared;
use sark_json::Source;

#[test]
fn inline_token_layout() {
    assert_eq!(mem::size_of::<sark_json::Token<31>>(), 32);
}

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn unescaped_borrows() {
    let input: &[u8] = br#""alpha","tail""#;
    let mut bpos = 0;
    let (borrowed, (balloc, _)) = Tracker::usage(|| Source::frame(input, &mut bpos));
    let borrowed = borrowed.expect("borrowed frame");
    assert_eq!(borrowed.as_slice(), b"alpha");
    assert_eq!(bpos, 7);
    assert_eq!(balloc, 0);
    assert!(borrowed.as_slice().is_within(input));

    let owner = Shared::try_copy(input).expect("owned input");
    let mut rpos = 0;
    let (retained, (ralloc, _)) = Tracker::usage(|| Source::frame(&owner, &mut rpos));
    let retained = retained.expect("retained frame");
    assert_eq!(retained.as_slice(), b"alpha");
    assert_eq!(rpos, bpos);
    assert_eq!(ralloc, 0);
    assert_eq!(retained.as_slice().as_ptr(), owner.as_slice()[1..].as_ptr());
    drop(owner);
    assert_eq!(retained.as_slice(), b"alpha");
}

#[test]
fn string_word_lanes() {
    for value in [
        '\0',
        ' ',
        '\u{7f}',
        '"',
        '\\',
        '\n',
        '\u{80}',
        '\u{800}',
        '\u{10000}',
    ] {
        for offset in 0..24 {
            let quoted = Quoted::try_new(value, offset).expect("scalar lane");
            let input = quoted.output.as_slice();
            let mut pos = 0;
            let frame = Source::frame(input, &mut pos).expect("string");
            assert_eq!(frame.as_slice(), quoted.input.as_bytes());
            assert_eq!(pos, input.len());
            if value >= ' ' && !matches!(value, '"' | '\\') {
                assert_eq!(frame.as_slice().as_ptr(), input[1..].as_ptr());
            }
        }
    }
}

#[test]
fn string_word_boundaries() {
    for offset in 0..24 {
        let mut quoted = Quoted::try_new('a', offset).expect("scalar lane");
        for byte in (0..0x20).chain(0x80..=u8::MAX).chain(*b"\\") {
            quoted.output[1 + offset] = byte;
            let mut pos = 0;
            assert!(matches!(
                Source::frame(quoted.output.as_slice(), &mut pos),
                Err(sark_json::Error::Invalid)
            ));
            assert_eq!(pos, 0);
        }

        quoted.output[1 + offset] = b'"';
        let input = quoted.output.as_slice();
        let mut pos = 0;
        let frame = Source::frame(input, &mut pos).expect("closing quote");
        assert_eq!(frame.as_slice(), &quoted.input.as_bytes()[..offset]);
        assert_eq!(frame.as_slice().as_ptr(), input[1..].as_ptr());
        assert_eq!(pos, offset + 2);
    }

    let mut pos = 0;
    assert!(matches!(
        Source::frame(common::decode::INTERRUPTED_UTF8, &mut pos),
        Err(sark_json::Error::Invalid)
    ));
    assert_eq!(pos, 0);
}

#[test]
fn escaped_allocates() {
    let input: &[u8] = br#""a\"b\\c\/d\b\f\n\r\t","tail""#;
    let expected = b"a\"b\\c/d\x08\x0c\n\r\t";

    let mut bpos = 0;
    let (borrowed, (balloc, _)) = Tracker::usage(|| Source::frame(input, &mut bpos));
    let borrowed = borrowed.expect("borrowed escaped frame");
    assert_eq!(borrowed.as_slice(), expected);
    assert_eq!(balloc, 1);
    assert!(!borrowed.as_slice().is_within(input));

    let owner = Shared::try_copy(input).expect("owned input");
    let mut rpos = 0;
    let (retained, (ralloc, _)) = Tracker::usage(|| Source::frame(&owner, &mut rpos));
    let retained = retained.expect("retained escaped frame");
    assert_eq!(retained.as_slice(), expected);
    assert_eq!(rpos, bpos);
    assert_eq!(ralloc, 1);
    assert!(!retained.as_slice().is_within(owner.as_slice()));
    drop(owner);
    assert_eq!(retained.as_slice(), expected);
}

#[test]
fn frames_keep_bounds() {
    let plain: &[u8] = br#""alpha",next"#;
    let mut ppos = 0;
    let pframe = Source::frame_plain(plain, &mut ppos).expect("plain frame");
    assert_eq!(pframe, b"alpha");
    assert_eq!(ppos, 7);

    let raw: &[u8] = b"12345,next";
    let mut rpos = 0;
    let rframe = Source::frame_raw(raw, &mut rpos).expect("raw frame");
    assert_eq!(rframe, b"12345");
    assert_eq!(rpos, 5);

    let mut beyond = raw.len() + 1;
    assert!(Source::frame_raw(raw, &mut beyond).is_err());
    assert_eq!(beyond, raw.len() + 1);
}

#[test]
fn errors_keep_offsets() {
    let invalid: &[u8] = br#""a\uZZZZ",next"#;
    let mut pos = 0;
    assert!(Source::frame(invalid, &mut pos).is_err());
    assert_eq!(pos, 0);

    let plain: &[u8] = br#""a\"b",next"#;
    let mut pos = 0;
    assert!(Source::frame_plain(plain, &mut pos).is_err());
    assert_eq!(pos, 6);
}

#[test]
fn inline_capacity() {
    let plain = br#""abc",next"#;
    let mut pos = 0;
    let token = Source::inline_plain::<3>(&plain[..], &mut pos).expect("inline plain");
    assert_eq!(token.as_bytes(), b"abc");
    assert_eq!(pos, 5);

    let overflow = br#""abcd",next"#;
    let mut pos = 0;
    assert!(matches!(
        Source::inline_plain::<3>(&overflow[..], &mut pos),
        Err(sark_json::Error::Token(error)) if {
            let (required, available) = error.into_parts();
            required.get() == 4 && available == 3
        }
    ));
    assert_eq!(pos, 0);

    let raw = b"123,next";
    let mut pos = 0;
    let token = Source::inline_raw::<3>(&raw[..], &mut pos).expect("inline raw");
    assert_eq!(token.as_bytes(), b"123");
    assert_eq!(pos, 3);

    let mut pos = 0;
    let token = Source::inline_raw::<3>(b"[1],next".as_slice(), &mut pos).expect("inline group");
    assert_eq!(token.as_bytes(), b"[1]");
    assert_eq!(pos, 3);

    let mut pos = 0;
    assert!(matches!(
        Source::inline_raw::<3>(b"[12]".as_slice(), &mut pos),
        Err(sark_json::Error::Token(error)) if {
            let (required, available) = error.into_parts();
            required.get() == 4 && available == 3
        }
    ));
    assert_eq!(pos, 0);

    let mut beyond = raw.len() + 1;
    assert!(Source::inline_raw::<3>(&raw[..], &mut beyond).is_err());
    assert_eq!(beyond, raw.len() + 1);
}
