use std::num::NonZero;
pub mod common;
use o3::buffer::storage::Shared;
use sark_json::{
    parsing::{Decode, Request},
    segment,
};

use crate::common::decode::*;

fn ready<T>(progress: segment::Progress<'_, T>) -> Option<T> {
    match progress {
        segment::Progress::Pending => None,
        segment::Progress::Ready(value) => Some(value),
        segment::Progress::Failed(error) => {
            panic!("complete segmented input must decode: {error:?}")
        }
        segment::Progress::NeedInput => panic!("complete segmented input requested more data"),
    }
}

#[test]
fn generic_decode_paths() {
    let raw = br#"{"text":"a\"b","number":42}"#;
    let owned = Shared::try_copy(raw).expect("allocate bounded JSON fixture");
    let owned = <General as Decode>::decode(owned).expect("owned decode");
    assert_eq!(owned.text.as_slice(), b"a\"b");
    assert_eq!(owned.number.as_slice(), b"42");

    let source = br#"{"text":"alpha"}"#;
    let plain = <Plain as Request>::decode(source).expect("plain borrowed decode");
    assert_eq!(plain.text, b"alpha");
}

#[test]
fn field_span_ownership() {
    let one = Shared::try_copy(br#"{"text":"alpha"}"#).expect("the literal segment must allocate");
    let expected = one.as_slice()[9..].as_ptr();
    let decoded = <Plain as Decode>::decode(one).expect("the complete literal must yield a value");
    assert_eq!(decoded.text.as_slice(), b"alpha");
    assert_eq!(decoded.text.as_slice().as_ptr(), expected);

    let mut decoder = segment::Decoder::<Plain>::new(vec![
        Shared::try_copy(br#"{"text":"al"#).expect("the first literal fragment must allocate"),
        Shared::try_copy(br#"pha"}"#).expect("the second literal fragment must allocate"),
    ])
    .expect("decoder input");
    let mut turns = 0;
    let decoded = loop {
        turns += 1;
        if let Some(decoded) = ready(decoder.poll(NonZero::<usize>::MIN.saturating_add(1))) {
            break decoded;
        }
    };
    assert!(turns > 1);
    assert_eq!(decoded.text.as_slice(), b"alpha");
}

#[test]
fn escaped_owner_join() {
    let first = Shared::try_copy(b"{\"text\":\"a\\").expect("allocate first JSON fragment");
    let second = Shared::try_copy(b"\"b\",\"number\":42}").expect("allocate second JSON fragment");
    let mut decoder =
        segment::Decoder::<General>::new(vec![first, second]).expect("admit segmented JSON input");
    let decoded = loop {
        if let Some(decoded) = ready(decoder.poll(NonZero::<usize>::MIN)) {
            break decoded;
        }
    };

    assert_eq!(decoded.text.as_slice(), b"a\"b");
    assert_eq!(decoded.number.as_slice(), b"42");
}

#[test]
fn skips_discarded_values_before_end() {
    for prefix in IGNORED_VALUES {
        let selected = Shared::try_copy(SELECTED_PREFIX).expect("selected owner");
        let ptr = selected.as_slice()[9..].as_ptr();
        let mut ignored = Vec::with_capacity(32 * 1024);
        ignored.extend_from_slice(prefix);
        ignored.resize(32 * 1024, b'x');
        let ignored = Shared::try_from(&mut ignored).expect("ignored owner");
        let skipped = ignored.as_slice().len();
        let mut decoder = segment::Decoder::<Selective>::try_stream().expect("decoder");
        for owner in [selected, ignored] {
            let segment::Transfer::Accepted(state) = decoder.push(owner.into()) else {
                panic!("input owner rejected");
            };
            decoder = state;
        }
        let mut turns = 0;
        let mut released = 0;
        let mut chunks = 0;
        loop {
            turns += 1;
            let progress = decoder.poll(NonZero::<usize>::MIN.saturating_add(1023));
            released += decoder.take_bytes();
            chunks += decoder.take_chunks().into_raw();
            match progress {
                segment::Progress::Pending => {}
                segment::Progress::NeedInput => break,
                _ => panic!("unfinished value must request input"),
            }
        }
        assert_eq!(released, skipped);
        assert_eq!(chunks, 1);
        assert_eq!(decoder.take_bytes(), 0);
        assert_eq!(decoder.take_chunks().into_raw(), 0);
        assert!(turns >= 32);
        let end = Shared::try_copy(VALUE_END).expect("closing owner");
        let segment::Transfer::Accepted(state) = decoder.push(end.into()) else {
            panic!("closing owner rejected");
        };
        let mut decoder = state.close();
        let decoded = loop {
            if let Some(decoded) = ready(decoder.poll(NonZero::<usize>::MIN.saturating_add(1023))) {
                break decoded;
            }
        };
        assert_eq!(decoded.text.as_slice(), b"alpha");
        assert_eq!(decoded.text.as_slice().as_ptr(), ptr);
    }
}
