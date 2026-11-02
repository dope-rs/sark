use std::num::NonZero;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{self, config::Config, machine::Connection},
    frame::{
        HEADER_LEN,
        layout::Header,
        numbers::{Flags, Type},
    },
};

#[test]
fn emits_one_frame() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client allocation");
    conn.transfer().drain(usize::MAX);
    let stream = conn.streams().headers().request([], Boundary::More);
    let stream = stream.expect("request head");
    conn.transfer().drain(usize::MAX);

    let written = conn
        .streams()
        .try_parts(stream, b"header", b"payload", Boundary::End);
    assert!(matches!(written, Ok(conn::DataOutcome::Sent(bytes)) if bytes.get() == 13));

    let out = conn.output().first();
    let header = Header::parse(out).expect("encoded frame header");
    assert!((header.kind) == (Type::Data));
    assert_eq!(header.length.into_u32(), 13);
    assert!(header.flags.contains(Flags::END_STREAM));
    assert_eq!(&out[HEADER_LEN..], b"headerpayload");

    let tail = out[5..].to_vec();
    conn.transfer().drain(5);
    assert_eq!(conn.output().first(), tail);
}

#[test]
fn keeps_admitted_prefix() {
    for (first, second, expected, end) in [
        (
            b"abcdefgh".as_slice(),
            b"ij".as_slice(),
            b"abcde".as_slice(),
            false,
        ),
        (
            b"ab".as_slice(),
            b"cdefg".as_slice(),
            b"abcde".as_slice(),
            false,
        ),
        (b"".as_slice(), b"abc".as_slice(), b"abc".as_slice(), true),
        (b"".as_slice(), b"".as_slice(), b"".as_slice(), true),
    ] {
        let mut conn = Connection::<ClientRole>::with_config(Config {
            outbound_capacity: 82,
            ..Config::default()
        })
        .expect("client allocation");
        conn.transfer().drain(usize::MAX);
        let stream = conn
            .streams()
            .headers()
            .request([], Boundary::More)
            .expect("request head");
        conn.transfer().drain(usize::MAX);
        for _ in 0..4 {
            conn.control().ping([0; 8]).expect("reserve output");
        }
        let credit = conn.status().send_window().value();
        let outcome = conn
            .streams()
            .try_parts(stream, first, second, Boundary::End)
            .expect("bounded DATA");
        assert_eq!(
            outcome,
            NonZero::new(expected.len()).map_or(conn::DataOutcome::Ended, conn::DataOutcome::Sent)
        );
        conn.transfer().drain(68);
        let mut wire = [0; HEADER_LEN + 5];
        let written = conn.transfer().drain_into(&mut wire);
        let header = Header::parse(&wire[..written]).expect("DATA header");
        assert_eq!(header.length.into_usize(), expected.len());
        assert_eq!(header.flags.contains(Flags::END_STREAM), end);
        assert_eq!(&wire[HEADER_LEN..written], expected);
        assert_eq!(
            conn.status().send_window().value(),
            credit - expected.len() as i32
        );
        assert_eq!(
            conn.status().send_for(stream).map(|window| window.value()),
            Some(credit - expected.len() as i32)
        );
    }
}
