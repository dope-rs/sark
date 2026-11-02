pub mod common;

use common::hpack::{Encoded, GET, SHORTEST_LITERALS};
use sark_h2::hpack::{
    decode::Decoder,
    encode::{Encoder, Mode},
};
use sark_http::field::{Indexing, metadata::map::record::Entry};

#[test]
fn never_existing_matches() {
    let seed = [
        GET[3],
        Entry::new(b"x-secret", b"www.example.com"),
        Entry::new(b"x-secret", b"other"),
    ];
    for (mode, value) in [
        (Mode::Plain, b"\x0fwww.example.com".as_slice()),
        (Mode::Shortest, &SHORTEST_LITERALS[2].1[1..]),
    ] {
        let mut encoder = Encoder::try_new(4096).expect("encoder");
        encoder.set_mode(mode);
        let mut wire = Vec::new();
        encoder.try_encode(seed, &mut wire).expect("HPACK table");
        let (mut decoder, _) = Encoded::new(&wire)
            .try_collect(Decoder::try_new(4096).expect("decoder"))
            .expect("initial table");
        let size = decoder.dyn_size();
        for field in [
            GET[0],
            seed[0],
            seed[1],
            seed[2],
            Entry::new(b"x-new", b"www.example.com"),
        ] {
            wire.clear();
            encoder
                .try_encode([field.with_index(Indexing::Never)], &mut wire)
                .expect("never indexed");
            assert_eq!(wire[0] & 0xf0, 0x10);
            if field.value == b"www.example.com" {
                assert!(wire.ends_with(value));
            }
            let (next, fields) = Encoded::new(&wire)
                .try_collect(decoder)
                .expect("never indexed field");
            decoder = next;
            assert_eq!(fields, [(field.name.to_vec(), field.value.to_vec())]);
            assert_eq!((decoder.dyn_len(), decoder.dyn_size()), (seed.len(), size));
        }
        wire.clear();
        encoder.try_encode(seed, &mut wire).expect("existing table");
        assert_eq!(wire, [0xc0, 0xbf, 0xbe]);
    }
}
