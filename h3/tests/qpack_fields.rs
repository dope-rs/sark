use delta::buffer::Storage;
use dope_quic::wire::stream::Id;
use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{self, Connection, event::Role, io::Send, output::WritePayload},
    frame::ErrorCode,
    qpack::{
        decoder,
        encoder::{Encoder, Error},
    },
};
use sark_http::field::literal;

use crate::common::field::entry;
pub mod common;

use sark_http::{
    field::metadata::strings::Encoding,
    payload::compress::dynamic::{ENTRY_OVERHEAD, table},
};
use table::delta;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn cold_delta_allocates() {
    let mut encoder = Encoder::try_table(2048).expect("fixed QPACK test contract must hold");
    let mut wire = Vec::with_capacity(128);
    let entries = 2048usize / ENTRY_OVERHEAD;
    let (storage, (_, resident)) = Tracker::usage(|| Storage::try_new(2048, entries));
    drop(storage.expect("fixed QPACK test contract must hold"));

    let (_, alloc) = Tracker::usage(|| {
        encoder
            .encode(0, [literal!(entry(b"x-cold", b"value"))], &mut wire)
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(alloc, (2, resident + 13));
    assert!(!encoder.take_wire().is_empty());
    wire.clear();
    let (_, alloc) = Tracker::usage(|| {
        encoder
            .encode(0, [literal!(entry(b"x-cold", b"value"))], &mut wire)
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(alloc, (0, 0));
}

#[test]
fn inline_wire_moves() {
    let mut encoder = Encoder::try_table(256).expect("fixed QPACK test contract must hold");
    let (capacity, alloc) = Tracker::usage(|| encoder.take_wire());
    assert_eq!(&capacity[..], [0x3f, 0xe1, 0x01]);
    assert_eq!(alloc, (0, 0));

    let mut wire = Vec::with_capacity(64);
    encoder
        .encode(0, [literal!(entry(b"x-inline", b"value"))], &mut wire)
        .expect("fixed QPACK test contract must hold");

    let (delta, alloc) = Tracker::usage(|| encoder.take_wire());
    assert!(!delta.is_empty());
    assert_eq!(alloc, (0, 0));
}

#[test]
fn bounds_references() {
    use sark_h3::qpack::dynamics;

    let mut decoder = decoder::Decoder::try_dynamic(256, 256).expect("QPACK table");
    decoder
        .ingest(b"\x3f\xe1\x01\x43x-a\x01v\x43x-b\x01w")
        .expect("two dynamic entries");
    for encoded in [[2, 1, 0x80], [2, 0x80, 0x11]] {
        assert!(matches!(
            decoder.decode(&encoded),
            Err(decoder::Error::DynamicReference(
                dynamics::Reference::Required { absolute: 1, .. }
            ))
        ));
    }
    for encoded in [[2, 1, 0x81], [2, 0x80, 0x10]] {
        let fields = decoder
            .decode(&encoded)
            .expect("reference within declared count");
        assert!(
            (fields.iter().collect::<Vec<_>>()) == ([literal!(entry(b"x-a", b"v")).as_record()])
        );
    }
}

#[test]
fn rejects_split_overflow() {
    use decoder::ingress::error::{Error, Overflow};

    let mut encoded = [0x80; 11];
    encoded[0] = 0x3f;
    encoded[10] = 0x02;
    for split in 0..=encoded.len() {
        let mut decoder = decoder::Decoder::try_dynamic(256, 256).expect("QPACK table");
        let result = decoder
            .ingest(&encoded[..split])
            .and_then(|_| decoder.ingest(&encoded[split..]));
        let position = if split == encoded.len() {
            10
        } else {
            10 - split
        };
        assert_eq!(
            result,
            Err(Error::Integer(Overflow {
                position,
                value: 31,
                shift: 63,
                byte: 0x02,
            })),
            "split at {split}",
        );
    }
}

#[test]
fn output_failure_is_atomic() {
    let seed_key = [b'w'; 64];
    let seed_val = [b'v'; 64];
    let mut encoder = Encoder::try_table(512).expect("fixed QPACK test contract must hold");
    let mut warm = Vec::with_capacity(256);
    encoder
        .encode(
            0,
            [entry(&seed_key, &seed_val).expect("field fixture")],
            &mut warm,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());

    let mut output = b"prefix".to_vec();
    let failed = Tracker::fail_after(0, || {
        encoder.encode(
            0,
            [literal!(entry(b"x-atomic", b"uncommitted"))],
            &mut output,
        )
    });
    assert_eq!(failed, Err(Error::Allocation(o3::alloc::Error::Exhausted)));
    assert_eq!(output, b"prefix");
    assert!(encoder.take_wire().is_empty());

    encoder
        .encode(
            0,
            [literal!(entry(b"x-atomic", b"uncommitted"))],
            &mut output,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(output.len() > b"prefix".len());
    assert!(!encoder.take_wire().is_empty());
}

#[test]
fn wire_failure_is_atomic() {
    let seed_key = [b'w'; 96];
    let seed_val = [b'v'; 96];
    let name = [b'n'; 80];
    let value = [b'x'; 80];
    let mut encoder = Encoder::try_table(1024).expect("fixed QPACK test contract must hold");
    let mut warm = Vec::with_capacity(256);
    encoder
        .encode(
            0,
            [entry(&seed_key, &seed_val).expect("field fixture")],
            &mut warm,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());

    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(b"prefix");
    let failed = Tracker::fail_after(0, || {
        encoder.encode(
            0,
            [entry(&name, &value).expect("field fixture")],
            &mut output,
        )
    });
    assert_eq!(failed, Err(Error::Allocation(o3::alloc::Error::Exhausted)));
    assert_eq!(output, b"prefix");
    assert!(encoder.take_wire().is_empty());

    encoder
        .encode(
            0,
            [entry(&name, &value).expect("field fixture")],
            &mut output,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(output.len() > b"prefix".len());
    assert!(!encoder.take_wire().is_empty());
}

#[test]
fn large_delta_adds_owner() {
    const CAPACITY: usize = 96 * 1024;
    const VALUE_LEN: usize = 16 * 1024;

    let first = [b'a'; VALUE_LEN];
    let second = [b'b'; VALUE_LEN];
    let mut encoder = Encoder::try_table(CAPACITY).expect("fixed QPACK test contract must hold");
    let mut wire = Vec::with_capacity(VALUE_LEN + 64);
    encoder
        .encode(
            0,
            [entry(b"x-large-alpha", &first).expect("field fixture")],
            &mut wire,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());
    wire.clear();

    let (_, (allocs, _)) = Tracker::usage(|| {
        encoder
            .encode(
                0,
                [entry(b"x-large-bravo", &second).expect("field fixture")],
                &mut wire,
            )
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(allocs, 1);
}

#[test]
fn field_paths_are_packed() {
    let large = vec![b'x'; 64 * 1024];
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed QPACK test contract must hold");
    Send::new(&mut sender)
        .headers(
            Id::ZERO,
            [literal!(entry(b":status", b"200"))],
            Boundary::More,
        )
        .expect("prime the H3 output queue");
    drop(sender.events().poll_write().expect("primed output"));
    let ((), allocs) = Tracker::count(|| {
        assert!(
            Send::new(&mut sender)
                .headers(
                    Id::ZERO,
                    [entry(b"content-type", &large).expect("field fixture")],
                    Boundary::More,
                )
                .is_ok(),
            "fixed QPACK test contract must hold",
        );
    });
    assert_eq!(
        allocs, 1,
        "H3 must retain the QPACK field-line allocation instead of duplicating it"
    );
    assert!(matches!(
        sender.events().poll_write(),
        Ok(Some(write)) if matches!(write.payload, Some(WritePayload::Owned(_)))
    ));
    let (_, (allocs, _)) = Tracker::usage(|| {
        assert!(
            Send::new(&mut sender)
                .headers(
                    Id::new(4).expect("fixed stream id must be valid"),
                    [literal!(entry(b":status", b"200"))],
                    Boundary::More,
                )
                .is_ok(),
            "fixed QPACK test contract must hold",
        );
    });
    assert_eq!(
        allocs, 1,
        "a typical H3 header allocates only its owned QPACK field lines"
    );
    assert!(matches!(sender.events().poll_write(), Ok(Some(_))));

    let fields = [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b"x-direct", b"segmented")),
    ];
    let mut seg_enc = Encoder::new();
    let section = seg_enc
        .encode_for(0, fields)
        .expect("fixed QPACK test contract must hold");
    let line_ptr = section.lines().as_ptr();
    let mut split = Vec::new();
    section.write_prefix(&mut split);
    split.extend_from_slice(section.lines());
    let lines = section.into_lines();
    let mut flat = Vec::new();
    Encoder::new()
        .encode(0, fields, &mut flat)
        .expect("fixed QPACK test contract must hold");
    assert_eq!(lines.as_ptr(), line_ptr);
    assert_eq!(split, flat);

    let seed_key = [b'n'; 64];
    let seed_val = [b'v'; 256];
    let mut no_table = Encoder::new();
    let mut plain = Vec::with_capacity(512);
    no_table
        .encode(
            0,
            [entry(&seed_key, &seed_val).expect("field fixture")],
            &mut plain,
        )
        .expect("fixed QPACK test contract must hold");
    plain.clear();
    let (_, (allocs, _)) = Tracker::usage(|| {
        no_table
            .encode(0, [literal!(entry(b"x", b"y"))], &mut plain)
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(
        allocs, 0,
        "a disabled dynamic table must not allocate field owners"
    );

    let mut dynamic = Encoder::try_table(2048).expect("fixed QPACK test contract must hold");
    let mut active = Vec::with_capacity(512);
    dynamic
        .encode(
            0,
            [entry(&seed_key, &seed_val).expect("field fixture")],
            &mut active,
        )
        .expect("fixed QPACK test contract must hold");
    assert!(!dynamic.take_wire().is_empty());
    active.clear();
    let (_, (allocs, _)) = Tracker::usage(|| {
        dynamic
            .encode(0, [literal!(entry(b"x", b"y"))], &mut active)
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(
        allocs, 0,
        "a warmed dynamic insertion reuses its packed journal and inline instruction buffer"
    );

    let literals = [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b"x-direct", b"huffman value")),
    ];
    let mut encoder = Encoder::new();
    encoder.set_encoding(Encoding::Huffman);
    let mut literal = Vec::new();
    encoder
        .encode(0, literals, &mut literal)
        .expect("fixed QPACK test contract must hold");
    let mut decoder = decoder::Decoder::try_new(4096).expect("fixed QPACK test contract must hold");

    let (decoded, (allocs, _)) = Tracker::usage(|| {
        decoder
            .decode(&literal)
            .expect("fixed QPACK test contract must hold")
    });
    assert_eq!(allocs, 0, "decoded fields must reuse their bounded pool");
    assert!((decoded.iter().collect::<Vec<_>>()) == (literals.map(|field| field.as_record())));

    let field = literal!(entry(b"x-dynamic", b"retained once"));
    let mut encoder = Encoder::try_table(256).expect("fixed QPACK test contract must hold");
    encoder.set_blocked(1);
    let mut first = Vec::new();
    encoder
        .encode(0, [field], &mut first)
        .expect("fixed QPACK test contract must hold");
    let updates = encoder.take_wire();
    let mut blocked = Vec::new();
    encoder
        .encode(0, [field], &mut blocked)
        .expect("fixed QPACK test contract must hold");

    for received in [false, true] {
        let mut decoder =
            decoder::Decoder::try_dynamic(4096, 256).expect("fixed QPACK test contract must hold");
        if received {
            decoder
                .ingest(&updates)
                .expect("fixed QPACK test contract must hold");
        }
        let (decoded, allocs) = Tracker::count(|| decoder.decode(&blocked));
        assert_eq!(allocs, 0);
        if received {
            assert!(
                decoded
                    .expect("received dynamic field")
                    .iter()
                    .eq([field.as_record()])
            );
        } else {
            assert!(matches!(decoded, Err(decoder::Error::Blocked { .. })));
        }
    }

    let mut limited = decoder::Decoder::try_new(41).expect("fixed QPACK test contract must hold");
    let mut method = Vec::new();
    Encoder::new()
        .encode(0, [literal!(entry(b":method", b"GET"))], &mut method)
        .expect("fixed QPACK test contract must hold");
    let error = limited
        .decode(&method)
        .expect_err("field capacity must reject");
    assert_eq!(
        error,
        decoder::Error::FieldCapacity {
            available: 2,
            required: 3,
        }
    );
    assert_eq!(
        conn::Error::Qpack(error).error_code(),
        ErrorCode::ExcessiveLoad
    );

    let oversize = vec![b'a'; 4096];
    let mut encoder = Encoder::new();
    encoder.set_encoding(Encoding::Huffman);
    let mut encoded = Vec::new();
    encoder
        .encode(
            0,
            [entry(b"x-oversized", &oversize).expect("field fixture")],
            &mut encoded,
        )
        .expect("fixed QPACK test contract must hold");
    let mut limited = decoder::Decoder::try_new(64).expect("fixed QPACK test contract must hold");
    let (_, (allocs, _)) = Tracker::usage(|| {
        let error = limited
            .decode(&encoded)
            .expect_err("literal capacity must reject");
        assert_eq!(
            error,
            decoder::Error::LiteralCapacity {
                written: 21,
                additional: 1,
                maximum: 21,
            }
        );
        assert_eq!(
            conn::Error::Qpack(error).error_code(),
            ErrorCode::ExcessiveLoad
        );
    });
    assert_eq!(
        allocs, 0,
        "Huffman expansion must stop inside the bounded field-section slot"
    );
}

#[test]
fn retains_huffman_heads() {
    use sark_h3::{
        conn::{
            event::Event,
            io::{Receive, Step},
        },
        frame,
    };

    let values = [
        (b"alpha".as_slice(), b"bravo".as_slice()),
        (b"charlie".as_slice(), b"delta".as_slice()),
    ];
    let mut conn = Connection::with_role(Role::Server).expect("request connection");
    let mut encoder = Encoder::new();
    encoder.set_encoding(Encoding::Huffman);
    let mut retained = Vec::new();
    for (index, (first, second)) in values.iter().copied().enumerate() {
        let fields = [
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":authority", b"example.test")),
            literal!(entry(b":path", b"/literal")),
            entry(b"x-first", first).expect("field fixture"),
            entry(b"x-second", second).expect("field fixture"),
        ];
        let stream = Id::new(index as u64 * 4).expect("request stream");
        let mut block = Vec::new();
        encoder
            .encode(stream.get(), fields, &mut block)
            .expect("literal block");
        let mut wire = Vec::new();
        frame::Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).expect("headers frame");
        let (fields, (allocs, _)) = Tracker::usage(|| {
            assert!(
                Receive::new(&mut conn)
                    .borrowed(stream, &wire, Boundary::End)
                    .is_ok(),
                "request ingress",
            );
            loop {
                match conn.events().poll_token() {
                    Ok(Step::Item(Event::Headers { head, .. })) => break head.into_parts().0,
                    step => {
                        assert!(step.is_ok(), "request event");
                        assert!(!matches!(step, Ok(Step::Idle)), "headers pending");
                    }
                }
            }
        });
        assert_eq!(
            allocs,
            [4, 2][index],
            "borrowed ingress preserves cold and reused scratch allocation counts"
        );
        retained.push(fields);
    }
    for (fields, (first, second)) in retained.iter().zip(values) {
        assert_eq!(fields.get(b":authority"), Some(b"example.test".as_slice()));
        assert_eq!(fields.get(b":path"), Some(b"/literal".as_slice()));
        assert_eq!(fields.get(b"x-first"), Some(first));
        assert_eq!(fields.get(b"x-second"), Some(second));
    }
}
