pub mod common;

use std::num::NonZero;

use common::{
    capacity::Fragments,
    hpack::{FIRST_DYNAMIC, GET, LATE_RESIZE, OVERSIZED_INDEXED, bound::Literal},
    protocol::{Session, rules::Wire},
};
use dope_test::alloc::Tracker;
use o3::buffer::storage::Shared;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{
        Error,
        config::{Config, Settings},
        machine::Connection,
    },
    frame::numbers::ErrorCode,
    hpack::decode,
    stream::Id,
};
use sark_http::field::metadata::{integer::PrefixedInt, strings::Encoding};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn refusal_avoids_scratch() {
    for field in OVERSIZED_INDEXED {
        for encoding in [Encoding::Plain, Encoding::Huffman] {
            for capacity in [0, 64] {
                let mut block = Vec::new();
                PrefixedInt::<5>::new(capacity).encode(0x20, &mut block);
                block.extend(
                    Literal::new(field.name, field.value)
                        .try_with(true, encoding)
                        .expect("literal"),
                );
                for (split, chunks) in [
                    (block.len(), usize::MAX),
                    (block.len(), 16),
                    (16, usize::MAX),
                ] {
                    let mut server = Connection::<ServerRole>::with_config(Config {
                        local_settings: Settings {
                            max_concurrent_streams: Some(0),
                            max_header_list_size: Some(match encoding {
                                Encoding::Plain => 512,
                                Encoding::Huffman => 256,
                            }),
                            ..Settings::DEFAULT
                        },
                        ..Config::default()
                    })
                    .expect("refusing connection");
                    Session::new(&mut server).try_prime().expect("preface");
                    let frames = Fragments::new(Id::FIRST_CLIENT, Boundary::End, &block)
                        .try_split(NonZero::new(split).expect("fragment"))
                        .expect("frames");
                    let owners = frames
                        .iter()
                        .flat_map(|frame| frame.chunks(chunks))
                        .map(Shared::try_copy)
                        .collect::<Result<Vec<_>, _>>()
                        .expect("owned frames");
                    let (result, count) = Tracker::count(|| {
                        owners
                            .into_iter()
                            .try_for_each(|owner| server.ingress().ingest_owned(owner))
                    });
                    result.expect("discard oversized indexed literal");
                    assert_eq!(count, 0);
                    assert_eq!(
                        Wire::new(server.output().first())
                            .try_reset()
                            .expect("reset"),
                        Some((Id::FIRST_CLIENT, ErrorCode::RefusedStream)),
                    );
                }
            }
        }
    }
}

#[test]
fn refusal_keeps_table() {
    for (suffix, expected) in [
        (FIRST_DYNAMIC, decode::Error::BadIndex),
        (LATE_RESIZE, decode::Error::LateResize),
    ] {
        let mut block = Vec::new();
        PrefixedInt::<5>::new(64).encode(0x20, &mut block);
        block.extend(Literal::new(GET[3].name, GET[3].value).encode_plain(true));
        block.extend_from_slice(FIRST_DYNAMIC);
        let field = OVERSIZED_INDEXED[1];
        block.extend(
            Literal::new(field.name, field.value)
                .try_encode(true)
                .expect("literal"),
        );
        block.extend_from_slice(suffix);
        for chunk in [block.len(), 16] {
            let mut server = Connection::<ServerRole>::with_config(Config {
                local_settings: Settings {
                    max_concurrent_streams: Some(0),
                    ..Settings::DEFAULT
                },
                ..Config::default()
            })
            .expect("refusing connection");
            Session::new(&mut server).try_prime().expect("preface");
            let frames = Fragments::new(Id::FIRST_CLIENT, Boundary::End, &block)
                .try_split(NonZero::new(chunk).expect("fragment"))
                .expect("frames");
            let result = frames
                .iter()
                .try_for_each(|frame| server.ingress().ingest(frame));
            assert_eq!(result, Err(Error::Hpack(expected)));
        }
    }
}

#[test]
fn refusal_checks_huffman() {
    let mut block = Vec::new();
    PrefixedInt::<5>::new(64).encode(0x20, &mut block);
    let field = OVERSIZED_INDEXED[1];
    block.extend(
        Literal::new(field.name, field.value)
            .try_bad_tail()
            .expect("literal"),
    );
    for (limit, chunk, expected) in [
        (
            None,
            block.len(),
            decode::Error::Encoding {
                consumed: 243,
                decoded: 384,
            },
        ),
        (
            None,
            16,
            decode::Error::Encoding {
                consumed: 10,
                decoded: 12,
            },
        ),
        (Some(256), 16, decode::Error::BadString),
    ] {
        let mut server = Connection::<ServerRole>::with_config(Config {
            local_settings: Settings {
                max_concurrent_streams: Some(0),
                max_header_list_size: limit,
                ..Settings::DEFAULT
            },
            ..Config::default()
        })
        .expect("refusing connection");
        Session::new(&mut server).try_prime().expect("preface");
        let frames = Fragments::new(Id::FIRST_CLIENT, Boundary::End, &block)
            .try_split(NonZero::new(chunk).expect("fragment"))
            .expect("frames");
        let result = frames
            .iter()
            .try_for_each(|frame| server.ingress().ingest(frame));
        assert_eq!(result, Err(Error::Hpack(expected)));
    }
}
