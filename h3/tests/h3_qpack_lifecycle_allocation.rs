use core::slice;

use crate::common::qpack::Instruction;
pub mod common;

use dope_quic::wire::stream::Id;
use dope_test::alloc::Tracker;
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{Connection, event::Role, io::Receive, options::Config},
    frame::{STREAM_TYPE_CONTROL, STREAM_TYPE_QPACK_DECODER, STREAM_TYPE_QPACK_ENCODER},
    qpack::decoder,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn literal_commits_in_arena() {
    let mut decoder = decoder::Decoder::try_dynamic(1024, 256).expect("test decoder counts");
    let mut capacity = Vec::new();
    Instruction::Capacity(256)
        .try_encode(&mut capacity)
        .expect("QPACK fixture");
    decoder
        .ingest(&capacity)
        .expect("fixed QPACK test contract must hold");

    let mut input = Vec::new();
    Instruction::Insert {
        name: b"x-direct",
        value: b"arena",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");
    decoder
        .ingest(&input)
        .expect("fixed QPACK test contract must hold");

    let (used, (allocs, _)) = Tracker::usage(|| decoder.ingest(&input));
    assert_eq!(used, Ok(input.len()));
    assert_eq!(
        allocs, 0,
        "literal decode must write into the reserved table arena"
    );
}

#[test]
fn cold_fragments_no_alloc() {
    let mut decoder = decoder::Decoder::try_dynamic(1024, 256).expect("test decoder counts");
    let mut input = Vec::new();
    Instruction::Capacity(256)
        .try_encode(&mut input)
        .expect("QPACK fixture");
    Instruction::Insert {
        name: b"x-cold",
        value: b"fragmented",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");

    let (_, counts) = Tracker::usage(|| {
        for byte in &input {
            assert_eq!(decoder.ingest(slice::from_ref(byte)), Ok(1));
        }
    });
    assert_eq!(counts, (0, 0));
    assert_eq!(decoder.insert_count(), 1);
}

#[test]
fn cold_streams_no_alloc() {
    let mut conn = Connection::with_config(Role::Server, Config::default())
        .expect("fixed QPACK test contract must hold");
    let mut control = Vec::with_capacity(1);
    let mut encoder = Vec::with_capacity(1);
    let mut decoder = Vec::with_capacity(1);
    STREAM_TYPE_CONTROL.encode(&mut control).expect("varint");
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut encoder)
        .expect("varint");
    STREAM_TYPE_QPACK_DECODER
        .encode(&mut decoder)
        .expect("varint");
    let [control, encoder, decoder] = [control, encoder, decoder].map(|mut wire| {
        storage::Shared::try_from(&mut wire)
            .map(resident::Shared::from)
            .expect("fixed QPACK test contract must hold")
    });

    let (_, counts) = Tracker::usage(|| {
        Receive::new(&mut conn)
            .shared(Id::new(2).expect("stream id"), control, Boundary::More)
            .expect("fixed QPACK test contract must hold");
        Receive::new(&mut conn)
            .shared(Id::new(6).expect("stream id"), encoder, Boundary::More)
            .expect("fixed QPACK test contract must hold");
        Receive::new(&mut conn)
            .shared(Id::new(10).expect("stream id"), decoder, Boundary::More)
            .expect("fixed QPACK test contract must hold");
    });
    assert_eq!(counts, (0, 0));
}
