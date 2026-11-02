use sark_h2::hpack::{
    capacity,
    decode::{Decoder, Error, limit::Admission},
    encode::{self, Encoder},
};
use sark_http::field::{
    Indexing,
    metadata::{
        huffman::source::Source,
        integer::PrefixedInt,
        map::record::{Entry, Span},
    },
};

type Values = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug)]
pub enum CollectError {
    Decode(Error),
    Exceeded,
}

pub struct Encoded<'a> {
    bytes: &'a [u8],
}

impl<'a> Encoded<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn try_collect(self, decoder: Decoder) -> Result<(Decoder, Values), CollectError> {
        let mut fields = Vec::new();
        let (decoder, admission) = decoder
            .decode_limit(self.bytes, |name, value| {
                fields.push(Field::new(name, value).into_parts());
            })
            .map_err(CollectError::Decode)?;
        match admission {
            Admission::Admitted => Ok((decoder, fields)),
            Admission::Exceeded => Err(CollectError::Exceeded),
        }
    }
}

pub struct Field<'a> {
    name: Span<'a>,
    value: Span<'a>,
}

impl<'a> Field<'a> {
    pub const fn new(name: Span<'a>, value: Span<'a>) -> Self {
        Self { name, value }
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (
            self.name.iter().copied().collect(),
            self.value.iter().copied().collect(),
        )
    }
}

pub mod bound;
pub mod stream;

pub const GET: [Entry<'static>; 4] = [
    Entry::new(b":method", b"GET"),
    Entry::new(b":scheme", b"http"),
    Entry::new(b":path", b"/"),
    Entry::new(b":authority", b"x"),
];

pub const SHORTEST_LITERALS: [(Entry<'static>, &[u8]); 3] = [
    (
        Entry::new(b":authority", b"").with_index(Indexing::Never),
        b"\x11\x00",
    ),
    (Entry::new(b":authority", b"\xff"), b"\x01\x01\xff"),
    (
        Entry::new(b":authority", b"www.example.com"),
        b"\x01\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff",
    ),
];

pub const INDEXED_NAMES: &[u8] = b"\x44\x05/sync\x41\x09localhost\x7e\x04next";
pub const INDEXED_FIELDS: [Entry<'static>; 3] = [
    Entry::new(b":path", b"/sync"),
    Entry::new(b":authority", b"localhost"),
    Entry::new(b":authority", b"next"),
];
pub const INDEXED_AUTHORITY: &[u8] = b"\x41\x01x";
pub const MISSING_INDEXED_NAME: &[u8] = b"\x7e\x01x";
pub const FIRST_DYNAMIC: &[u8] = b"\xbe";
pub const LATE_RESIZE: &[u8] = b"\x20";
pub const OVERSIZED_INDEXED: [Entry<'static>; 2] = [
    Entry::new(&[b'n'; 64], b"x"),
    Entry::new(b"x", &[b'a'; 384]),
];

pub const DYNAMIC_REFERENCES: [Entry<'static>; 7] = [
    GET[0],
    GET[1],
    GET[2],
    GET[3],
    Entry::new(b"x-a", b"v"),
    Entry::new(b"x-b", b"v"),
    Entry::new(b"x-a", b"v"),
];

pub const RESIZE_128_GET: &[u8] = &[0x3f, 0x61, 0x82];
pub const RESIZE_64_128_GET: &[u8] = &[0x3f, 0x21, 0x3f, 0x61, 0x82];

pub struct Fields<'a> {
    entries: &'a [Entry<'a>],
}

impl<'a> Fields<'a> {
    pub const fn new(entries: &'a [Entry<'a>]) -> Self {
        Self { entries }
    }

    pub fn try_encode(self) -> Result<Vec<u8>, Build> {
        let mut encoder = Encoder::try_new(4096).map_err(Build::Capacity)?;
        self.try_with(&mut encoder).map_err(Build::Encode)
    }

    pub fn try_with(self, encoder: &mut Encoder) -> Result<Vec<u8>, encode::Error> {
        let mut out = Vec::new();
        encoder.try_encode(self.entries.iter().copied(), &mut out)?;
        Ok(out)
    }

    pub fn encode_never(self) -> Vec<u8> {
        let mut out = Vec::new();
        for field in self.entries {
            out.push(0x10);
            for bytes in [field.name, field.value] {
                if let Ok(measured) = Source::new(bytes).try_measure() {
                    let len = measured.encoded_len();
                    if len < bytes.len() {
                        PrefixedInt::<7>::new(len as u64).encode(0x80, &mut out);
                        measured.encode(&mut out);
                        continue;
                    }
                }
                PrefixedInt::<7>::new(bytes.len() as u64).encode(0, &mut out);
                out.extend_from_slice(bytes);
            }
        }
        out
    }
}

pub const MALFORMED_DROPPED: [Entry<'static>; 5] = [
    Entry::new(b":method", b"GET"),
    Entry::new(b":scheme", b"http"),
    Entry::new(b":authority", b"x"),
    Entry::new(b":path", b"/"),
    Entry::new(b"x-dropped", b"aaaaaaaaaaaa "),
];

#[derive(Debug)]
pub enum Build {
    Capacity(capacity::Error),
    Encode(encode::Error),
}
