use sark_h2::hpack::{
    decode::{Decoder, Error, limit::Admission},
    encode::{self, Encoder},
};
use sark_http::field::metadata::map::record::Entry;

pub const GET_BAD_INDEX: &[u8] = &[0x82, 0xff, 0x7f];
pub const GET_PATH: &[u8] = &[0x82, 0x04, 0x01, b'/'];

pub const WIDE_INDEXED: Entry<'static> = Entry::new(b"x-name", b"0");

pub trait Prime {
    /// Retains 66 distinct fields so the oldest one has HPACK index 127.
    fn try_wide(&mut self) -> Result<(Vec<u8>, Vec<u8>), encode::Error>;
}

impl Prime for Encoder {
    fn try_wide(&mut self) -> Result<(Vec<u8>, Vec<u8>), encode::Error> {
        let mut values = Vec::new();
        for index in 0..66 {
            values.push(index.to_string());
        }
        let mut fields = super::GET.to_vec();
        for value in &values {
            fields.push(Entry::new(WIDE_INDEXED.name, value.as_bytes()));
        }
        let warm = super::Fields::new(&fields).try_with(self)?;
        let mut fields = super::GET.to_vec();
        fields.push(WIDE_INDEXED);
        Ok((warm, super::Fields::new(&fields).try_with(self)?))
    }
}

pub struct Repeated {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl Default for Repeated {
    fn default() -> Self {
        Self {
            name: vec![b'z'; 160],
            value: vec![b'a'; 300],
        }
    }
}

impl Repeated {
    pub fn try_encode(self) -> Result<Vec<u8>, super::Build> {
        let Self { name, value } = self;
        let headers = [
            Entry::new(b":method", b"GET"),
            Entry::new(&name, &value),
            Entry::new(&name, &value),
        ];
        let mut encoder = Encoder::try_new(4096).map_err(super::Build::Capacity)?;
        encoder.set_max_size(1024);
        super::Fields::new(&headers)
            .try_with(&mut encoder)
            .map_err(super::Build::Encode)
    }
}

pub struct Decoded {
    pub fields: super::Values,
    pub decoder: Decoder,
    pub admit: Admission,
}

impl Decoded {
    pub fn try_chunks<'a>(
        chunks: impl IntoIterator<Item = &'a [u8]>,
        decoder: Decoder,
    ) -> Result<Self, Error> {
        use sark_h2::hpack::decode::Block;
        let mut block = Block::from(decoder);
        let mut fields = Vec::new();
        for chunk in chunks {
            block = block.decode(chunk, &mut |name, value| {
                fields.push(super::Field::new(name, value).into_parts());
            })?;
        }
        let (decoder, admit) = block.finish()?;
        Ok(Self {
            fields,
            decoder,
            admit,
        })
    }
}
