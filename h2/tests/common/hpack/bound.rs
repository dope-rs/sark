use sark_http::field::metadata::{
    huffman::source::{Overflow, Source},
    integer::PrefixedInt,
    strings::Encoding,
};

pub struct String<'a> {
    bytes: &'a [u8],
    encoding: Encoding,
}

impl<'a> String<'a> {
    pub const fn new(bytes: &'a [u8], encoding: Encoding) -> Self {
        Self { bytes, encoding }
    }

    pub fn try_encode(self, out: &mut Vec<u8>) -> Result<(), Overflow> {
        match self.encoding {
            Encoding::Huffman => {
                let measured = Source::new(self.bytes).try_measure()?;
                PrefixedInt::<7>::new(measured.encoded_len() as u64).encode(0x80, out);
                measured.encode(out);
            }
            Encoding::Plain => Self::plain(self.bytes, out),
        }
        Ok(())
    }

    fn plain(bytes: &[u8], out: &mut Vec<u8>) {
        PrefixedInt::<7>::new(bytes.len() as u64).encode(0x00, out);
        out.extend_from_slice(bytes);
    }
}

pub struct Literal<'a> {
    name: &'a [u8],
    value: &'a [u8],
}

#[derive(Debug)]
pub enum TailError {
    Huffman(Overflow),
    Length { encoded: usize },
}

impl<'a> Literal<'a> {
    pub const fn new(name: &'a [u8], value: &'a [u8]) -> Self {
        Self { name, value }
    }

    pub fn try_encode(self, indexing: bool) -> Result<Vec<u8>, Overflow> {
        self.try_with(indexing, Encoding::Huffman)
    }

    pub fn encode_plain(self, indexing: bool) -> Vec<u8> {
        let mut block = vec![if indexing { 0x40 } else { 0x00 }];
        String::plain(self.name, &mut block);
        String::plain(self.value, &mut block);
        block
    }

    pub fn try_bad_tail(self) -> Result<Vec<u8>, TailError> {
        let mut block = vec![0x40];
        String::plain(self.name, &mut block);
        let measured = Source::new(self.value)
            .try_measure()
            .map_err(TailError::Huffman)?;
        let len = measured.encoded_len();
        let len = len
            .checked_add(4)
            .ok_or(TailError::Length { encoded: len })?;
        PrefixedInt::<7>::new(len as u64).encode(0x80, &mut block);
        measured.encode(&mut block);
        block.extend_from_slice(&[0xff; 4]);
        Ok(block)
    }

    pub fn try_with(self, indexing: bool, encoding: Encoding) -> Result<Vec<u8>, Overflow> {
        let mut block = vec![if indexing { 0x40 } else { 0x00 }];
        String::new(self.name, encoding).try_encode(&mut block)?;
        String::new(self.value, encoding).try_encode(&mut block)?;
        Ok(block)
    }
}
