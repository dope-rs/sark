use sark_http::h1::{chunk, codec::decode::chunks};

pub trait Commit {
    fn try_take<'a>(&mut self, input: &'a [u8]) -> Result<chunks::Event<'a>, chunks::Error>;
}

impl Commit for chunks::Decoder {
    fn try_take<'a>(&mut self, input: &'a [u8]) -> Result<chunks::Event<'a>, chunks::Error> {
        let plan = self.decode(input)?;
        Ok(plan.commit(self))
    }
}

pub const TRAILER_PREFIX: &[u8] = b"0\r\nx-pro";
pub const TRAILER_SUFFIX: &[u8] = b"of: yes\r\n\r\nNEXT";
pub const TRAILER_FIELD_SUFFIX: &[u8] = b"of: yes\r\n";

pub const DATA_BYTES: usize = 16 * 1024;
pub const ALLOCATION: &[u8] = b"3;part=1\r\nabc\r\n4\r\ndefg\r\n0\r\nx-proof: yes\r\n\r\n";

pub const SYNTAX_ERRORS: [(&[u8], chunk::Syntax); 8] = [
    (b"1;x@", chunk::Syntax::Name),
    (b"1;x y", chunk::Syntax::NameTail),
    (b"1;x=a@", chunk::Syntax::Token),
    (b"1;x=\"a\"z", chunk::Syntax::ValueEnd),
    (b"1 @", chunk::Syntax::SizeTail),
    (b"1;x=a @", chunk::Syntax::ValueTail),
    (b"1;x=\"\x7f", chunk::Syntax::Quoted),
    (b"1;x=\"\\\x7f", chunk::Syntax::Escape),
];

pub const FRAMING: [(&[u8], &[u8], usize); 2] = [
    (
        b"4;note=\"yes\"\r\nbody\r\n3\r\nend\r\n0\r\nContent-Digest: ok\r\n\r\n",
        b"bodyend",
        1024,
    ),
    (ALLOCATION, b"abcdefg", 7),
];
