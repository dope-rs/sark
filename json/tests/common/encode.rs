use std::{cell::Cell, iter, ops::ControlFlow};

use o3::buffer::write::Sink;
use sark_json::writing::{Failure, Size, TryEncode, Writer};

pub const ADJACENT: (&str, &[u8]) = (
    "\0\u{8}\u{c}\n\r\t\"\\\u{1f}é",
    r#""\u0000\b\f\n\r\t\"\\\u001fé""#.as_bytes(),
);

pub struct Length<const N: usize>;

pub struct Quoted {
    pub input: String,
    pub output: Vec<u8>,
}

impl Quoted {
    pub fn try_new(value: char, offset: usize) -> Option<Self> {
        let suffix = 23usize.checked_sub(offset)?;
        let mut input = "a".repeat(offset);
        let mut scalar = [0; 4];
        let scalar = value.encode_utf8(&mut scalar);
        let mut output = Vec::new();
        output.push(b'"');
        output.extend_from_slice(input.as_bytes());
        input.push_str(scalar);
        input.extend(iter::repeat_n('a', suffix));
        match value {
            '"' => output.extend_from_slice(b"\\\""),
            '\\' => output.extend_from_slice(b"\\\\"),
            '\n' => output.extend_from_slice(b"\\n"),
            '\r' => output.extend_from_slice(b"\\r"),
            '\t' => output.extend_from_slice(b"\\t"),
            '\u{8}' => output.extend_from_slice(b"\\b"),
            '\u{c}' => output.extend_from_slice(b"\\f"),
            '\0'..='\u{1f}' => {
                output.extend_from_slice(format!("\\u{:04x}", value as u32).as_bytes())
            }
            _ => output.extend_from_slice(scalar.as_bytes()),
        }
        output.extend(iter::repeat_n(b'a', suffix));
        output.push(b'"');
        Some(Self { input, output })
    }
}

impl<const N: usize> Size for Length<N> {
    fn json_size(&self) -> usize {
        N
    }
}

impl<const N: usize> TryEncode for Length<N> {
    fn try_write<'a, W: Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        writer.put(b"x")
    }
}

pub struct Value<'a> {
    pub len: usize,
    pub sizes: &'a Cell<usize>,
    pub drops: &'a Cell<usize>,
}

impl Size for Value<'_> {
    fn json_size(&self) -> usize {
        self.sizes.set(self.sizes.get() + 1);
        self.len
    }
}

impl TryEncode for Value<'_> {
    fn try_write<'a, W: Sink>(
        &self,
        out: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        out.put(b"true")
    }
}

impl Drop for Value<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[sark_json::json(try_encode)]
pub struct Document<'a> {
    #[field(nested)]
    pub value: Value<'a>,
}
