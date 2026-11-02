use sark_http::field::{
    Writer,
    metadata::{huffman::source, integer},
};

use crate::hpack::encode::Mode;

pub(in crate::hpack) fn encode<W>(bytes: &[u8], mode: Mode, out: &mut W)
where
    W: Extend<u8> + Writer,
{
    match mode {
        Mode::Shortest => {
            if let Ok(measured) = source::Source::new(bytes).try_measure() {
                let len = measured.encoded_len();
                if len < bytes.len() {
                    integer::PrefixedInt::<7>::new(len as u64).encode(0x80, out);
                    measured.encode(out);
                    return;
                }
            }
        }
        Mode::Plain => {}
    }
    integer::PrefixedInt::<7>::new(bytes.len() as u64).encode(0, out);
    out.write_slice(bytes);
}
