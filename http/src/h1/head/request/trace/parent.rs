use crate::h1::head::request::trace::Context;

pub struct Parent {
    pub(in crate::h1::head::request) context: Context,
}

impl Parent {
    pub const BYTES: usize = 55;
    pub const HEADER_BYTES: usize = b"traceparent: ".len() + Self::BYTES + 2;

    pub const fn from_ids(trace_id: &[u8; 16], span_id: &[u8; 8]) -> Self {
        Self {
            context: Context {
                trace_id: *trace_id,
                span_id: *span_id,
            },
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::BYTES] {
        let mut bytes = [0; Self::BYTES];
        bytes[..3].copy_from_slice(b"00-");
        Self::write_hex(&mut bytes[3..35], &self.context.trace_id);
        bytes[35] = b'-';
        Self::write_hex(&mut bytes[36..52], &self.context.span_id);
        bytes[52..].copy_from_slice(b"-01");
        bytes
    }

    fn write_hex<const N: usize>(output: &mut [u8], input: &[u8; N]) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, byte) in input.iter().copied().enumerate() {
            output[index * 2] = HEX[usize::from(byte >> 4)];
            output[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
        }
    }
}

const _: () = assert!(size_of::<Parent>() == 24);
