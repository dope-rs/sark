use super::{DecoderError, PrefixedInt, literal::Literal};
use sark_core::http::Field;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncoderInstruction {
    SetCapacity(u64),
    InsertWithNameRef {
        dynamic: bool,
        name_index: u64,
        value: Vec<u8>,
    },
    InsertWithLiteralName {
        name: Vec<u8>,
        value: Vec<u8>,
    },
    Duplicate {
        index: u64,
    },
}

impl EncoderInstruction {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::SetCapacity(capacity) => PrefixedInt::<5>::new(*capacity).encode(0x20, out),
            Self::InsertWithNameRef {
                dynamic,
                name_index,
                value,
            } => {
                let prefix = if *dynamic { 0x80 } else { 0xc0 };
                PrefixedInt::<6>::new(*name_index).encode(prefix, out);
                Literal::raw(value).encode::<7>(false, 0, out);
            }
            Self::InsertWithLiteralName { name, value } => {
                Literal::raw(name).encode::<5>(false, 0x40, out);
                Literal::raw(value).encode::<7>(false, 0, out);
            }
            Self::Duplicate { index } => PrefixedInt::<5>::new(*index).encode(0, out),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let first = buf[0];
        if first & 0x80 != 0 {
            let dynamic = first & 0x40 == 0;
            let (name_index, mut pos) = PrefixedInt::<6>::decode(buf)?;
            let (value, n) = Literal::decode::<7>(&buf[pos..])?;
            pos += n;
            return Ok((
                Self::InsertWithNameRef {
                    dynamic,
                    name_index: name_index.get(),
                    value,
                },
                pos,
            ));
        }
        if first & 0xc0 == 0x40 {
            let (name, mut pos) = Literal::decode::<5>(buf)?;
            let (value, n) = Literal::decode::<7>(&buf[pos..])?;
            pos += n;
            return Ok((Self::InsertWithLiteralName { name, value }, pos));
        }
        if first & 0xe0 == 0x20 {
            let (capacity, n) = PrefixedInt::<5>::decode(buf)?;
            return Ok((Self::SetCapacity(capacity.get()), n));
        }
        let (index, n) = PrefixedInt::<5>::decode(buf)?;
        Ok((Self::Duplicate { index: index.get() }, n))
    }

    pub(super) fn encode_literal_insert(field: Field<'_>, out: &mut Vec<u8>) {
        Literal::raw(field.name).encode::<5>(false, 0x40, out);
        Literal::raw(field.value).encode::<7>(false, 0, out);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecoderInstruction {
    SectionAcknowledgment { stream_id: u64 },
    StreamCancellation { stream_id: u64 },
    InsertCountIncrement { increment: u64 },
}

impl DecoderInstruction {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::SectionAcknowledgment { stream_id } => {
                PrefixedInt::<7>::new(*stream_id).encode(0x80, out);
            }
            Self::StreamCancellation { stream_id } => {
                PrefixedInt::<6>::new(*stream_id).encode(0x40, out);
            }
            Self::InsertCountIncrement { increment } => {
                PrefixedInt::<6>::new(*increment).encode(0, out);
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let first = buf[0];
        if first & 0x80 != 0 {
            let (stream_id, n) = PrefixedInt::<7>::decode(buf)?;
            return Ok((
                Self::SectionAcknowledgment {
                    stream_id: stream_id.get(),
                },
                n,
            ));
        }
        if first & 0xc0 == 0x40 {
            let (stream_id, n) = PrefixedInt::<6>::decode(buf)?;
            return Ok((
                Self::StreamCancellation {
                    stream_id: stream_id.get(),
                },
                n,
            ));
        }
        let (increment, n) = PrefixedInt::<6>::decode(buf)?;
        let increment = increment.get();
        if increment == 0 {
            return Err(DecoderError::DecoderStream);
        }
        Ok((Self::InsertCountIncrement { increment }, n))
    }
}
