use super::{DecoderError, PrefixedInt, StringLiteral};

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
            Self::SetCapacity(capacity) => PrefixedInt::encode(*capacity, 5, 0x20, out),
            Self::InsertWithNameRef {
                dynamic,
                name_index,
                value,
            } => {
                let prefix = if *dynamic { 0x80 } else { 0xc0 };
                PrefixedInt::encode(*name_index, 6, prefix, out);
                StringLiteral::encode(value, 7, false, 0, out);
            }
            Self::InsertWithLiteralName { name, value } => {
                StringLiteral::encode(name, 5, false, 0x40, out);
                StringLiteral::encode(value, 7, false, 0, out);
            }
            Self::Duplicate { index } => PrefixedInt::encode(*index, 5, 0, out),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let first = buf[0];
        if first & 0x80 != 0 {
            let dynamic = first & 0x40 == 0;
            let (name_index, mut pos) = PrefixedInt::decode(buf, 6)?;
            let (value, n) = StringLiteral::decode(&buf[pos..], 7)?;
            pos += n;
            return Ok((
                Self::InsertWithNameRef {
                    dynamic,
                    name_index,
                    value,
                },
                pos,
            ));
        }
        if first & 0xc0 == 0x40 {
            let (name, mut pos) = StringLiteral::decode(buf, 5)?;
            let (value, n) = StringLiteral::decode(&buf[pos..], 7)?;
            pos += n;
            return Ok((Self::InsertWithLiteralName { name, value }, pos));
        }
        if first & 0xe0 == 0x20 {
            let (capacity, n) = PrefixedInt::decode(buf, 5)?;
            return Ok((Self::SetCapacity(capacity), n));
        }
        let (index, n) = PrefixedInt::decode(buf, 5)?;
        Ok((Self::Duplicate { index }, n))
    }

    pub(super) fn literal_insert(name: Vec<u8>, value: Vec<u8>) -> Self {
        Self::InsertWithLiteralName { name, value }
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
                PrefixedInt::encode(*stream_id, 7, 0x80, out);
            }
            Self::StreamCancellation { stream_id } => {
                PrefixedInt::encode(*stream_id, 6, 0x40, out);
            }
            Self::InsertCountIncrement { increment } => {
                PrefixedInt::encode(*increment, 6, 0, out);
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let first = buf[0];
        if first & 0x80 != 0 {
            let (stream_id, n) = PrefixedInt::decode(buf, 7)?;
            return Ok((Self::SectionAcknowledgment { stream_id }, n));
        }
        if first & 0xc0 == 0x40 {
            let (stream_id, n) = PrefixedInt::decode(buf, 6)?;
            return Ok((Self::StreamCancellation { stream_id }, n));
        }
        let (increment, n) = PrefixedInt::decode(buf, 6)?;
        if increment == 0 {
            return Err(DecoderError::DecoderStream);
        }
        Ok((Self::InsertCountIncrement { increment }, n))
    }
}
