mod instruction;
mod static_table;
mod table;

pub use instruction::{DecoderInstruction, EncoderInstruction};
use sark_core::http::{Field, HpackHuffman, OwnedField};
pub use static_table::StaticTable;
pub use table::DynamicTable;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecoderError {
    NeedMore,
    BadInteger,
    DynamicReference,
    InvalidReference,
    EncoderStream,
    DecoderStream,
    BadLiteral,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeOutcome {
    Ready {
        fields: Vec<OwnedField>,
        required_insert_count: u64,
    },
    Blocked {
        required_insert_count: u64,
    },
}

pub struct Encoder {
    use_huffman: bool,
    table: DynamicTable,
    encoder_stream: Vec<u8>,
    known_received_count: u64,
    max_blocked_streams: u64,
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            use_huffman: false,
            table: DynamicTable::new(0),
            encoder_stream: Vec::new(),
            known_received_count: 0,
            max_blocked_streams: 0,
        }
    }

    pub fn with_dynamic_capacity(max_table_capacity: usize) -> Self {
        Self {
            use_huffman: false,
            table: DynamicTable::new(max_table_capacity),
            encoder_stream: Vec::new(),
            known_received_count: 0,
            max_blocked_streams: 0,
        }
    }

    pub fn set_huffman(&mut self, enabled: bool) {
        self.use_huffman = enabled;
    }

    pub fn set_dynamic_capacity(&mut self, capacity: usize) -> Result<(), DecoderError> {
        self.table.set_capacity(capacity)?;
        EncoderInstruction::SetCapacity(capacity as u64).encode(&mut self.encoder_stream);
        Ok(())
    }

    pub fn set_max_blocked_streams(&mut self, max_blocked_streams: u64) {
        self.max_blocked_streams = max_blocked_streams;
    }

    pub fn dynamic_capacity(&self) -> usize {
        self.table.capacity()
    }

    pub fn take_encoder_instructions(&mut self) -> Vec<u8> {
        core::mem::take(&mut self.encoder_stream)
    }

    pub fn ingest_decoder(&mut self, buf: &[u8]) -> Result<usize, DecoderError> {
        let mut consumed = 0usize;
        while consumed < buf.len() {
            let (instruction, n) = match DecoderInstruction::decode(&buf[consumed..]) {
                Ok(v) => v,
                Err(DecoderError::NeedMore) => break,
                Err(err) => return Err(err),
            };
            consumed += n;
            match instruction {
                DecoderInstruction::SectionAcknowledgment { stream_id: _ } => {
                    self.known_received_count =
                        self.known_received_count.max(self.table.insert_count());
                }
                DecoderInstruction::StreamCancellation { stream_id: _ } => {}
                DecoderInstruction::InsertCountIncrement { increment } => {
                    self.known_received_count = self
                        .known_received_count
                        .checked_add(increment)
                        .ok_or(DecoderError::BadInteger)?;
                    if self.known_received_count > self.table.insert_count() {
                        return Err(DecoderError::DecoderStream);
                    }
                }
            }
        }
        Ok(consumed)
    }

    pub fn encode<'a, I>(&mut self, fields: I, out: &mut Vec<u8>)
    where
        I: IntoIterator<Item = Field<'a>>,
    {
        let fields: Vec<Field<'a>> = fields.into_iter().collect();
        let base = self.table.insert_count();
        let mut reps = Vec::new();
        let mut required_insert_count = 0u64;

        for field in fields.iter().copied() {
            if let Some(index) = StaticTable::find(field) {
                Self::encode_indexed_static(index, &mut reps);
                continue;
            }
            if let Some(abs) = self.find_exact_for_reference(field) {
                required_insert_count = required_insert_count.max(abs + 1);
                Self::encode_dynamic_index(abs, base, &mut reps);
                continue;
            }
            if let Some(index) = StaticTable::find_name(field.name) {
                Self::encode_literal_static_name(index, field.value, self.use_huffman, &mut reps);
                continue;
            }
            if let Some(abs) = self.find_name_for_reference(field.name) {
                required_insert_count = required_insert_count.max(abs + 1);
                Self::encode_literal_dynamic_name(
                    abs,
                    base,
                    field.value,
                    self.use_huffman,
                    &mut reps,
                );
                continue;
            }

            self.encode_literal_name(field, &mut reps);
            let owned = OwnedField {
                name: field.name.to_vec(),
                value: field.value.to_vec(),
            };
            if self.table.insert(owned.clone()).ok().flatten().is_some() {
                EncoderInstruction::literal_insert(owned.name, owned.value)
                    .encode(&mut self.encoder_stream);
            }
        }

        let encoded_required_insert_count =
            Self::encode_required_insert_count(required_insert_count, self.table.max_entries());
        PrefixedInt::encode(encoded_required_insert_count, 8, 0, out);
        Self::encode_delta_base(required_insert_count, base, out);
        out.extend_from_slice(&reps);
    }

    fn find_exact_for_reference(&self, field: Field<'_>) -> Option<u64> {
        let abs = self.table.find_exact(field)?;
        self.can_reference(abs).then_some(abs)
    }

    fn find_name_for_reference(&self, name: &[u8]) -> Option<u64> {
        let abs = self.table.find_name(name)?;
        self.can_reference(abs).then_some(abs)
    }

    fn can_reference(&self, abs: u64) -> bool {
        abs < self.known_received_count || self.max_blocked_streams > 0
    }

    fn encode_literal_name(&self, field: Field<'_>, out: &mut Vec<u8>) {
        StringLiteral::encode(field.name, 3, self.use_huffman, 0x20, out);
        StringLiteral::encode(field.value, 7, self.use_huffman, 0, out);
    }

    fn encode_required_insert_count(required_insert_count: u64, max_entries: u64) -> u64 {
        if required_insert_count == 0 {
            return 0;
        }
        let full_range = 2 * max_entries;
        if full_range == 0 {
            return 0;
        }
        (required_insert_count % full_range) + 1
    }

    fn encode_delta_base(required_insert_count: u64, base: u64, out: &mut Vec<u8>) {
        if required_insert_count == 0 || base >= required_insert_count {
            PrefixedInt::encode(base.saturating_sub(required_insert_count), 7, 0, out);
        } else {
            PrefixedInt::encode(required_insert_count - base - 1, 7, 0x80, out);
        }
    }

    fn encode_dynamic_index(absolute: u64, base: u64, out: &mut Vec<u8>) {
        if absolute < base {
            Self::encode_indexed_dynamic(base - absolute - 1, out);
        } else {
            Self::encode_indexed_post_base(absolute - base, out);
        }
    }

    fn encode_indexed_dynamic(relative: u64, out: &mut Vec<u8>) {
        PrefixedInt::encode(relative, 6, 0x80, out);
    }

    fn encode_indexed_post_base(post_base: u64, out: &mut Vec<u8>) {
        PrefixedInt::encode(post_base, 4, 0x10, out);
    }

    fn encode_indexed_static(index: u64, out: &mut Vec<u8>) {
        PrefixedInt::encode(index, 6, 0xc0, out);
    }

    fn encode_literal_static_name(index: u64, value: &[u8], huffman: bool, out: &mut Vec<u8>) {
        PrefixedInt::encode(index, 4, 0x50, out);
        StringLiteral::encode(value, 7, huffman, 0, out);
    }

    fn encode_literal_dynamic_name(
        absolute: u64,
        base: u64,
        value: &[u8],
        huffman: bool,
        out: &mut Vec<u8>,
    ) {
        if absolute < base {
            PrefixedInt::encode(base - absolute - 1, 4, 0x40, out);
        } else {
            PrefixedInt::encode(absolute - base, 3, 0x00, out);
        }
        StringLiteral::encode(value, 7, huffman, 0, out);
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Decoder {
    max_field_section_size: usize,
    table: DynamicTable,
    scratch: Vec<OwnedField>,
    decoder_stream: Vec<u8>,
}

impl Decoder {
    pub fn new(max_field_section_size: usize) -> Self {
        Self::with_dynamic_capacity(max_field_section_size, 0)
    }

    pub fn with_dynamic_capacity(max_field_section_size: usize, max_table_capacity: usize) -> Self {
        Self {
            max_field_section_size,
            table: DynamicTable::new(max_table_capacity),
            scratch: Vec::new(),
            decoder_stream: Vec::new(),
        }
    }

    pub fn dynamic_insert_count(&self) -> u64 {
        self.table.insert_count()
    }

    pub fn take_decoder_instructions(&mut self) -> Vec<u8> {
        core::mem::take(&mut self.decoder_stream)
    }

    pub fn ingest_encoder(&mut self, buf: &[u8]) -> Result<usize, DecoderError> {
        let mut consumed = 0usize;
        while consumed < buf.len() {
            let (instruction, n) = match EncoderInstruction::decode(&buf[consumed..]) {
                Ok(v) => v,
                Err(DecoderError::NeedMore) => break,
                Err(err) => return Err(err),
            };
            consumed += n;
            match instruction {
                EncoderInstruction::SetCapacity(capacity) => {
                    let capacity =
                        usize::try_from(capacity).map_err(|_| DecoderError::BadInteger)?;
                    self.table.set_capacity(capacity)?;
                }
                EncoderInstruction::InsertWithLiteralName { name, value } => {
                    self.table.insert(OwnedField { name, value })?;
                    DecoderInstruction::InsertCountIncrement { increment: 1 }
                        .encode(&mut self.decoder_stream);
                }
                EncoderInstruction::InsertWithNameRef {
                    dynamic,
                    name_index,
                    value,
                } => {
                    let field = if dynamic {
                        let Some(mut field) = self.table.get_relative(name_index) else {
                            return Err(DecoderError::InvalidReference);
                        };
                        field.value = value;
                        field
                    } else {
                        let Some(name) = StaticTable::name(name_index) else {
                            return Err(DecoderError::InvalidReference);
                        };
                        OwnedField {
                            name: name.to_vec(),
                            value,
                        }
                    };
                    self.table.insert(field)?;
                    DecoderInstruction::InsertCountIncrement { increment: 1 }
                        .encode(&mut self.decoder_stream);
                }
                EncoderInstruction::Duplicate { index } => {
                    self.table.duplicate_relative(index)?;
                    DecoderInstruction::InsertCountIncrement { increment: 1 }
                        .encode(&mut self.decoder_stream);
                }
            }
        }
        Ok(consumed)
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<Vec<OwnedField>, DecoderError> {
        match self.decode_or_blocked(buf)? {
            DecodeOutcome::Ready { fields, .. } => Ok(fields),
            DecodeOutcome::Blocked { .. } => Err(DecoderError::DynamicReference),
        }
    }

    pub fn acknowledge_section(&mut self, stream_id: u64) {
        DecoderInstruction::SectionAcknowledgment { stream_id }.encode(&mut self.decoder_stream);
    }

    pub fn decode_or_blocked(&mut self, buf: &[u8]) -> Result<DecodeOutcome, DecoderError> {
        let mut pos = 0;
        let (encoded_insert_count, n) = PrefixedInt::decode(buf, 8)?;
        pos += n;
        let required_insert_count = Self::decode_required_insert_count(
            encoded_insert_count,
            self.table.insert_count(),
            self.table.max_entries(),
        )?;
        let (delta_base, n) = PrefixedInt::decode(&buf[pos..], 7)?;
        let sign = (buf[pos] & 0x80) != 0;
        pos += n;

        if required_insert_count > self.table.insert_count() {
            return Ok(DecodeOutcome::Blocked {
                required_insert_count,
            });
        }
        let base = Self::decode_base(required_insert_count, delta_base, sign)?;

        self.scratch.clear();
        let mut total = 0usize;
        while pos < buf.len() {
            let first = buf[pos];
            if first & 0x80 != 0 {
                let is_static = first & 0x40 != 0;
                let (index, n) = PrefixedInt::decode(&buf[pos..], 6)?;
                pos += n;
                let field = if is_static {
                    StaticTable::get(index).ok_or(DecoderError::InvalidReference)?
                } else {
                    self.table
                        .get_relative_to_base(base, index)
                        .ok_or(DecoderError::InvalidReference)?
                };
                total = Self::checked_total(total, &field, self.max_field_section_size)?;
                self.scratch.push(field);
                continue;
            }
            if first & 0xc0 == 0x40 {
                let is_static = first & 0x10 != 0;
                let (index, n) = PrefixedInt::decode(&buf[pos..], 4)?;
                pos += n;
                let name = if is_static {
                    StaticTable::name(index)
                        .ok_or(DecoderError::InvalidReference)?
                        .to_vec()
                } else {
                    self.table
                        .get_relative_to_base(base, index)
                        .ok_or(DecoderError::InvalidReference)?
                        .name
                };
                let (value, n) = StringLiteral::decode(&buf[pos..], 7)?;
                pos += n;
                let field = OwnedField { name, value };
                total = Self::checked_total(total, &field, self.max_field_section_size)?;
                self.scratch.push(field);
                continue;
            }
            if first & 0xf0 == 0x10 {
                let (index, n) = PrefixedInt::decode(&buf[pos..], 4)?;
                pos += n;
                let field = self
                    .table
                    .get_absolute(base + index)
                    .ok_or(DecoderError::InvalidReference)?;
                total = Self::checked_total(total, &field, self.max_field_section_size)?;
                self.scratch.push(field);
                continue;
            }
            if first & 0xf0 == 0x00 {
                if first & 0x08 != 0 {
                    return Err(DecoderError::BadLiteral);
                }
                let (index, n) = PrefixedInt::decode(&buf[pos..], 3)?;
                pos += n;
                let name = self
                    .table
                    .get_absolute(base + index)
                    .ok_or(DecoderError::InvalidReference)?
                    .name;
                let (value, n) = StringLiteral::decode(&buf[pos..], 7)?;
                pos += n;
                let field = OwnedField { name, value };
                total = Self::checked_total(total, &field, self.max_field_section_size)?;
                self.scratch.push(field);
                continue;
            }
            if first & 0xe0 != 0x20 {
                return Err(DecoderError::BadLiteral);
            }
            let (name, n) = StringLiteral::decode(&buf[pos..], 3)?;
            pos += n;
            let (value, n) = StringLiteral::decode(&buf[pos..], 7)?;
            pos += n;
            let field = OwnedField { name, value };
            total = Self::checked_total(total, &field, self.max_field_section_size)?;
            self.scratch.push(field);
        }
        Ok(DecodeOutcome::Ready {
            fields: core::mem::take(&mut self.scratch),
            required_insert_count,
        })
    }

    fn decode_required_insert_count(
        encoded_insert_count: u64,
        total_number_of_inserts: u64,
        max_entries: u64,
    ) -> Result<u64, DecoderError> {
        if encoded_insert_count == 0 {
            return Ok(0);
        }
        let full_range = 2 * max_entries;
        if full_range == 0 || encoded_insert_count > full_range {
            return Err(DecoderError::DynamicReference);
        }
        let max_value = total_number_of_inserts
            .checked_add(max_entries)
            .ok_or(DecoderError::BadInteger)?;
        let max_wrapped = (max_value / full_range) * full_range;
        let mut required_insert_count = max_wrapped
            .checked_add(encoded_insert_count)
            .and_then(|v| v.checked_sub(1))
            .ok_or(DecoderError::BadInteger)?;
        if required_insert_count > max_value {
            if required_insert_count <= full_range {
                return Err(DecoderError::DynamicReference);
            }
            required_insert_count -= full_range;
        }
        if required_insert_count == 0 {
            return Err(DecoderError::DynamicReference);
        }
        Ok(required_insert_count)
    }

    fn decode_base(
        required_insert_count: u64,
        delta_base: u64,
        sign: bool,
    ) -> Result<u64, DecoderError> {
        if sign {
            required_insert_count
                .checked_sub(delta_base)
                .and_then(|v| v.checked_sub(1))
                .ok_or(DecoderError::InvalidReference)
        } else {
            required_insert_count
                .checked_add(delta_base)
                .ok_or(DecoderError::BadInteger)
        }
    }

    fn checked_total(total: usize, field: &OwnedField, max: usize) -> Result<usize, DecoderError> {
        let total = total
            .checked_add(field.name.len() + field.value.len())
            .ok_or(DecoderError::BadInteger)?;
        if total > max {
            return Err(DecoderError::BadLiteral);
        }
        Ok(total)
    }
}

pub(super) struct StringLiteral;

impl StringLiteral {
    pub(super) fn encode(
        value: &[u8],
        prefix_bits: u8,
        huffman: bool,
        prefix_byte: u8,
        out: &mut Vec<u8>,
    ) {
        if huffman {
            let len = HpackHuffman::encoded_len(value);
            let huffman_bit = Self::huffman_bit(prefix_bits);
            PrefixedInt::encode(len as u64, prefix_bits, prefix_byte | huffman_bit, out);
            HpackHuffman::encode(value, out);
        } else {
            PrefixedInt::encode(value.len() as u64, prefix_bits, prefix_byte, out);
            out.extend_from_slice(value);
        }
    }

    pub(super) fn decode(buf: &[u8], prefix_bits: u8) -> Result<(Vec<u8>, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let huffman = (buf[0] & Self::huffman_bit(prefix_bits)) != 0;
        let (len, n) = PrefixedInt::decode(buf, prefix_bits)?;
        let len = usize::try_from(len).map_err(|_| DecoderError::BadInteger)?;
        let end = n.checked_add(len).ok_or(DecoderError::BadInteger)?;
        if buf.len() < end {
            return Err(DecoderError::NeedMore);
        }
        if huffman {
            let mut out = Vec::new();
            HpackHuffman::decode(&buf[n..end], &mut out).map_err(|_| DecoderError::BadLiteral)?;
            Ok((out, end))
        } else {
            Ok((buf[n..end].to_vec(), end))
        }
    }

    fn huffman_bit(prefix_bits: u8) -> u8 {
        if prefix_bits == 7 {
            0x80
        } else {
            1u8 << prefix_bits
        }
    }
}

pub(super) struct PrefixedInt;

impl PrefixedInt {
    pub(super) fn encode(value: u64, prefix_bits: u8, prefix_byte: u8, out: &mut Vec<u8>) {
        let max_prefix = (1u64 << prefix_bits) - 1;
        let mask = if prefix_bits == 8 {
            0
        } else {
            !((1u8 << prefix_bits).wrapping_sub(1))
        };
        let high = prefix_byte & mask;
        if value < max_prefix {
            out.push(high | value as u8);
            return;
        }
        out.push(high | max_prefix as u8);
        let mut remaining = value - max_prefix;
        while remaining >= 128 {
            out.push(((remaining & 0x7f) as u8) | 0x80);
            remaining >>= 7;
        }
        out.push(remaining as u8);
    }

    pub(super) fn decode(buf: &[u8], prefix_bits: u8) -> Result<(u64, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let max_prefix = (1u64 << prefix_bits) - 1;
        let mask = if prefix_bits == 8 {
            u8::MAX
        } else {
            max_prefix as u8
        };
        let first = (buf[0] & mask) as u64;
        if first < max_prefix {
            return Ok((first, 1));
        }
        let mut value = max_prefix;
        let mut shift = 0u32;
        let mut pos = 1usize;
        loop {
            if pos >= buf.len() {
                return Err(DecoderError::NeedMore);
            }
            let b = buf[pos];
            pos += 1;
            let chunk = (b & 0x7f) as u64;
            let shifted = chunk.checked_shl(shift).ok_or(DecoderError::BadInteger)?;
            value = value.checked_add(shifted).ok_or(DecoderError::BadInteger)?;
            if b & 0x80 == 0 {
                return Ok((value, pos));
            }
            shift = shift.checked_add(7).ok_or(DecoderError::BadInteger)?;
            if shift >= 64 {
                return Err(DecoderError::BadInteger);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_section_round_trips() {
        let mut encoder = Encoder::new();
        let mut block = Vec::new();
        encoder.encode(
            [
                Field::new(b":method", b"GET"),
                Field::new(b":path", b"/"),
                Field::new(b"user-agent", b"sark"),
            ],
            &mut block,
        );

        let decoded = Decoder::new(1024).decode(&block).unwrap();
        assert_eq!(decoded[0].name, b":method");
        assert_eq!(decoded[0].value, b"GET");
        assert_eq!(decoded[2].name, b"user-agent");
        assert_eq!(decoded[2].value, b"sark");
    }

    #[test]
    fn static_indexed_fields_round_trip() {
        let mut encoder = Encoder::new();
        let mut block = Vec::new();
        encoder.encode(
            [
                Field::new(b":method", b"GET"),
                Field::new(b":scheme", b"https"),
                Field::new(b":path", b"/"),
                Field::new(b"content-type", b"application/json"),
            ],
            &mut block,
        );

        let decoded = Decoder::new(1024).decode(&block).unwrap();
        assert_eq!(decoded[0].name, b":method");
        assert_eq!(decoded[0].value, b"GET");
        assert_eq!(decoded[3].name, b"content-type");
        assert_eq!(decoded[3].value, b"application/json");
        assert!(block.len() < 12);
    }

    #[test]
    fn literal_with_static_name_matches_rfc_example() {
        let mut encoder = Encoder::new();
        let mut block = Vec::new();
        encoder.encode([Field::new(b":path", b"/index.html")], &mut block);

        assert_eq!(
            block,
            [
                0x00, 0x00, 0x51, 0x0b, b'/', b'i', b'n', b'd', b'e', b'x', b'.', b'h', b't', b'm',
                b'l',
            ]
        );

        let decoded = Decoder::new(1024).decode(&block).unwrap();
        assert_eq!(decoded[0].name, b":path");
        assert_eq!(decoded[0].value, b"/index.html");
    }

    #[test]
    fn huffman_literal_value_round_trips() {
        let mut encoder = Encoder::new();
        encoder.set_huffman(true);
        let mut block = Vec::new();
        encoder.encode([Field::new(b"x-huff", b"www.example.com")], &mut block);

        assert!(block.iter().any(|b| (b & 0x80) != 0));
        let decoded = Decoder::new(1024).decode(&block).unwrap();
        assert_eq!(decoded[0].name, b"x-huff");
        assert_eq!(decoded[0].value, b"www.example.com");
    }

    #[test]
    fn huffman_bad_padding_is_rejected() {
        assert_eq!(
            StringLiteral::decode(&[0x81, 0x00], 7),
            Err(DecoderError::BadLiteral)
        );
    }

    #[test]
    fn static_table_rejects_bad_index() {
        let mut block = vec![0x00, 0x00];
        PrefixedInt::encode(StaticTable::LEN, 6, 0xc0, &mut block);

        assert_eq!(
            Decoder::new(1024).decode(&block),
            Err(DecoderError::InvalidReference)
        );
    }

    #[test]
    fn encoder_stream_instructions_populate_dynamic_table() {
        let mut encoder = Encoder::with_dynamic_capacity(256);
        encoder.set_dynamic_capacity(128).unwrap();
        let mut first = Vec::new();
        encoder.encode([Field::new(b"x-cache-key", b"abc")], &mut first);
        let instructions = encoder.take_encoder_instructions();

        let mut decoder = Decoder::with_dynamic_capacity(1024, 256);
        assert_eq!(
            decoder.ingest_encoder(&instructions).unwrap(),
            instructions.len()
        );
        assert_eq!(decoder.dynamic_insert_count(), 1);

        let decoder_acks = decoder.take_decoder_instructions();
        assert!(!decoder_acks.is_empty());
        assert_eq!(
            encoder.ingest_decoder(&decoder_acks).unwrap(),
            decoder_acks.len()
        );

        let mut second = Vec::new();
        encoder.encode([Field::new(b"x-cache-key", b"abc")], &mut second);
        let decoded = decoder.decode(&second).unwrap();
        assert_eq!(decoded[0].name, b"x-cache-key");
        assert_eq!(decoded[0].value, b"abc");
        assert!(second.len() < first.len());
    }

    #[test]
    fn required_insert_count_uses_wrapped_encoding() {
        assert_eq!(Encoder::encode_required_insert_count(0, 8), 0);
        assert_eq!(Encoder::encode_required_insert_count(1, 8), 2);
        assert_eq!(Encoder::encode_required_insert_count(17, 8), 2);
        assert_eq!(Decoder::decode_required_insert_count(2, 16, 8).unwrap(), 17);
    }

    #[test]
    fn decoder_reports_blocked_until_encoder_stream_catches_up() {
        let mut encoder = Encoder::with_dynamic_capacity(256);
        encoder.set_max_blocked_streams(1);
        encoder.set_dynamic_capacity(128).unwrap();

        let mut first = Vec::new();
        encoder.encode([Field::new(b"x-cache-key", b"abc")], &mut first);
        let encoder_stream = encoder.take_encoder_instructions();

        let mut blocked = Vec::new();
        encoder.encode([Field::new(b"x-cache-key", b"abc")], &mut blocked);

        let mut decoder = Decoder::with_dynamic_capacity(1024, 256);
        assert_eq!(
            decoder.decode_or_blocked(&blocked).unwrap(),
            DecodeOutcome::Blocked {
                required_insert_count: 1
            }
        );

        decoder.ingest_encoder(&encoder_stream).unwrap();
        let DecodeOutcome::Ready { fields, .. } = decoder.decode_or_blocked(&blocked).unwrap()
        else {
            panic!("blocked section should be ready after encoder stream insert");
        };
        assert_eq!(fields[0].name, b"x-cache-key");
        assert_eq!(fields[0].value, b"abc");
    }

    #[test]
    fn post_base_indexed_field_round_trips() {
        let mut encoder = Encoder::with_dynamic_capacity(256);
        encoder.set_max_blocked_streams(1);
        encoder.set_dynamic_capacity(128).unwrap();

        let mut block = Vec::new();
        encoder.encode(
            [
                Field::new(b"x-cache-key", b"abc"),
                Field::new(b"x-cache-key", b"abc"),
            ],
            &mut block,
        );
        let encoder_stream = encoder.take_encoder_instructions();

        assert_ne!(block[0], 0);
        assert_eq!(block[1] & 0x80, 0x80);
        assert!(block.iter().any(|b| b & 0xf0 == 0x10));

        let mut decoder = Decoder::with_dynamic_capacity(1024, 256);
        assert_eq!(
            decoder.decode_or_blocked(&block).unwrap(),
            DecodeOutcome::Blocked {
                required_insert_count: 1
            }
        );
        decoder.ingest_encoder(&encoder_stream).unwrap();
        let decoded = decoder.decode(&block).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].name, b"x-cache-key");
        assert_eq!(decoded[0].value, b"abc");
        assert_eq!(decoded[1].name, b"x-cache-key");
        assert_eq!(decoded[1].value, b"abc");
    }

    #[test]
    fn post_base_name_reference_round_trips() {
        let mut encoder = Encoder::with_dynamic_capacity(256);
        encoder.set_max_blocked_streams(1);
        encoder.set_dynamic_capacity(128).unwrap();

        let mut block = Vec::new();
        encoder.encode(
            [
                Field::new(b"x-cache-key", b"abc"),
                Field::new(b"x-cache-key", b"def"),
            ],
            &mut block,
        );
        let encoder_stream = encoder.take_encoder_instructions();

        assert_eq!(block[1] & 0x80, 0x80);
        assert!(block[2..].iter().any(|b| b & 0xf0 == 0x00));

        let mut decoder = Decoder::with_dynamic_capacity(1024, 256);
        decoder.ingest_encoder(&encoder_stream).unwrap();
        let decoded = decoder.decode(&block).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].name, b"x-cache-key");
        assert_eq!(decoded[0].value, b"abc");
        assert_eq!(decoded[1].name, b"x-cache-key");
        assert_eq!(decoded[1].value, b"def");
    }

    #[test]
    fn encoder_and_decoder_instructions_round_trip() {
        let mut bytes = Vec::new();
        EncoderInstruction::SetCapacity(64).encode(&mut bytes);
        EncoderInstruction::InsertWithLiteralName {
            name: b"x".to_vec(),
            value: b"y".to_vec(),
        }
        .encode(&mut bytes);
        let (first, n) = EncoderInstruction::decode(&bytes).unwrap();
        assert_eq!(first, EncoderInstruction::SetCapacity(64));
        let (second, m) = EncoderInstruction::decode(&bytes[n..]).unwrap();
        assert_eq!(
            second,
            EncoderInstruction::InsertWithLiteralName {
                name: b"x".to_vec(),
                value: b"y".to_vec(),
            }
        );
        assert_eq!(n + m, bytes.len());

        bytes.clear();
        DecoderInstruction::InsertCountIncrement { increment: 2 }.encode(&mut bytes);
        assert_eq!(
            DecoderInstruction::decode(&bytes).unwrap().0,
            DecoderInstruction::InsertCountIncrement { increment: 2 }
        );
    }
}
