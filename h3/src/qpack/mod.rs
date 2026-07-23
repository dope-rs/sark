mod instruction;
mod static_table;
mod table;

pub use instruction::{DecoderInstruction, EncoderInstruction};
use sark_core::http::{Field, HpackHuffman, OwnedField, PrefixedInt, PrefixedIntError};
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

impl From<PrefixedIntError> for DecoderError {
    fn from(error: PrefixedIntError) -> Self {
        match error {
            PrefixedIntError::NeedMore => Self::NeedMore,
            PrefixedIntError::Overflow => Self::BadInteger,
        }
    }
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
    section: Vec<u8>,
    known_received_count: u64,
    max_blocked_streams: u64,
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            use_huffman: false,
            table: DynamicTable::new(0),
            encoder_stream: Vec::new(),
            section: Vec::new(),
            known_received_count: 0,
            max_blocked_streams: 0,
        }
    }

    pub fn with_dynamic_capacity(max_table_capacity: usize) -> Self {
        Self {
            use_huffman: false,
            table: DynamicTable::new(max_table_capacity),
            encoder_stream: Vec::new(),
            section: Vec::new(),
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
        let base = self.table.insert_count();
        let mut reps = core::mem::take(&mut self.section);
        reps.clear();
        let mut required_insert_count = 0u64;

        for field in fields {
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
        reps.clear();
        self.section = reps;
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
