mod instruction;
mod literal;
mod static_table;
mod table;

use std::ops::Range;

pub use instruction::{DecoderInstruction, EncoderInstruction};
use o3::buffer::{ByteSink, SharedPoolPlan, SpareWriter};
use sark_core::http::{
    DecodedFieldBlock, Field, FieldMatch, HeadBlock, HeadConsumer, HeadDecision, HeadDisposition,
    HeadPlan, HeadSection, KnownHeadName, OwnedField, PrefixedInt, PrefixedIntError,
};
use sark_core::pool::GrowingSharedPool;
pub use static_table::StaticTable;
pub use table::DynamicTable;

use literal::Literal;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecoderError {
    NeedMore,
    BadInteger,
    DynamicReference,
    InvalidReference,
    EncoderStream,
    DecoderStream,
    BadLiteral,
    Capacity,
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
pub enum DecodeOutcome<B = DecodedFieldBlock> {
    Ready {
        fields: B,
        required_insert_count: u64,
    },
    Blocked {
        required_insert_count: u64,
    },
}

pub(crate) enum MessageDecodeOutcome<S, B> {
    Ready {
        fields: B,
        selection: S,
        required_insert_count: u64,
    },
    Blocked {
        required_insert_count: u64,
    },
}

struct RawHeadConsumer;

impl HeadConsumer for RawHeadConsumer {
    const TAGGED: bool = false;

    fn disposition(
        &mut self,
        _name: &[u8],
        known: Option<KnownHeadName>,
        _retained: &[u8],
    ) -> HeadDecision {
        HeadDecision {
            disposition: HeadDisposition::Raw,
            name: known.unwrap_or(KnownHeadName::Regular),
        }
    }

    fn decoded(&mut self, _decision: HeadDecision, _field: Field<'_>, _retained: &[u8]) -> bool {
        true
    }

    fn committed(
        &mut self,
        _disposition: HeadDisposition,
        _value_range: Range<usize>,
        _retained: &[u8],
    ) {
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct EncodedSection {
    encoded_required_insert_count: PrefixedInt<8>,
    delta_base: PrefixedInt<7>,
    delta_base_negative: bool,
    field_lines: Vec<u8>,
}

impl EncodedSection {
    fn new(
        encoded_required_insert_count: u64,
        required_insert_count: u64,
        base: u64,
        field_lines: Vec<u8>,
    ) -> Self {
        let (delta_base, delta_base_negative) = if base >= required_insert_count {
            (base - required_insert_count, false)
        } else {
            (required_insert_count - base - 1, true)
        };
        Self {
            encoded_required_insert_count: PrefixedInt::new(encoded_required_insert_count),
            delta_base: PrefixedInt::new(delta_base),
            delta_base_negative,
            field_lines,
        }
    }

    pub fn prefix_len(&self) -> usize {
        self.encoded_required_insert_count.encoded_len() + self.delta_base.encoded_len()
    }

    pub fn encoded_len(&self) -> usize {
        self.prefix_len() + self.field_lines.len()
    }

    pub fn encode_prefix(&self, out: &mut impl Extend<u8>) {
        self.encoded_required_insert_count.encode(0, out);
        self.delta_base
            .encode(if self.delta_base_negative { 0x80 } else { 0 }, out);
    }

    pub fn field_lines(&self) -> &[u8] {
        &self.field_lines
    }

    pub fn into_field_lines(self) -> Vec<u8> {
        self.field_lines
    }
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

    pub(crate) fn set_max_dynamic_capacity(&mut self, max_capacity: u64) {
        self.table.set_max_capacity(max_capacity);
    }

    pub fn dynamic_capacity(&self) -> usize {
        self.table.capacity()
    }

    pub(crate) fn has_encoder_instructions(&self) -> bool {
        !self.encoder_stream.is_empty()
    }

    pub fn take_encoder_instructions(&mut self) -> Vec<u8> {
        let mut instructions = Vec::new();
        core::mem::swap(&mut instructions, &mut self.encoder_stream);
        instructions
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
        let section = self.encode_section(fields);
        section.encode_prefix(out);
        out.extend_from_slice(section.field_lines());
        self.recycle_section(section.into_field_lines());
    }

    pub(crate) fn recycle_section(&mut self, mut section: Vec<u8>) {
        section.clear();
        if section.capacity() > self.section.capacity() {
            self.section = section;
        }
    }

    pub fn encode_section<'a, I>(&mut self, fields: I) -> EncodedSection
    where
        I: IntoIterator<Item = Field<'a>>,
    {
        let base = self.table.insert_count();
        let mut reps = core::mem::take(&mut self.section);
        reps.clear();
        let mut required_insert_count = 0u64;
        let dynamic_enabled = self.table.capacity() != 0;

        for field in fields {
            let static_match = StaticTable::lookup(field);
            if let Some(index) = static_match.as_ref().and_then(|found| found.exact) {
                Self::encode_indexed_static(index, &mut reps);
                continue;
            }
            let known_received_count = self.known_received_count;
            let can_block = self.max_blocked_streams > 0;
            let dynamic_match = if dynamic_enabled {
                self.table.lookup(field, |absolute| {
                    absolute < known_received_count || can_block
                })
            } else {
                FieldMatch {
                    exact: None,
                    name: None,
                }
            };
            if let Some(abs) = dynamic_match.exact {
                required_insert_count = required_insert_count.max(abs + 1);
                Self::encode_dynamic_index(abs, base, &mut reps);
                continue;
            }
            if let Some(index) = static_match.map(|found| found.name) {
                Self::encode_literal_static_name(index, field.value, self.use_huffman, &mut reps);
                continue;
            }
            if let Some(abs) = dynamic_match.name {
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
            if dynamic_enabled && self.table.insert_field(field).ok().flatten().is_some() {
                EncoderInstruction::encode_literal_insert(field, &mut self.encoder_stream);
            }
        }

        let encoded_required_insert_count =
            Self::encode_required_insert_count(required_insert_count, self.table.max_entries());
        EncodedSection::new(
            encoded_required_insert_count,
            required_insert_count,
            base,
            reps,
        )
    }

    fn encode_literal_name(&self, field: Field<'_>, out: &mut Vec<u8>) {
        Literal::raw(field.name).encode::<3>(self.use_huffman, 0x20, out);
        Literal::raw(field.value).encode::<7>(self.use_huffman, 0, out);
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

    fn encode_dynamic_index(absolute: u64, base: u64, out: &mut Vec<u8>) {
        if absolute < base {
            Self::encode_indexed_dynamic(base - absolute - 1, out);
        } else {
            Self::encode_indexed_post_base(absolute - base, out);
        }
    }

    fn encode_indexed_dynamic(relative: u64, out: &mut Vec<u8>) {
        PrefixedInt::<6>::new(relative).encode(0x80, out);
    }

    fn encode_indexed_post_base(post_base: u64, out: &mut Vec<u8>) {
        PrefixedInt::<4>::new(post_base).encode(0x10, out);
    }

    fn encode_indexed_static(index: u64, out: &mut Vec<u8>) {
        PrefixedInt::<6>::new(index).encode(0xc0, out);
    }

    fn encode_literal_static_name(index: u64, value: &[u8], huffman: bool, out: &mut Vec<u8>) {
        PrefixedInt::<4>::new(index).encode(0x50, out);
        Literal::raw(value).encode::<7>(huffman, 0, out);
    }

    fn encode_literal_dynamic_name(
        absolute: u64,
        base: u64,
        value: &[u8],
        huffman: bool,
        out: &mut Vec<u8>,
    ) {
        if absolute < base {
            PrefixedInt::<4>::new(base - absolute - 1).encode(0x40, out);
        } else {
            PrefixedInt::<3>::new(absolute - base).encode(0x00, out);
        }
        Literal::raw(value).encode::<7>(huffman, 0, out);
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
    decoder_stream: Vec<u8>,
    field_pool: Option<GrowingSharedPool>,
    name_scratch: Vec<u8>,
    value_scratch: Vec<u8>,
}

impl Decoder {
    pub(crate) const DEFAULT_FIELD_BLOCKS: usize = 1024;

    pub fn new(max_field_section_size: usize) -> Self {
        Self::with_dynamic_capacity(max_field_section_size, 0)
    }

    pub fn with_dynamic_capacity(max_field_section_size: usize, max_table_capacity: usize) -> Self {
        let field_pool =
            SharedPoolPlan::new(Self::DEFAULT_FIELD_BLOCKS, max_field_section_size.max(1))
                .ok()
                .map(GrowingSharedPool::from_plan);
        Self::from_parts(max_field_section_size, max_table_capacity, field_pool)
    }

    pub(crate) fn with_pool_plan(
        max_field_section_size: usize,
        max_table_capacity: usize,
        field_pool: SharedPoolPlan,
    ) -> Self {
        Self::from_parts(
            max_field_section_size,
            max_table_capacity,
            Some(GrowingSharedPool::from_plan(field_pool)),
        )
    }

    fn from_parts(
        max_field_section_size: usize,
        max_table_capacity: usize,
        field_pool: Option<GrowingSharedPool>,
    ) -> Self {
        Self {
            max_field_section_size,
            table: DynamicTable::new(max_table_capacity),
            decoder_stream: Vec::new(),
            field_pool,
            name_scratch: Vec::new(),
            value_scratch: Vec::new(),
        }
    }

    pub fn dynamic_insert_count(&self) -> u64 {
        self.table.insert_count()
    }

    pub(crate) fn has_decoder_instructions(&self) -> bool {
        !self.decoder_stream.is_empty()
    }

    pub fn take_decoder_instructions(&mut self) -> Vec<u8> {
        let mut instructions = Vec::new();
        core::mem::swap(&mut instructions, &mut self.decoder_stream);
        instructions
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
                        let Some(field) = self.table.get_relative(name_index) else {
                            return Err(DecoderError::InvalidReference);
                        };
                        OwnedField {
                            name: field.name.to_vec(),
                            value,
                        }
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

    pub fn decode(&mut self, buf: &[u8]) -> Result<DecodedFieldBlock, DecoderError> {
        match self.decode_or_blocked(buf)? {
            DecodeOutcome::Ready { fields, .. } => Ok(fields),
            DecodeOutcome::Blocked { .. } => Err(DecoderError::DynamicReference),
        }
    }

    pub fn acknowledge_section(&mut self, stream_id: u64) {
        DecoderInstruction::SectionAcknowledgment { stream_id }.encode(&mut self.decoder_stream);
    }

    pub fn decode_or_blocked(&mut self, buf: &[u8]) -> Result<DecodeOutcome, DecoderError> {
        self.decode_or_blocked_inner(buf, &mut RawHeadConsumer)
    }

    pub(crate) fn decode_message_or_blocked<P: HeadPlan>(
        &mut self,
        buf: &[u8],
        request: bool,
        trailing: bool,
    ) -> Result<(MessageDecodeOutcome<P::Selection, P::Block>, bool), DecoderError> {
        let mut section = HeadSection::<P>::new(request, trailing);
        let outcome = self.decode_or_blocked_inner(buf, &mut section)?;
        let (outcome, valid) = match outcome {
            DecodeOutcome::Ready {
                fields,
                required_insert_count,
            } => {
                let (selection, valid) = section.finish();
                (
                    MessageDecodeOutcome::Ready {
                        fields,
                        selection,
                        required_insert_count,
                    },
                    valid,
                )
            }
            DecodeOutcome::Blocked {
                required_insert_count,
            } => (
                MessageDecodeOutcome::Blocked {
                    required_insert_count,
                },
                true,
            ),
        };
        Ok((outcome, valid))
    }

    fn decode_or_blocked_inner<C: HeadConsumer, B: HeadBlock>(
        &mut self,
        buf: &[u8],
        consumer: &mut C,
    ) -> Result<DecodeOutcome<B>, DecoderError> {
        let mut pos = 0;
        let (encoded_insert_count, n) = PrefixedInt::<8>::decode(buf)?;
        pos += n;
        let required_insert_count = Self::decode_required_insert_count(
            encoded_insert_count.get(),
            self.table.insert_count(),
            self.table.max_entries(),
        )?;
        let (delta_base, n) = PrefixedInt::<7>::decode(&buf[pos..])?;
        let sign = (buf[pos] & 0x80) != 0;
        pos += n;

        if required_insert_count > self.table.insert_count() {
            return Ok(DecodeOutcome::Blocked {
                required_insert_count,
            });
        }
        let base = Self::decode_base(required_insert_count, delta_base.get(), sign)?;

        let mut fields = self
            .field_pool
            .as_mut()
            .and_then(GrowingSharedPool::try_acquire)
            .ok_or(DecoderError::Capacity)?;
        let mut writer = fields.spare_writer();
        let mut total = 0usize;
        macro_rules! push_literal {
            ($name:expr, $known:expr) => {{
                let (value, n) = Literal::parse::<7>(&buf[pos..])?;
                pos += n;
                Self::push_field(
                    &mut writer,
                    &mut total,
                    $name,
                    value,
                    self.max_field_section_size,
                    $known,
                    consumer,
                    &mut self.name_scratch,
                    &mut self.value_scratch,
                )?;
            }};
        }
        while pos < buf.len() {
            let first = buf[pos];
            if first & 0x80 != 0 {
                let is_static = first & 0x40 != 0;
                let (index, n) = PrefixedInt::<6>::decode(&buf[pos..])?;
                pos += n;
                let index = index.get();
                let field = if is_static {
                    StaticTable::get(index).ok_or(DecoderError::InvalidReference)?
                } else {
                    self.table
                        .get_relative_to_base(base, index)
                        .ok_or(DecoderError::InvalidReference)?
                };
                Self::push_field(
                    &mut writer,
                    &mut total,
                    Literal::raw(field.name),
                    Literal::raw(field.value),
                    self.max_field_section_size,
                    is_static.then(|| StaticTable::known_name(index)),
                    consumer,
                    &mut self.name_scratch,
                    &mut self.value_scratch,
                )?;
                continue;
            }
            if first & 0xc0 == 0x40 {
                let is_static = first & 0x10 != 0;
                let (index, n) = PrefixedInt::<4>::decode(&buf[pos..])?;
                pos += n;
                let index = index.get();
                let (name, known) = if is_static {
                    (
                        StaticTable::name(index)
                            .map(Literal::raw)
                            .ok_or(DecoderError::InvalidReference)?,
                        Some(StaticTable::known_name(index)),
                    )
                } else {
                    (
                        Literal::raw(
                            self.table
                                .get_relative_to_base(base, index)
                                .ok_or(DecoderError::InvalidReference)?
                                .name,
                        ),
                        None,
                    )
                };
                push_literal!(name, known);
                continue;
            }
            if first & 0xf0 == 0x10 {
                let (index, n) = PrefixedInt::<4>::decode(&buf[pos..])?;
                pos += n;
                let field = self
                    .table
                    .get_absolute(base + index.get())
                    .ok_or(DecoderError::InvalidReference)?;
                Self::push_field(
                    &mut writer,
                    &mut total,
                    Literal::raw(field.name),
                    Literal::raw(field.value),
                    self.max_field_section_size,
                    None,
                    consumer,
                    &mut self.name_scratch,
                    &mut self.value_scratch,
                )?;
                continue;
            }
            if first & 0xf0 == 0x00 {
                if first & 0x08 != 0 {
                    return Err(DecoderError::BadLiteral);
                }
                let (index, n) = PrefixedInt::<3>::decode(&buf[pos..])?;
                pos += n;
                let name = Literal::raw(
                    self.table
                        .get_absolute(base + index.get())
                        .ok_or(DecoderError::InvalidReference)?
                        .name,
                );
                push_literal!(name, None);
                continue;
            }
            if first & 0xe0 != 0x20 {
                return Err(DecoderError::BadLiteral);
            }
            let (name, n) = Literal::parse::<3>(&buf[pos..])?;
            pos += n;
            push_literal!(name, None);
        }
        drop(writer);
        Ok(DecodeOutcome::Ready {
            fields: B::from_pooled(fields.freeze()),
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

    fn push_field<C: HeadConsumer>(
        writer: &mut SpareWriter<'_>,
        total: &mut usize,
        name: Literal<'_>,
        value: Literal<'_>,
        max: usize,
        known: Option<KnownHeadName>,
        consumer: &mut C,
        name_scratch: &mut Vec<u8>,
        value_scratch: &mut Vec<u8>,
    ) -> Result<(), DecoderError> {
        if C::TAGGED {
            return Self::push_planned_field(
                writer,
                total,
                name,
                value,
                max,
                known,
                consumer,
                name_scratch,
                value_scratch,
            );
        }
        Self::push_raw_field(writer, total, name, value, max, known, consumer)
    }

    fn push_raw_field<C: HeadConsumer>(
        writer: &mut SpareWriter<'_>,
        total: &mut usize,
        name: Literal<'_>,
        value: Literal<'_>,
        max: usize,
        known: Option<KnownHeadName>,
        consumer: &mut C,
    ) -> Result<(), DecoderError> {
        let remaining = max
            .checked_sub(total.checked_add(32).ok_or(DecoderError::BadInteger)?)
            .ok_or(DecoderError::BadLiteral)?;
        let field_start = writer.len();
        let decoded = (|| {
            writer
                .write_slice(&[0; 8])
                .map_err(|_| DecoderError::Capacity)?;
            let name_len = name.write_to(writer, remaining)?;
            let value_len = value.write_to(writer, remaining - name_len)?;
            let name_len = u32::try_from(name_len).map_err(|_| DecoderError::BadLiteral)?;
            let value_len = u32::try_from(value_len).map_err(|_| DecoderError::BadLiteral)?;
            Ok::<_, DecoderError>((name_len, value_len))
        })();
        let (name_len, value_len) = match decoded {
            Ok(lengths) => lengths,
            Err(error) => {
                writer.truncate(field_start);
                return Err(error);
            }
        };
        writer.as_mut_slice()[field_start..field_start + 4]
            .copy_from_slice(&name_len.to_ne_bytes());
        writer.as_mut_slice()[field_start + 4..field_start + 8]
            .copy_from_slice(&value_len.to_ne_bytes());
        let name_start = field_start + 8;
        let value_start = name_start + name_len as usize;
        let value_end = value_start + value_len as usize;
        let disposition = {
            let retained = writer.as_mut_slice();
            let field = Field::new(
                &retained[name_start..value_start],
                &retained[value_start..value_end],
            );
            let decision = consumer.disposition(field.name, known, retained);
            if consumer.decoded(decision, field, retained) {
                decision.disposition
            } else {
                HeadDisposition::Discard
            }
        };
        *total = Self::checked_total(*total, name_len as usize, value_len as usize, max)?;
        if disposition == HeadDisposition::Discard {
            writer.truncate(field_start);
        } else {
            debug_assert_eq!(disposition, HeadDisposition::Raw);
            consumer.committed(disposition, value_start..value_end, writer.as_mut_slice());
        }
        Ok(())
    }

    fn push_planned_field<C: HeadConsumer>(
        writer: &mut SpareWriter<'_>,
        total: &mut usize,
        name: Literal<'_>,
        value: Literal<'_>,
        max: usize,
        known: Option<KnownHeadName>,
        consumer: &mut C,
        name_scratch: &mut Vec<u8>,
        value_scratch: &mut Vec<u8>,
    ) -> Result<(), DecoderError> {
        let remaining = max
            .checked_sub(total.checked_add(32).ok_or(DecoderError::BadInteger)?)
            .ok_or(DecoderError::BadLiteral)?;
        let decoded_name;
        let name = if let Some(name) = name.raw_bytes() {
            if name.len() > remaining {
                return Err(DecoderError::BadLiteral);
            }
            name
        } else {
            decoded_name = name.decode_into(name_scratch, remaining)?;
            decoded_name.as_slice()
        };
        let decision = consumer.disposition(name, known, writer.as_mut_slice());
        match decision.disposition {
            HeadDisposition::Discard => {
                let decoded_value;
                let value = if let Some(value) = value.raw_bytes() {
                    if value.len() > remaining - name.len() {
                        return Err(DecoderError::BadLiteral);
                    }
                    value
                } else {
                    decoded_value = value.decode_into(value_scratch, remaining - name.len())?;
                    decoded_value.as_slice()
                };
                *total = Self::checked_total(*total, name.len(), value.len(), max)?;
                consumer.decoded(decision, Field::new(name, value), writer.as_mut_slice());
            }
            HeadDisposition::Skip => {
                let value_len = value.decoded_len(remaining - name.len())?;
                *total = Self::checked_total(*total, name.len(), value_len, max)?;
            }
            HeadDisposition::Tagged(tag) => {
                Self::push_planned_value(
                    writer, total, name, tag, value, remaining, max, decision, consumer,
                )?;
            }
            HeadDisposition::Raw => return Err(DecoderError::BadLiteral),
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn push_planned_value<C: HeadConsumer>(
        writer: &mut SpareWriter<'_>,
        total: &mut usize,
        name: &[u8],
        tag: sark_core::http::HeadTag,
        value: Literal<'_>,
        remaining: usize,
        max: usize,
        decision: HeadDecision,
        consumer: &mut C,
    ) -> Result<(), DecoderError> {
        let field_start = writer.len();
        let decoded = (|| {
            writer
                .write_slice(&[0; 6])
                .map_err(|_| DecoderError::Capacity)?;
            value.write_to(writer, remaining - name.len())
        })();
        let value_len = match decoded {
            Ok(len) => len,
            Err(error) => {
                writer.truncate(field_start);
                return Err(error);
            }
        };
        let prefix = tag.prefix(value_len).ok_or(DecoderError::BadLiteral)?;
        writer.as_mut_slice()[field_start..field_start + 6].copy_from_slice(&prefix);
        let value_start = field_start + 6;
        let value_end = value_start + value_len;
        *total = Self::checked_total(*total, name.len(), value_len, max)?;
        let valid = {
            let retained = writer.as_mut_slice();
            consumer.decoded(
                decision,
                Field::new(name, &retained[value_start..value_end]),
                retained,
            )
        };
        if valid {
            consumer.committed(
                decision.disposition,
                value_start..value_end,
                writer.as_mut_slice(),
            );
        } else {
            writer.truncate(field_start);
        }
        Ok(())
    }

    fn checked_total(
        total: usize,
        name_len: usize,
        value_len: usize,
        max: usize,
    ) -> Result<usize, DecoderError> {
        let total = total
            .checked_add(name_len)
            .and_then(|total| total.checked_add(value_len))
            .and_then(|total| total.checked_add(32))
            .ok_or(DecoderError::BadInteger)?;
        if total > max {
            return Err(DecoderError::BadLiteral);
        }
        Ok(total)
    }
}
