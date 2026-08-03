mod dynamic_table;
mod literal;
mod static_table;

use dynamic_table::DynamicTable;
use literal::Literal;
pub type Header<'a> = sark_core::http::Field<'a>;
pub type OwnedHeader = sark_core::http::OwnedField;
pub type HeaderBlock = sark_core::http::PooledFieldBlock;
use sark_core::http::{HpackHuffmanDecoder, PrefixedInt, PrefixedIntError};
use static_table::StaticTable;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecoderError {
    NeedMore,
    BadIndex,
    BadInteger,
    BadString,
    BadDynSizeUpdate,
}

impl From<PrefixedIntError> for DecoderError {
    fn from(error: PrefixedIntError) -> Self {
        match error {
            PrefixedIntError::NeedMore => Self::NeedMore,
            PrefixedIntError::Overflow => Self::BadInteger,
        }
    }
}

pub struct Encoder {
    dyn_table: DynamicTable,
    max_size_setting: usize,
    pending_size_update: Option<usize>,
    pending_reset: bool,
    use_huffman: bool,
}

impl Encoder {
    pub fn new(max_dyn_size: usize) -> Self {
        Self {
            dyn_table: DynamicTable::new(max_dyn_size),
            max_size_setting: max_dyn_size,
            pending_size_update: None,
            pending_reset: false,
            use_huffman: true,
        }
    }

    pub fn set_max_size(&mut self, n: usize) {
        if self
            .pending_size_update
            .is_some_and(|previous| n > previous)
        {
            self.dyn_table.set_max(0);
            self.pending_reset = true;
        }
        self.max_size_setting = n;
        self.dyn_table.set_max(n);
        self.pending_size_update = Some(n);
    }

    pub fn set_huffman(&mut self, enabled: bool) {
        self.use_huffman = enabled;
    }

    pub fn encode<'a, I>(&mut self, headers: I, out: &mut Vec<u8>)
    where
        I: IntoIterator<Item = Header<'a>>,
    {
        if let Some(n) = self.pending_size_update.take() {
            let reset = core::mem::take(&mut self.pending_reset);
            if reset {
                PrefixedInt::<5>::new(0).encode(0x20, out);
            }
            if !reset || n != 0 {
                PrefixedInt::<5>::new(n as u64).encode(0x20, out);
            }
        }
        for h in headers {
            self.encode_one(h, out);
        }
    }

    pub(crate) fn discard_block(&mut self) {
        self.dyn_table.set_max(0);
        self.dyn_table.set_max(self.max_size_setting);
        self.pending_size_update = Some(self.max_size_setting);
        self.pending_reset = true;
    }

    pub fn encode_one(&mut self, h: Header<'_>, out: &mut Vec<u8>) {
        let static_match = StaticTable::lookup(h.name, h.value);
        if let Some(index) = static_match.as_ref().and_then(|found| found.exact) {
            PrefixedInt::<7>::new(index as u64).encode(0x80, out);
            return;
        }
        let dynamic_match = self.dyn_table.lookup(h.name, h.value);
        if let Some(dyn_idx) = dynamic_match.exact {
            let absolute = StaticTable::LEN + 1 + dyn_idx;
            PrefixedInt::<7>::new(absolute as u64).encode(0x80, out);
            return;
        }
        let name_idx = static_match
            .map(|found| found.name)
            .or_else(|| dynamic_match.name.map(|i| StaticTable::LEN + 1 + i));
        match name_idx {
            Some(idx) => {
                PrefixedInt::<6>::new(idx as u64).encode(0x40, out);
            }
            None => {
                out.push(0x40);
                Literal::new(h.name).encode(self.use_huffman, out);
            }
        }
        Literal::new(h.value).encode(self.use_huffman, out);
        self.dyn_table.insert(h.name, h.value);
    }
}

pub struct Decoder {
    dyn_table: DynamicTable,
    max_size_setting: usize,
    max_header_list_size: Option<usize>,
    name_scratch: Vec<u8>,
    value_scratch: Vec<u8>,
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
// HPACK literals are capped below usize::MAX, so the sentinel is equivalent
// to both an absent limit and the largest representable configured limit.
struct Limit(usize);

impl Limit {
    const UNBOUNDED: Self = Self(usize::MAX);

    const fn new(limit: Option<usize>) -> Self {
        match limit {
            Some(limit) => Self(limit),
            None => Self::UNBOUNDED,
        }
    }

    fn option(self) -> Option<usize> {
        (self != Self::UNBOUNDED).then_some(self.0)
    }

    fn allows(self, len: usize) -> bool {
        self == Self::UNBOUNDED || len <= self.0
    }

    fn saturating_sub(self, amount: usize) -> Self {
        if self == Self::UNBOUNDED {
            self
        } else {
            Self(self.0.saturating_sub(amount))
        }
    }

    fn max(self, bounded: usize) -> Self {
        if self == Self::UNBOUNDED {
            self
        } else {
            Self(self.0.max(bounded))
        }
    }
}

struct HeaderListBudget {
    max: Limit,
    used: usize,
    exceeded: bool,
    allow_size_update: bool,
}

impl HeaderListBudget {
    const fn new(max: Option<usize>) -> Self {
        Self {
            max: Limit::new(max),
            used: 0,
            exceeded: false,
            allow_size_update: true,
        }
    }

    fn retained_data_limit(&self) -> Limit {
        self.max
            .saturating_sub(self.used)
            .saturating_sub(DynamicTable::OVERHEAD)
    }

    fn admit(&mut self, name_len: usize, value_len: usize) -> bool {
        self.used = self
            .used
            .saturating_add(name_len)
            .saturating_add(value_len)
            .saturating_add(DynamicTable::OVERHEAD);
        if !self.max.allows(self.used) {
            self.exceeded = true;
            false
        } else {
            true
        }
    }
}

#[derive(Clone, Copy)]
struct LiteralContext {
    retain_limit: Limit,
    name_len: usize,
    flags: u8,
}

const INDEX_IT: u8 = 1 << 0;
const NAME_RETAINED: u8 = 1 << 1;
const NAME_TARGET: u8 = 1 << 2;

impl LiteralContext {
    fn new(index_it: bool, retain_limit: Limit) -> Self {
        Self {
            retain_limit,
            name_len: 0,
            flags: if index_it { INDEX_IT } else { 0 },
        }
    }

    fn with_name(mut self, name_len: usize, retained: bool) -> Self {
        self.name_len = name_len;
        self.flags &= !NAME_TARGET;
        if retained {
            self.flags |= NAME_RETAINED;
        } else {
            self.flags &= !NAME_RETAINED;
        }
        self
    }

    fn index_it(self) -> bool {
        self.flags & INDEX_IT != 0
    }

    fn name_retained(self) -> bool {
        self.flags & NAME_RETAINED != 0
    }
}

#[derive(Clone, Copy)]
struct StringTarget(LiteralContext);

impl StringTarget {
    fn name(mut context: LiteralContext) -> Self {
        context.flags |= NAME_TARGET;
        Self(context)
    }

    fn value(mut context: LiteralContext) -> Self {
        context.flags &= !NAME_TARGET;
        Self(context)
    }

    fn retain_limit(self) -> Limit {
        if self.is_name() {
            self.0.retain_limit
        } else {
            self.0.retain_limit.saturating_sub(self.0.name_len)
        }
    }

    fn is_name(self) -> bool {
        self.0.flags & NAME_TARGET != 0
    }

    fn context(self) -> LiteralContext {
        self.0
    }
}

#[derive(Clone, Copy)]
enum IntegerTarget {
    Indexed,
    LiteralName { index_it: bool },
    DynamicSize,
    StringLength { huffman: bool, target: StringTarget },
}

struct IntegerState {
    value: u64,
    shift: u32,
    target: IntegerTarget,
}

struct StringState {
    remaining: usize,
    decoded_len: usize,
    retained: bool,
    target: StringTarget,
    huffman: Option<HpackHuffmanDecoder>,
}

enum Phase {
    Representation,
    Integer(IntegerState),
    StringPrefix(StringTarget),
    String(StringState),
}

pub struct DecoderBlock {
    phase: Phase,
    budget: HeaderListBudget,
}

impl DecoderBlock {
    pub fn finish(self) -> Result<bool, DecoderError> {
        if !matches!(self.phase, Phase::Representation) {
            return Err(DecoderError::NeedMore);
        }
        Ok(self.budget.exceeded)
    }
}

struct DecodedField {
    consumed: usize,
    emit: bool,
}

struct PartialLiteral {
    consumed: usize,
    target: StringTarget,
}

impl Decoder {
    pub fn new(max_dyn_size: usize) -> Self {
        Self {
            dyn_table: DynamicTable::new(max_dyn_size),
            max_size_setting: max_dyn_size,
            max_header_list_size: None,
            name_scratch: Vec::new(),
            value_scratch: Vec::new(),
        }
    }

    pub fn set_max_size(&mut self, n: usize) {
        self.max_size_setting = n;
        if n < self.dyn_table.max_size() {
            self.dyn_table.set_max(n);
        }
    }

    pub fn set_max_header_list_size(&mut self, n: Option<usize>) {
        self.max_header_list_size = n;
    }

    pub fn dyn_size(&self) -> usize {
        self.dyn_table.current_size()
    }

    pub fn dyn_max(&self) -> usize {
        self.dyn_table.max_size()
    }

    pub fn dyn_len(&self) -> usize {
        self.dyn_table.len()
    }

    pub fn dyn_get(&self, index: usize) -> Option<(&[u8], &[u8])> {
        self.dyn_table.get(index)
    }

    pub fn decode<F>(&mut self, buf: &[u8], mut emit: F) -> Result<(), DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        self.decode_bounded(buf, |n, v| emit(n, v)).map(|_| ())
    }

    pub fn decode_bounded<F>(&mut self, buf: &[u8], mut emit: F) -> Result<bool, DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let mut budget = HeaderListBudget::new(self.max_header_list_size);
        let mut input = buf;
        while !input.is_empty() {
            let consumed = self.decode_one(input, &mut budget, &mut emit, None)?;
            input = &input[consumed..];
        }
        Ok(budget.exceeded)
    }

    pub fn start_block(&self) -> DecoderBlock {
        DecoderBlock {
            phase: Phase::Representation,
            budget: HeaderListBudget::new(self.max_header_list_size),
        }
    }

    pub fn decode_fragment<F>(
        &mut self,
        block: &mut DecoderBlock,
        mut input: &[u8],
        mut emit: F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        while !input.is_empty() {
            let phase = core::mem::replace(&mut block.phase, Phase::Representation);
            match phase {
                Phase::Representation => {
                    let mut partial = None;
                    match self.decode_one(input, &mut block.budget, &mut emit, Some(&mut partial)) {
                        Ok(consumed) => {
                            input = &input[consumed..];
                            continue;
                        }
                        Err(DecoderError::NeedMore) => {
                            if let Some(partial) = partial {
                                input = &input[partial.consumed..];
                                block.phase = Phase::StringPrefix(partial.target);
                                continue;
                            }
                        }
                        Err(error) => return Err(error),
                    }
                    let first = input[0];
                    input = &input[1..];
                    let (mask, target) = if first & 0x80 != 0 {
                        (0x7f, IntegerTarget::Indexed)
                    } else if first & 0xC0 == 0x40 {
                        (0x3f, IntegerTarget::LiteralName { index_it: true })
                    } else if first & 0xE0 == 0x20 {
                        (0x1f, IntegerTarget::DynamicSize)
                    } else {
                        (0x0f, IntegerTarget::LiteralName { index_it: false })
                    };
                    self.start_integer(block, first, mask, target, &mut emit)?;
                }
                Phase::Integer(mut integer) => {
                    let mut complete = false;
                    while let Some((&byte, rest)) = input.split_first() {
                        input = rest;
                        let chunk = (byte & 0x7f) as u64;
                        let shifted = chunk
                            .checked_shl(integer.shift)
                            .ok_or(DecoderError::BadInteger)?;
                        integer.value = integer
                            .value
                            .checked_add(shifted)
                            .ok_or(DecoderError::BadInteger)?;
                        if byte & 0x80 == 0 {
                            complete = true;
                            break;
                        }
                        integer.shift = integer
                            .shift
                            .checked_add(7)
                            .ok_or(DecoderError::BadInteger)?;
                        if integer.shift >= 64 {
                            return Err(DecoderError::BadInteger);
                        }
                    }
                    if complete {
                        self.complete_integer(block, integer.value, integer.target, &mut emit)?;
                    } else {
                        block.phase = Phase::Integer(integer);
                    }
                }
                Phase::StringPrefix(target) => {
                    let first = input[0];
                    input = &input[1..];
                    self.start_integer(
                        block,
                        first,
                        0x7f,
                        IntegerTarget::StringLength {
                            huffman: first & 0x80 != 0,
                            target,
                        },
                        &mut emit,
                    )?;
                }
                Phase::String(mut string) => {
                    let amount = input.len().min(string.remaining);
                    let payload = &input[..amount];
                    input = &input[amount..];
                    string.remaining -= amount;
                    let scratch = if string.target.is_name() {
                        &mut self.name_scratch
                    } else {
                        &mut self.value_scratch
                    };
                    if let Some(huffman) = &mut string.huffman {
                        let retain_limit = string.target.retain_limit();
                        huffman
                            .feed(payload, |byte| {
                                string.decoded_len = string.decoded_len.checked_add(1).ok_or(())?;
                                if string.retained && retain_limit.allows(string.decoded_len) {
                                    scratch.push(byte);
                                } else if string.retained {
                                    string.retained = false;
                                    scratch.clear();
                                }
                                Ok::<(), ()>(())
                            })
                            .map_err(|_| DecoderError::BadString)?;
                    } else if string.retained {
                        scratch.extend_from_slice(payload);
                        string.decoded_len += amount;
                    } else {
                        string.decoded_len += amount;
                    }
                    if string.remaining == 0 {
                        self.complete_string(block, string, &mut emit)?;
                    } else {
                        block.phase = Phase::String(string);
                    }
                }
            }
        }
        Ok(())
    }

    fn lookup(dyn_table: &DynamicTable, idx: usize) -> Result<(&[u8], &[u8]), DecoderError> {
        if idx == 0 {
            return Err(DecoderError::BadIndex);
        }
        if idx <= StaticTable::LEN {
            let (n, v) = StaticTable::get(idx).ok_or(DecoderError::BadIndex)?;
            return Ok((n, v));
        }
        let dyn_idx = idx - StaticTable::LEN - 1;
        dyn_table.get(dyn_idx).ok_or(DecoderError::BadIndex)
    }

    fn decode_one<F>(
        &mut self,
        input: &[u8],
        budget: &mut HeaderListBudget,
        emit: &mut F,
        partial: Option<&mut Option<PartialLiteral>>,
    ) -> Result<usize, DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let first = input[0];
        let size_update = first & 0xE0 == 0x20;
        if size_update {
            if !budget.allow_size_update {
                return Err(DecoderError::BadDynSizeUpdate);
            }
        } else {
            budget.allow_size_update = false;
        }
        if first & 0x80 != 0 {
            let (index, consumed) = PrefixedInt::<7>::decode(input)?;
            let index = usize::try_from(index.get()).map_err(|_| DecoderError::BadIndex)?;
            let (name, value) = Self::lookup(&self.dyn_table, index)?;
            if budget.admit(name.len(), value.len()) {
                emit(name, value);
            }
            return Ok(consumed);
        }
        if first & 0xC0 == 0x40 {
            let (name_index, consumed) = PrefixedInt::<6>::decode(input)?;
            let name_index =
                usize::try_from(name_index.get()).map_err(|_| DecoderError::BadIndex)?;
            let literal = self.decode_complete_literal(
                &input[consumed..],
                consumed,
                name_index,
                true,
                budget,
                partial,
            )?;
            if literal.emit {
                emit(&self.name_scratch, &self.value_scratch);
            }
            return Ok(consumed + literal.consumed);
        }
        if size_update {
            let (new_size, consumed) = PrefixedInt::<5>::decode(input)?;
            let new_size =
                usize::try_from(new_size.get()).map_err(|_| DecoderError::BadDynSizeUpdate)?;
            if new_size > self.max_size_setting {
                return Err(DecoderError::BadDynSizeUpdate);
            }
            self.dyn_table.set_max(new_size);
            return Ok(consumed);
        }
        let (name_index, consumed) = PrefixedInt::<4>::decode(input)?;
        let name_index = usize::try_from(name_index.get()).map_err(|_| DecoderError::BadIndex)?;
        let literal = self.decode_complete_literal(
            &input[consumed..],
            consumed,
            name_index,
            false,
            budget,
            partial,
        )?;
        if literal.emit {
            emit(&self.name_scratch, &self.value_scratch);
        }
        Ok(consumed + literal.consumed)
    }

    fn decode_complete_literal(
        &mut self,
        input: &[u8],
        representation_consumed: usize,
        name_index: usize,
        index_it: bool,
        budget: &mut HeaderListBudget,
        partial: Option<&mut Option<PartialLiteral>>,
    ) -> Result<DecodedField, DecoderError> {
        let context = self.literal_context(budget, index_it);
        let retain_limit = context.retain_limit;
        let mut consumed = 0;
        let (name_len, name_retained) = if name_index == 0 {
            let name = Literal::decode_into(input, &mut self.name_scratch, retain_limit.option())?;
            consumed += name.consumed;
            (name.len, name.retained)
        } else {
            self.retain_indexed_name(name_index, retain_limit)?
        };
        let value = Literal::decode_into(
            &input[consumed..],
            &mut self.value_scratch,
            retain_limit.saturating_sub(name_len).option(),
        );
        let value = match value {
            Ok(value) => value,
            Err(DecoderError::NeedMore) => {
                if let Some(partial) = partial {
                    *partial = Some(PartialLiteral {
                        consumed: representation_consumed + consumed,
                        target: StringTarget::value(context.with_name(name_len, name_retained)),
                    });
                }
                return Err(DecoderError::NeedMore);
            }
            Err(error) => return Err(error),
        };
        consumed += value.consumed;
        let emit = self.finish_literal(
            budget,
            context.with_name(name_len, name_retained),
            value.len,
            value.retained,
        )?;
        Ok(DecodedField { consumed, emit })
    }

    fn start_integer<F>(
        &mut self,
        block: &mut DecoderBlock,
        first: u8,
        mask: u8,
        target: IntegerTarget,
        emit: &mut F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let prefix = first & mask;
        if prefix < mask {
            self.complete_integer(block, prefix as u64, target, emit)
        } else {
            block.phase = Phase::Integer(IntegerState {
                value: mask as u64,
                shift: 0,
                target,
            });
            Ok(())
        }
    }

    fn complete_integer<F>(
        &mut self,
        block: &mut DecoderBlock,
        value: u64,
        target: IntegerTarget,
        emit: &mut F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        match target {
            IntegerTarget::Indexed => {
                let index = usize::try_from(value).map_err(|_| DecoderError::BadIndex)?;
                let (name, header_value) = Self::lookup(&self.dyn_table, index)?;
                if block.budget.admit(name.len(), header_value.len()) {
                    emit(name, header_value);
                }
                block.phase = Phase::Representation;
            }
            IntegerTarget::LiteralName { index_it } => {
                let name_index = usize::try_from(value).map_err(|_| DecoderError::BadIndex)?;
                self.start_literal(block, name_index, index_it)?;
            }
            IntegerTarget::DynamicSize => {
                let new_size =
                    usize::try_from(value).map_err(|_| DecoderError::BadDynSizeUpdate)?;
                if new_size > self.max_size_setting {
                    return Err(DecoderError::BadDynSizeUpdate);
                }
                self.dyn_table.set_max(new_size);
                block.phase = Phase::Representation;
            }
            IntegerTarget::StringLength { huffman, target } => {
                if value > literal::MAX_LITERAL_LEN as u64 {
                    return Err(DecoderError::BadString);
                }
                let len = value as usize;
                let retained = huffman || target.retain_limit().allows(len);
                let scratch = if target.is_name() {
                    &mut self.name_scratch
                } else {
                    &mut self.value_scratch
                };
                scratch.clear();
                let string = StringState {
                    remaining: len,
                    decoded_len: 0,
                    retained,
                    target,
                    huffman: huffman.then(HpackHuffmanDecoder::new),
                };
                if len == 0 {
                    self.complete_string(block, string, emit)?;
                } else {
                    block.phase = Phase::String(string);
                }
            }
        }
        Ok(())
    }

    fn start_literal(
        &mut self,
        block: &mut DecoderBlock,
        name_index: usize,
        index_it: bool,
    ) -> Result<(), DecoderError> {
        let context = self.literal_context(&block.budget, index_it);
        if name_index == 0 {
            block.phase = Phase::StringPrefix(StringTarget::name(context));
        } else {
            let (name_len, retained) =
                self.retain_indexed_name(name_index, context.retain_limit)?;
            block.phase =
                Phase::StringPrefix(StringTarget::value(context.with_name(name_len, retained)));
        }
        Ok(())
    }

    fn literal_context(&self, budget: &HeaderListBudget, index_it: bool) -> LiteralContext {
        let retain_limit = if index_it {
            let dynamic_data_limit = self
                .dyn_table
                .max_size()
                .saturating_sub(DynamicTable::OVERHEAD);
            budget.retained_data_limit().max(dynamic_data_limit)
        } else {
            budget.retained_data_limit()
        };
        LiteralContext::new(index_it, retain_limit)
    }

    fn retain_indexed_name(
        &mut self,
        name_index: usize,
        retain_limit: Limit,
    ) -> Result<(usize, bool), DecoderError> {
        let (name, _) = Self::lookup(&self.dyn_table, name_index)?;
        self.name_scratch.clear();
        let retained = retain_limit.allows(name.len());
        if retained {
            self.name_scratch.extend_from_slice(name);
        }
        Ok((name.len(), retained))
    }

    fn complete_string<F>(
        &mut self,
        block: &mut DecoderBlock,
        string: StringState,
        emit: &mut F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        if let Some(huffman) = string.huffman {
            huffman.finish().map_err(|_| DecoderError::BadString)?;
        }
        if string.target.is_name() {
            let context = string
                .target
                .context()
                .with_name(string.decoded_len, string.retained);
            block.phase = Phase::StringPrefix(StringTarget::value(context));
        } else {
            let should_emit = self.finish_literal(
                &mut block.budget,
                string.target.context(),
                string.decoded_len,
                string.retained,
            )?;
            if should_emit {
                emit(&self.name_scratch, &self.value_scratch);
            }
            block.phase = Phase::Representation;
        }
        Ok(())
    }

    fn finish_literal(
        &mut self,
        budget: &mut HeaderListBudget,
        context: LiteralContext,
        value_len: usize,
        value_retained: bool,
    ) -> Result<bool, DecoderError> {
        let field_data_len = context.name_len.checked_add(value_len);
        let retained = context.name_retained() && value_retained;
        if context.index_it() {
            let dynamic_data_limit = self
                .dyn_table
                .max_size()
                .saturating_sub(DynamicTable::OVERHEAD);
            if retained && field_data_len.is_some_and(|len| len <= dynamic_data_limit) {
                self.dyn_table
                    .insert(&self.name_scratch, &self.value_scratch);
            } else {
                self.dyn_table.reject_oversized_insert();
            }
        }
        let should_emit = budget.admit(context.name_len, value_len);
        if should_emit && !retained {
            return Err(DecoderError::BadString);
        }
        Ok(should_emit)
    }
}
