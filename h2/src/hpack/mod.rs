mod dynamic_table;
mod static_table;
mod string;

use dynamic_table::DynamicTable;
pub use sark_core::http::{
    Field as Header, OwnedField as OwnedHeader, PooledFieldBlock as HeaderBlock,
};
use sark_core::http::{PrefixedInt, PrefixedIntError};
use static_table::StaticTable;
use string::Codec;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecoderError {
    NeedMore,
    BadIndex,
    BadInteger,
    BadString,
    BadDynSizeUpdate,
    Truncated,
    HeaderListTooLarge,
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
    use_huffman: bool,
}

impl Encoder {
    pub fn new(max_dyn_size: usize) -> Self {
        Self {
            dyn_table: DynamicTable::new(max_dyn_size),
            max_size_setting: max_dyn_size,
            pending_size_update: None,
            use_huffman: true,
        }
    }

    pub fn set_max_size(&mut self, n: usize) {
        self.max_size_setting = n;
        if n < self.dyn_table.max_size() {
            self.dyn_table.set_max(n);
        }
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
            PrefixedInt::encode(n as u64, 5, 0x20, out);
        }
        for h in headers {
            self.encode_one(h, out);
        }
    }

    pub fn encode_one(&mut self, h: Header<'_>, out: &mut Vec<u8>) {
        if let Some(idx) = StaticTable::find(h.name, h.value) {
            PrefixedInt::encode(idx as u64, 7, 0x80, out);
            return;
        }
        if let Some(dyn_idx) = self.dyn_table.find(h.name, h.value) {
            let absolute = StaticTable::LEN + 1 + dyn_idx;
            PrefixedInt::encode(absolute as u64, 7, 0x80, out);
            return;
        }
        let name_idx = StaticTable::find_name(h.name).or_else(|| {
            self.dyn_table
                .find_name(h.name)
                .map(|i| StaticTable::LEN + 1 + i)
        });
        match name_idx {
            Some(idx) => {
                PrefixedInt::encode(idx as u64, 6, 0x40, out);
            }
            None => {
                out.push(0x40);
                Codec::encode(h.name, self.use_huffman, out);
            }
        }
        Codec::encode(h.value, self.use_huffman, out);
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
        let mut running = 0usize;
        let mut over_limit = false;
        let limit = self.max_header_list_size;
        let mut emit = |name: &[u8], value: &[u8]| {
            if let Some(max) = limit {
                running = running
                    .saturating_add(name.len())
                    .saturating_add(value.len())
                    .saturating_add(32);
                if running > max {
                    over_limit = true;
                    return;
                }
            }
            emit(name, value);
        };
        let mut pos = 0;
        while pos < buf.len() {
            let first = buf[pos];
            if first & 0x80 != 0 {
                let (idx, n) = PrefixedInt::decode(&buf[pos..], 7)?;
                pos += n;
                if idx == 0 {
                    return Err(DecoderError::BadIndex);
                }
                let idx = idx as usize;
                let (name, value) = Self::lookup(&self.dyn_table, idx)?;
                emit(name, value);
            } else if first & 0xC0 == 0x40 {
                let (name_idx, n) = PrefixedInt::decode(&buf[pos..], 6)?;
                pos += n;
                let consumed = self.literal(&buf[pos..], name_idx as usize, true, &mut emit)?;
                pos += consumed;
            } else if first & 0xE0 == 0x20 {
                let (new_size, n) = PrefixedInt::decode(&buf[pos..], 5)?;
                pos += n;
                let new_size = new_size as usize;
                if new_size > self.max_size_setting {
                    return Err(DecoderError::BadDynSizeUpdate);
                }
                self.dyn_table.set_max(new_size);
            } else {
                let (name_idx, n) = PrefixedInt::decode(&buf[pos..], 4)?;
                pos += n;
                let consumed = self.literal(&buf[pos..], name_idx as usize, false, &mut emit)?;
                pos += consumed;
            }
        }
        Ok(over_limit)
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

    fn literal<F>(
        &mut self,
        rest: &[u8],
        name_idx: usize,
        index_it: bool,
        emit: &mut F,
    ) -> Result<usize, DecoderError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let mut consumed = 0;
        if name_idx == 0 {
            consumed += Codec::decode_into(rest, &mut self.name_scratch)?;
        } else {
            let (sn, _) = Self::lookup(&self.dyn_table, name_idx)?;
            self.name_scratch.clear();
            self.name_scratch.extend_from_slice(sn);
        }
        consumed += Codec::decode_into(&rest[consumed..], &mut self.value_scratch)?;
        if index_it {
            self.dyn_table
                .insert(&self.name_scratch, &self.value_scratch);
        }
        emit(&self.name_scratch, &self.value_scratch);
        Ok(consumed)
    }
}
