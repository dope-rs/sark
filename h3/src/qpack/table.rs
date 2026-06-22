use std::collections::VecDeque;

use sark_core::http::{Field, OwnedField};

use super::DecoderError;

#[derive(Clone, Debug)]
pub struct DynamicTable {
    entries: VecDeque<OwnedField>,
    capacity: usize,
    max_capacity: usize,
    size: usize,
    insert_count: u64,
}

impl DynamicTable {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity: 0,
            max_capacity,
            size: 0,
            insert_count: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn max_entries(&self) -> u64 {
        (self.max_capacity / 32) as u64
    }

    pub fn insert_count(&self) -> u64 {
        self.insert_count
    }

    pub fn set_capacity(&mut self, capacity: usize) -> Result<(), DecoderError> {
        if capacity > self.max_capacity {
            return Err(DecoderError::EncoderStream);
        }
        self.capacity = capacity;
        self.evict_to_capacity()?;
        Ok(())
    }

    pub fn insert(&mut self, field: OwnedField) -> Result<Option<u64>, DecoderError> {
        let entry_size = Self::entry_size(field.name.len(), field.value.len());
        if entry_size > self.capacity {
            return Ok(None);
        }
        while self.size + entry_size > self.capacity {
            self.evict_oldest()?;
        }
        let absolute = self.insert_count;
        self.insert_count = self
            .insert_count
            .checked_add(1)
            .ok_or(DecoderError::BadInteger)?;
        self.size += entry_size;
        self.entries.push_front(field);
        Ok(Some(absolute))
    }

    pub fn duplicate_relative(&mut self, relative: u64) -> Result<Option<u64>, DecoderError> {
        let Some(field) = self.get_relative(relative) else {
            return Err(DecoderError::InvalidReference);
        };
        self.insert(field)
    }

    pub fn get_relative(&self, relative: u64) -> Option<OwnedField> {
        let index = usize::try_from(relative).ok()?;
        self.entries.get(index).cloned()
    }

    pub fn get_absolute(&self, absolute: u64) -> Option<OwnedField> {
        if absolute >= self.insert_count {
            return None;
        }
        let relative = self.insert_count - absolute - 1;
        self.get_relative(relative)
    }

    pub fn get_relative_to_base(&self, base: u64, relative: u64) -> Option<OwnedField> {
        let absolute = base.checked_sub(relative)?.checked_sub(1)?;
        self.get_absolute(absolute)
    }

    pub fn find_exact(&self, field: Field<'_>) -> Option<u64> {
        self.entries
            .iter()
            .enumerate()
            .find_map(|(relative, entry)| {
                if entry.name == field.name && entry.value == field.value {
                    Some(self.insert_count - relative as u64 - 1)
                } else {
                    None
                }
            })
    }

    pub fn find_name(&self, name: &[u8]) -> Option<u64> {
        self.entries
            .iter()
            .enumerate()
            .find_map(|(relative, entry)| {
                if entry.name == name {
                    Some(self.insert_count - relative as u64 - 1)
                } else {
                    None
                }
            })
    }

    fn evict_to_capacity(&mut self) -> Result<(), DecoderError> {
        while self.size > self.capacity {
            self.evict_oldest()?;
        }
        Ok(())
    }

    fn evict_oldest(&mut self) -> Result<(), DecoderError> {
        let Some(field) = self.entries.pop_back() else {
            return Err(DecoderError::EncoderStream);
        };
        self.size = self
            .size
            .checked_sub(Self::entry_size(field.name.len(), field.value.len()))
            .ok_or(DecoderError::BadInteger)?;
        Ok(())
    }

    fn entry_size(name_len: usize, value_len: usize) -> usize {
        name_len + value_len + 32
    }
}
