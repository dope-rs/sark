use std::{borrow::Cow, slice};

use o3::alloc;

use crate::{
    field::{self, Lower, Value, storage},
    h1::{codec::decode::fields, field::Entry},
    payload::response::headers,
};

type Field<'field, N> = (N, Value<Cow<'field, [u8]>>);

#[derive(PartialEq, Eq)]
pub struct List<'field, N = Lower<Cow<'field, str>>> {
    entries: Vec<Field<'field, N>>,
}

impl<N> Default for List<'_, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'field, N> List<'field, N> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> slice::Iter<'_, Field<'field, N>> {
        self.entries.iter()
    }

    fn try_append(&mut self, field: Field<'field, N>) -> Result<(), alloc::Error> {
        let slot = alloc::Vacant::try_new(&mut self.entries)?;
        slot.insert(field);
        Ok(())
    }
}

impl<'field, N: AsRef<[u8]>> List<'field, N> {
    pub fn get(&self, name: &str) -> Option<&Value<Cow<'field, [u8]>>> {
        self.entries
            .iter()
            .find(|entry| entry.0.as_ref().eq_ignore_ascii_case(name.as_bytes()))
            .map(|entry| &entry.1)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}

impl field::Source for List<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        for (name, value) in &self.entries {
            sink.entry(field::Entry::new(name.borrow(), value.borrow()));
        }
    }
}

impl headers::Source for List<'_, headers::Name<Cow<'_, str>>> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        for (name, value) in &self.entries {
            sink.entry(headers::Entry::new(name.borrow(), value.borrow()));
        }
    }
}

impl<'input> fields::Sink<'input> for List<'_> {
    type Error = fields::Error;

    fn push(&mut self, name: &'input [u8], value: &'input [u8]) -> Result<(), Self::Error> {
        let name = Lower::try_copy(name).map_err(fields::Error::Name)?;
        let value = Value::try_copy(value).map_err(fields::Error::Value)?;
        self.try_append((name, value))
            .map_err(fields::Error::Allocation)
    }

    fn entry(&mut self, field: Entry<'input>) -> Result<(), Self::Error> {
        let name = Lower::try_entry(field)
            .map_err(storage::Error::Allocation)
            .map_err(fields::Error::Name)?;
        let value = Value::try_entry(field)
            .map_err(storage::Error::Allocation)
            .map_err(fields::Error::Value)?;
        self.try_append((name, value))
            .map_err(fields::Error::Allocation)
    }
}

impl<'a, 'field, N> IntoIterator for &'a List<'field, N> {
    type Item = &'a Field<'field, N>;
    type IntoIter = slice::Iter<'a, Field<'field, N>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
