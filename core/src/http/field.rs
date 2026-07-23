use std::fmt;
use std::ops::Range;

use o3::buffer::{Pooled, SharedPool, SpareWriter};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Field<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Field<'a> {
    pub const fn new(name: &'a [u8], value: &'a [u8]) -> Self {
        Self { name, value }
    }
}

impl<'field> From<&Field<'field>> for Field<'field> {
    fn from(field: &Field<'field>) -> Self {
        *field
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedField {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl OwnedField {
    pub fn new(name: &[u8], value: &[u8]) -> Self {
        Self {
            name: name.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn as_ref(&self) -> Field<'_> {
        Field::new(&self.name, &self.value)
    }
}

impl From<Field<'_>> for OwnedField {
    fn from(field: Field<'_>) -> Self {
        Self::new(field.name, field.value)
    }
}

impl<'a> From<&'a OwnedField> for Field<'a> {
    fn from(field: &'a OwnedField) -> Self {
        field.as_ref()
    }
}

pub trait FieldStorage {
    type Iter<'a>: Iterator<Item = Field<'a>>
    where
        Self: 'a;

    fn fields(&self) -> Self::Iter<'_>;
}

#[derive(Clone)]
pub struct FieldBlock<S> {
    storage: S,
}

impl<S> FieldBlock<S> {
    pub const fn from_storage(storage: S) -> Self {
        Self { storage }
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }
}

impl<S: FieldStorage> FieldBlock<S> {
    pub fn iter(&self) -> S::Iter<'_> {
        self.storage.fields()
    }

    pub fn get(&self, name: &[u8]) -> Option<&[u8]> {
        self.iter()
            .find(|field| field.name == name)
            .map(|field| field.value)
    }
}

impl<S: FieldStorage> fmt::Debug for FieldBlock<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<S, T> PartialEq<FieldBlock<T>> for FieldBlock<S>
where
    S: FieldStorage,
    T: FieldStorage,
{
    fn eq(&self, other: &FieldBlock<T>) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<S: FieldStorage> Eq for FieldBlock<S> {}

impl<'a, S: FieldStorage> IntoIterator for &'a FieldBlock<S> {
    type Item = Field<'a>;
    type IntoIter = S::Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct OwnedFieldIter<'a> {
    fields: std::slice::Iter<'a, OwnedField>,
}

impl<'a> Iterator for OwnedFieldIter<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fields.next().map(OwnedField::as_ref)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.fields.size_hint()
    }
}

impl ExactSizeIterator for OwnedFieldIter<'_> {}

impl FieldStorage for Vec<OwnedField> {
    type Iter<'a> = OwnedFieldIter<'a>;

    fn fields(&self) -> Self::Iter<'_> {
        OwnedFieldIter {
            fields: self.iter(),
        }
    }
}

pub type OwnedFieldBlock = FieldBlock<Vec<OwnedField>>;

impl FieldBlock<Vec<OwnedField>> {
    pub const fn new() -> Self {
        Self::from_storage(Vec::new())
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) {
        self.storage.push(OwnedField::new(name, value));
    }

    pub fn as_slice(&self) -> &[OwnedField] {
        &self.storage
    }
}

impl Default for FieldBlock<Vec<OwnedField>> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct PackedFields<S> {
    first: S,
    second: Option<S>,
}

trait PackedWriter {
    fn extend(&mut self, bytes: &[u8]);
}

impl PackedWriter for Vec<u8> {
    fn extend(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

impl PackedWriter for SpareWriter<'_> {
    fn extend(&mut self, bytes: &[u8]) {
        self.try_extend_from_slice(bytes)
            .expect("precomputed field block capacity");
    }
}

fn write_packed_prefix(writer: &mut impl PackedWriter, name: &[u8], value_len: usize) {
    let name_len = packed_len(name.len());
    let value_len = packed_len(value_len);
    writer.extend(&name_len);
    writer.extend(&value_len);
    writer.extend(name);
}

fn packed_len(len: usize) -> [u8; 4] {
    u32::try_from(len)
        .expect("field component length overflow")
        .to_ne_bytes()
}

fn write_packed_field(writer: &mut impl PackedWriter, field: Field<'_>) {
    write_packed_prefix(writer, field.name, field.value.len());
    writer.extend(field.value);
}

fn packed_capacity(fields: &[Field<'_>]) -> usize {
    fields.iter().fold(0usize, |size, field| {
        field
            .name
            .len()
            .checked_add(field.value.len())
            .and_then(|field_len| field_len.checked_add(8))
            .and_then(|field_len| size.checked_add(field_len))
            .expect("field block size overflow")
    })
}

impl<S> PackedFields<S> {
    pub const fn new(first: S) -> Self {
        Self {
            first,
            second: None,
        }
    }
}

impl<S: AsRef<[u8]>> FieldStorage for PackedFields<S> {
    type Iter<'a>
        = PackedFieldIter<'a>
    where
        S: 'a;

    fn fields(&self) -> Self::Iter<'_> {
        PackedFieldIter {
            current: self.first.as_ref(),
            second: self.second.as_ref().map(AsRef::as_ref),
        }
    }
}

pub type PooledFieldBlock = FieldBlock<PackedFields<Pooled>>;
pub type VecFieldBlock = FieldBlock<PackedFields<Vec<u8>>>;

impl FieldBlock<PackedFields<Pooled>> {
    pub fn from_pooled(pooled: Pooled) -> Self {
        Self::from_storage(PackedFields::new(pooled))
    }

    pub fn from_fields(fields: &[Field<'_>]) -> Self {
        let capacity = packed_capacity(fields);
        let pool = SharedPool::new(1, capacity.max(1));
        let mut lease = pool.try_acquire().expect("new field pool has one slot");
        let mut writer = lease.spare_writer();
        for field in fields {
            write_packed_field(&mut writer, *field);
        }
        drop(writer);
        Self::from_pooled(lease.freeze())
    }

    pub fn from_headers(fields: &[Field<'_>]) -> Self {
        Self::from_fields(fields)
    }

    pub fn append(&mut self, other: Self) -> Result<(), Self> {
        if self.storage.second.is_some() || other.storage.second.is_some() {
            return Err(other);
        }
        self.storage.second = Some(other.storage.first);
        Ok(())
    }

    pub fn to_owned(&self) -> Vec<OwnedField> {
        self.iter().map(OwnedField::from).collect()
    }
}

impl FieldBlock<PackedFields<Vec<u8>>> {
    pub const fn new() -> Self {
        Self::from_storage(PackedFields::new(Vec::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::from_storage(PackedFields::new(Vec::with_capacity(capacity)))
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) {
        self.push_encoded(name, value.len(), |writer| writer.extend_from_slice(value));
    }

    pub fn push_encoded(
        &mut self,
        name: &[u8],
        value_len: usize,
        encode: impl FnOnce(&mut FieldValueWriter<'_>),
    ) {
        write_packed_prefix(&mut self.storage.first, name, value_len);
        let start = self.storage.first.len();
        encode(&mut FieldValueWriter {
            bytes: &mut self.storage.first,
        });
        assert_eq!(
            self.storage.first.len() - start,
            value_len,
            "encoded field value length mismatch"
        );
    }

    pub fn try_push_parts<E>(
        &mut self,
        encode_name: impl FnOnce(&mut FieldValueWriter<'_>) -> Result<(), E>,
        encode_value: impl FnOnce(&mut FieldValueWriter<'_>, usize) -> Result<(), E>,
    ) -> Result<(usize, usize), E> {
        let field_start = self.storage.first.len();
        self.storage.first.extend_from_slice(&[0; 8]);
        let name_start = self.storage.first.len();
        if let Err(error) = encode_name(&mut FieldValueWriter {
            bytes: &mut self.storage.first,
        }) {
            self.storage.first.truncate(field_start);
            return Err(error);
        }
        let value_start = self.storage.first.len();
        let name_len = value_start - name_start;
        if let Err(error) = encode_value(
            &mut FieldValueWriter {
                bytes: &mut self.storage.first,
            },
            name_len,
        ) {
            self.storage.first.truncate(field_start);
            return Err(error);
        }
        let end = self.storage.first.len();
        let value_len = end - value_start;
        self.storage.first[field_start..field_start + 4].copy_from_slice(&packed_len(name_len));
        self.storage.first[field_start + 4..name_start].copy_from_slice(&packed_len(value_len));
        Ok((name_len, value_len))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.storage.first
    }

    pub fn iter_with_value_ranges(&self) -> PackedFieldRangeIter<'_> {
        PackedFieldRangeIter {
            current: &self.storage.first,
            offset: 0,
        }
    }
}

impl Default for FieldBlock<PackedFields<Vec<u8>>> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FieldValueWriter<'a> {
    bytes: &'a mut Vec<u8>,
}

impl FieldValueWriter<'_> {
    pub fn push(&mut self, byte: u8) {
        self.bytes.push(byte);
    }

    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
}

pub struct PackedFieldIter<'a> {
    current: &'a [u8],
    second: Option<&'a [u8]>,
}

impl<'a> Iterator for PackedFieldIter<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_empty() {
            self.current = self.second.take()?;
        }
        let Some((field, _, end)) = parse_packed_field(self.current) else {
            self.current = &[];
            self.second = None;
            return None;
        };
        self.current = &self.current[end..];
        Some(field)
    }
}

pub struct PackedFieldRangeIter<'a> {
    current: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for PackedFieldRangeIter<'a> {
    type Item = (Field<'a>, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let Some((field, value_start, end)) = parse_packed_field(self.current) else {
            self.current = &[];
            return None;
        };
        let value_range = self.offset + value_start..self.offset + end;
        self.current = &self.current[end..];
        self.offset += end;
        Some((field, value_range))
    }
}

fn parse_packed_field(bytes: &[u8]) -> Option<(Field<'_>, usize, usize)> {
    if bytes.len() < 8 {
        return None;
    }
    let name_len = u32::from_ne_bytes(bytes[..4].try_into().ok()?) as usize;
    let value_len = u32::from_ne_bytes(bytes[4..8].try_into().ok()?) as usize;
    let value_start = 8usize.checked_add(name_len)?;
    let end = value_start.checked_add(value_len)?;
    if end > bytes.len() {
        return None;
    }
    Some((
        Field::new(&bytes[8..value_start], &bytes[value_start..end]),
        value_start,
        end,
    ))
}
