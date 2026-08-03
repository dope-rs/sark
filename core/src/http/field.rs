use std::convert::Infallible;
use std::fmt;
use std::ops::Range;

use o3::buffer::{ByteSink, PoolLayoutError, Pooled, SharedPool, SharedPoolLayout};

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

#[doc(hidden)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FieldMatch<I> {
    pub exact: Option<I>,
    pub name: Option<I>,
}

#[doc(hidden)]
pub fn match_field_candidates<'a, K, I>(
    target: Field<'_>,
    candidates: impl IntoIterator<Item = (K, Field<'a>)>,
    mut classify: impl FnMut(K) -> Option<I>,
) -> FieldMatch<I>
where
    I: Copy,
{
    let mut name = None;
    for (key, candidate) in candidates {
        if candidate.name != target.name {
            continue;
        }
        let Some(index) = classify(key) else {
            continue;
        };
        name.get_or_insert(index);
        if candidate.value == target.value {
            return FieldMatch {
                exact: Some(index),
                name,
            };
        }
    }
    FieldMatch { exact: None, name }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackedFieldError<E> {
    Write(E),
    ComponentTooLarge { len: usize },
    ValueLengthMismatch { expected: usize, actual: usize },
}

fn write_packed_prefix<W: ByteSink>(
    writer: &mut W,
    name: &[u8],
    value_len: usize,
) -> Result<(), PackedFieldError<W::Error>> {
    let name_len = packed_len(name.len())?;
    let value_len = packed_len(value_len)?;
    writer
        .write_slices([&name_len, &value_len, name])
        .map_err(PackedFieldError::Write)
}

fn packed_len<E>(len: usize) -> Result<[u8; 4], PackedFieldError<E>> {
    match u32::try_from(len) {
        Ok(len) => Ok(len.to_ne_bytes()),
        Err(_) => Err(PackedFieldError::ComponentTooLarge { len }),
    }
}

fn write_packed_field<W: ByteSink>(
    writer: &mut W,
    field: Field<'_>,
) -> Result<(), PackedFieldError<W::Error>> {
    let name_len = packed_len(field.name.len())?;
    let value_len = packed_len(field.value.len())?;
    writer
        .write_slices([&name_len, &value_len, field.name, field.value])
        .map_err(PackedFieldError::Write)
}

fn packed_capacity(fields: &[Field<'_>]) -> Option<usize> {
    fields.iter().try_fold(0usize, |size, field| {
        u32::try_from(field.name.len()).ok()?;
        u32::try_from(field.value.len()).ok()?;
        field
            .name
            .len()
            .checked_add(field.value.len())?
            .checked_add(8)?
            .checked_add(size)
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
pub type DecodedFieldBlock = PooledFieldBlock;

impl FieldBlock<PackedFields<Pooled>> {
    pub fn from_pooled(pooled: Pooled) -> Self {
        Self::from_storage(PackedFields::new(pooled))
    }

    pub fn from_fields(fields: &[Field<'_>]) -> Result<Self, PoolLayoutError> {
        let capacity = packed_capacity(fields).ok_or(PoolLayoutError::CapacityOverflow)?;
        let layout = SharedPoolLayout::new(1, capacity.max(1))?;
        let pool = SharedPool::from_layout(layout);
        let mut lease = pool.try_acquire().ok_or(PoolLayoutError::SlotOverflow)?;
        let mut writer = lease.spare_writer();
        for field in fields {
            write_packed_field(&mut writer, *field)
                .map_err(|_| PoolLayoutError::CapacityOverflow)?;
        }
        drop(writer);
        Ok(Self::from_pooled(lease.freeze()))
    }

    pub fn from_headers(fields: &[Field<'_>]) -> Result<Self, PoolLayoutError> {
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

    pub(crate) fn into_primary(self) -> Pooled {
        self.storage.first
    }
}

impl FieldBlock<PackedFields<Vec<u8>>> {
    pub const fn new() -> Self {
        Self::from_storage(PackedFields::new(Vec::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::from_storage(PackedFields::new(Vec::with_capacity(capacity)))
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) -> Result<(), PackedFieldError<Infallible>> {
        write_packed_field(&mut self.storage.first, Field::new(name, value))
    }

    pub fn push_encoded(
        &mut self,
        name: &[u8],
        value_len: usize,
        encode: impl FnOnce(&mut FieldValueWriter<'_>),
    ) -> Result<(), PackedFieldError<Infallible>> {
        let field_start = self.storage.first.len();
        write_packed_prefix(&mut self.storage.first, name, value_len)?;
        let start = self.storage.first.len();
        encode(&mut FieldValueWriter {
            bytes: &mut self.storage.first,
        });
        let actual = self.storage.first.len() - start;
        if actual != value_len {
            self.storage.first.truncate(field_start);
            return Err(PackedFieldError::ValueLengthMismatch {
                expected: value_len,
                actual,
            });
        }
        Ok(())
    }

    pub fn try_push_parts<E>(
        &mut self,
        encode_name: impl FnOnce(&mut FieldValueWriter<'_>) -> Result<(), E>,
        encode_value: impl FnOnce(&mut FieldValueWriter<'_>, usize) -> Result<(), E>,
    ) -> Result<(usize, usize), PackedFieldError<E>> {
        let field_start = self.storage.first.len();
        self.storage.first.extend_from_slice(&[0; 8]);
        let name_start = self.storage.first.len();
        if let Err(error) = encode_name(&mut FieldValueWriter {
            bytes: &mut self.storage.first,
        }) {
            self.storage.first.truncate(field_start);
            return Err(PackedFieldError::Write(error));
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
            return Err(PackedFieldError::Write(error));
        }
        let end = self.storage.first.len();
        let value_len = end - value_start;
        let name_len_bytes = match packed_len(name_len) {
            Ok(len) => len,
            Err(error) => {
                self.storage.first.truncate(field_start);
                return Err(error);
            }
        };
        let value_len_bytes = match packed_len(value_len) {
            Ok(len) => len,
            Err(error) => {
                self.storage.first.truncate(field_start);
                return Err(error);
            }
        };
        self.storage.first[field_start..field_start + 4].copy_from_slice(&name_len_bytes);
        self.storage.first[field_start + 4..name_start].copy_from_slice(&value_len_bytes);
        Ok((name_len, value_len))
    }
}

impl<S: AsRef<[u8]>> FieldBlock<PackedFields<S>> {
    pub fn as_bytes(&self) -> &[u8] {
        self.storage.first.as_ref()
    }

    pub fn iter_with_value_ranges(&self) -> PackedFieldRangeIter<'_> {
        PackedFieldRangeIter {
            current: self.storage.first.as_ref(),
            offset: 0,
        }
    }

    #[doc(hidden)]
    pub fn iter_with_value_ranges_from(&self, offset: usize) -> PackedFieldRangeIter<'_> {
        PackedFieldRangeIter {
            current: self
                .storage
                .first
                .as_ref()
                .get(offset..)
                .unwrap_or_default(),
            offset,
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

#[cfg(test)]
mod tests {
    use super::{Field, match_field_candidates};

    #[test]
    fn field_matching_classifies_only_name_candidates_and_stops_at_exact() {
        let candidates = [
            (0, Field::new(b"other", b"target")),
            (1, Field::new(b"name", b"older")),
            (2, Field::new(b"name", b"target")),
            (3, Field::new(b"name", b"newer")),
        ];
        let mut classified = Vec::new();
        let found = match_field_candidates(Field::new(b"name", b"target"), candidates, |index| {
            classified.push(index);
            (index != 1).then_some(index)
        });

        assert_eq!(found.exact, Some(2));
        assert_eq!(found.name, Some(2));
        assert_eq!(classified, [1, 2]);
    }
}
