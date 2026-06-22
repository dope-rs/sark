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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedField {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl OwnedField {
    pub fn from(field: Field<'_>) -> Self {
        Self {
            name: field.name.to_vec(),
            value: field.value.to_vec(),
        }
    }

    pub fn as_ref(&self) -> Field<'_> {
        Field {
            name: &self.name,
            value: &self.value,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldBlock {
    fields: Vec<OwnedField>,
}

impl FieldBlock {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) {
        self.fields.push(OwnedField {
            name: name.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn fields(&self) -> &[OwnedField] {
        &self.fields
    }

    pub fn as_fields(&self) -> impl Iterator<Item = Field<'_>> {
        self.fields.iter().map(OwnedField::as_ref)
    }
}
