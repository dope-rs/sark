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
