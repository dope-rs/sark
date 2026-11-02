use o3::buffer::storage;

use crate::{field::Value, payload::response::headers::Name};

pub(super) enum Inner<'req> {
    Str(Value<&'req str>),
    Borrowed(Value<&'req [u8]>),
    Retained(Value<storage::Shared>),
}

impl<'req> Inner<'req> {
    pub(super) fn borrow(&self) -> Value<&[u8]> {
        match self {
            Self::Str(value) => value.borrow(),
            Self::Borrowed(value) => value.borrow(),
            Self::Retained(value) => value.borrow(),
        }
    }
}

pub struct Item<'req> {
    pub(super) name: Name<&'req [u8]>,
    pub(super) value: Inner<'req>,
}

impl<'req> Item<'req> {
    pub fn from_str(name: Name<&'req [u8]>, value: Value<&'req str>) -> Self {
        Self {
            name,
            value: Inner::Str(value),
        }
    }

    pub fn borrow(name: Name<&'req [u8]>, value: Value<&'req [u8]>) -> Self {
        Self {
            name,
            value: Inner::Borrowed(value),
        }
    }

    pub fn retain(name: Name<&'req [u8]>, value: Value<storage::Shared>) -> Self {
        Self {
            name,
            value: Inner::Retained(value),
        }
    }

    pub(crate) const fn placeholder() -> Self {
        Self {
            name: crate::field::literal!(Name::try_bytes(b"x")),
            value: Inner::Borrowed(crate::field::literal!(Value::try_bytes(b""))),
        }
    }

    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    pub fn value_bytes(&self) -> &[u8] {
        self.value.borrow().into_inner()
    }
}
