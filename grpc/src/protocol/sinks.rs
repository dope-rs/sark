use sark_http::{field, field::Entry};

use crate::protocol;

pub(super) struct Count {
    pub(super) capacity: Result<usize, crate::Status>,
}

impl<'field> field::Sink<'field> for Count {
    fn entry(&mut self, field: Entry<'field>) {
        if let Ok(capacity) = self.capacity {
            self.capacity =
                protocol::Block::add_capacity(capacity, field.name(), field.value().get().len());
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        value: E,
        _: field::Indexing,
    ) {
        let len = value.encoded_len();
        if let Ok(capacity) = self.capacity {
            self.capacity = protocol::Block::add_capacity(capacity, name.get(), len);
        }
    }
}

pub(super) struct Block<'a> {
    pub(super) block: &'a mut protocol::Block,
    pub(super) error: Option<crate::Status>,
}

impl<'field, 'b> field::Sink<'field> for Block<'b> {
    fn entry(&mut self, field: Entry<'field>) {
        if self.error.is_none()
            && let Err(error) = self.block.push_field(field)
        {
            self.error = Some(error);
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        value: E,
        indexing: field::Indexing,
    ) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) = self.block.push_encoded(name, value, indexing) {
            self.error = Some(error);
        }
    }
}
