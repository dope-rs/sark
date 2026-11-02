use std::num::NonZero;

use o3::buffer::{pool, write::Sink as _};
use sark_http::{
    field::{
        self, Indexing,
        metadata::map::{self, record::Entry},
    },
    payload::response::headers,
};

use crate::common::field::blocks;

pub trait Invalid: Sized {
    fn try_invalid(len: NonZero<usize>) -> Result<Self, blocks::Error>;
}

impl Invalid for pool::Span {
    fn try_invalid(len: NonZero<usize>) -> Result<Self, blocks::Error> {
        let mut lease = pool::Span::try_new(len.get()).map_err(blocks::Error::Pool)?;
        for _ in 1..len.get() {
            lease
                .try_append(&[0])
                .map_err(blocks::Error::Write)?
                .commit();
        }
        lease
            .try_append(&[2])
            .map_err(blocks::Error::Write)?
            .commit();
        Ok(lease)
    }
}

pub const FIELDS: [Entry<'static>; 2] = [
    Entry::new(b"authorization", b"secret").with_index(Indexing::Never),
    Entry::new(b"x-generated", b"private").with_index(Indexing::Never),
];

const GROUP_ENTRIES: [field::Entry<'static>; 2] = [
    field::Entry::new(
        field::literal!(field::Lower::try_bytes(FIELDS[0].name)),
        field::literal!(field::Value::try_bytes(FIELDS[0].value)),
    )
    .with_index(FIELDS[0].indexing),
    field::Entry::new(
        field::literal!(field::Lower::try_bytes(FIELDS[1].name)),
        field::literal!(field::Value::try_bytes(FIELDS[1].value)),
    )
    .with_index(FIELDS[1].indexing),
];

pub const GROUP: headers::Group<'static> = field::literal!(headers::Group::try_new(
    field::literal!(field::Group::try_new(
        &GROUP_ENTRIES,
        b"authorization: secret\r\nx-generated: private\r\n",
    ))
));

pub const EMPTY: field::Group<'static> = field::literal!(field::Group::try_new(&[], b""));

pub const MISMATCHES: [(
    Result<field::Group<'static>, field::Mismatch>,
    field::Mismatch,
); 3] = [
    (
        field::Group::try_new(&GROUP_ENTRIES, b"authorization: Secret"),
        field::Mismatch {
            offset: 15,
            expected: Some(b's'),
            actual: Some(b'S'),
        },
    ),
    (
        field::Group::try_new(&GROUP_ENTRIES, b"authorization: "),
        field::Mismatch {
            offset: 15,
            expected: Some(b's'),
            actual: None,
        },
    ),
    (
        field::Group::try_new(&[], b"!"),
        field::Mismatch {
            offset: 0,
            expected: None,
            actual: Some(b'!'),
        },
    ),
];

const MANAGED_ENTRIES: [field::Entry<'static>; 1] = [field::Entry::new(
    field::literal!(field::Lower::try_bytes(b"date")),
    field::literal!(field::Value::try_bytes(b"x")),
)];

pub const MANAGED: Result<headers::Group<'static>, headers::Error> = headers::Group::try_new(
    field::literal!(field::Group::try_new(&MANAGED_ENTRIES, b"date: x\r\n",)),
);

const PSEUDO_ENTRIES: [field::Entry<'static>; 1] = [field::Entry::pseudo(
    field::Pseudo::Status,
    field::literal!(field::Value::try_bytes(b"200")),
)];

pub const PSEUDO: Result<headers::Group<'static>, headers::Error> = headers::Group::try_new(
    field::literal!(field::Group::try_new(&PSEUDO_ENTRIES, b":status: 200\r\n",)),
);

pub struct Store<'a> {
    pub fields: &'a mut map::Fields<Vec<u8>>,
    pub result: Result<(), map::Error>,
}

impl<'field> field::Sink<'field> for Store<'_> {
    fn entry(&mut self, entry: field::Entry<'field>) {
        if self.result.is_ok() {
            self.result = self.fields.push_entry(entry.as_record());
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        value: E,
        indexing: Indexing,
    ) {
        if self.result.is_ok() {
            self.result =
                self.fields
                    .push_with(name.as_bytes(), value.encoded_len(), indexing, |out| {
                        value.try_write(|bytes| out.write_slice(bytes))
                    });
        }
    }
}

impl<'field> headers::Sink<'field> for Store<'_> {
    fn entry(&mut self, entry: headers::Entry<'field>) {
        field::Sink::entry(self, entry.as_field());
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: headers::Name<&'field [u8]>,
        value: E,
        indexing: Indexing,
    ) {
        field::Sink::encoded_with(self, name.into_inner(), value, indexing);
    }
}
