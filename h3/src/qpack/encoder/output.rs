use o3::buffer::write::{Reserved, Sink as _, Staged};
use sark_http::{
    field,
    field::metadata::{integer, map::record::Entry, strings},
};

use crate::{
    frame,
    qpack::{self, encoder, literal, statics},
};

pub(crate) struct Output {
    lines: Vec<u8>,
}

#[derive(Clone, Copy)]
enum Offset {
    Before(u64),
    After(u64),
}

impl Output {
    pub(crate) const fn new(lines: Vec<u8>) -> Self {
        Self { lines }
    }

    pub(crate) fn use_static(&mut self, index: statics::Index) -> Result<(), encoder::Error> {
        let index = integer::PrefixedInt::<6>::new(index.get());
        self.write(index.encoded_len().get(), |out| index.encode(0xc0, out))
    }

    pub(crate) fn use_dynamic(
        &mut self,
        index: qpack::Reference,
        base: u64,
    ) -> Result<(), encoder::Error> {
        let absolute = index.absolute();
        if absolute < base {
            let index = integer::PrefixedInt::<6>::new(base - absolute - 1);
            self.write(index.encoded_len().get(), |out| index.encode(0x80, out))
        } else {
            let index = integer::PrefixedInt::<4>::new(absolute - base);
            self.write(index.encoded_len().get(), |out| index.encode(0x10, out))
        }
    }

    pub(crate) fn literal(
        &mut self,
        field: Entry<'_>,
        encoding: strings::Encoding,
    ) -> Result<(), encoder::Error> {
        let name = literal::Raw::try_new(field.name, encoding)?;
        let name_len = literal_len::<3>(&name)?;
        let value = literal::Raw::try_new(field.value, encoding)?;
        let size = literal_len::<7>(&value)?;
        self.write(sum(name_len, size)?, |out| {
            name.encode::<3>(literal_prefix(field.indexing), out);
            value.encode::<7>(0, out);
        })
    }

    pub(crate) fn copy_static(
        &mut self,
        index: statics::Index,
        value: &[u8],
        encoding: strings::Encoding,
    ) -> Result<(), encoder::Error> {
        let index = integer::PrefixedInt::<4>::new(index.get());
        let value = literal::Raw::try_new(value, encoding)?;
        let size = literal_len::<7>(&value)?;
        self.write(sum(index.encoded_len().get(), size)?, |out| {
            index.encode(0x50, out);
            value.encode::<7>(0, out);
        })
    }

    pub(crate) fn copy_dynamic(
        &mut self,
        index: qpack::Reference,
        base: u64,
        value: &[u8],
        encoding: strings::Encoding,
    ) -> Result<(), encoder::Error> {
        let absolute = index.absolute();
        let offset = if absolute < base {
            Offset::Before(base - absolute - 1)
        } else {
            Offset::After(absolute - base)
        };
        let width = match offset {
            Offset::Before(index) => integer::PrefixedInt::<4>::new(index).encoded_len().get(),
            Offset::After(index) => integer::PrefixedInt::<3>::new(index).encoded_len().get(),
        };
        let value = literal::Raw::try_new(value, encoding)?;
        let size = literal_len::<7>(&value)?;
        self.write(sum(width, size)?, |out| {
            match offset {
                Offset::Before(index) => {
                    integer::PrefixedInt::<4>::new(index).encode(0x40, out);
                }
                Offset::After(index) => {
                    integer::PrefixedInt::<3>::new(index).encode(0, out);
                }
            }
            value.encode::<7>(0, out);
        })
    }

    pub(crate) fn name(
        &mut self,
        name: &[u8],
        encoding: strings::Encoding,
        indexing: field::Indexing,
    ) -> Result<(), encoder::Error> {
        let name = literal::Raw::try_new(name, encoding)?;
        self.write(literal_len::<3>(&name)?, |out| {
            name.encode::<3>(literal_prefix(indexing), out);
        })
    }

    pub(crate) fn static_name(&mut self, index: statics::Index) -> Result<(), encoder::Error> {
        let index = integer::PrefixedInt::<4>::new(index.get());
        self.write(index.encoded_len().get(), |out| index.encode(0x50, out))
    }

    pub(crate) fn dynamic_name(
        &mut self,
        index: qpack::Reference,
        base: u64,
    ) -> Result<(), encoder::Error> {
        let absolute = index.absolute();
        if absolute < base {
            let index = integer::PrefixedInt::<4>::new(base - absolute - 1);
            self.write(index.encoded_len().get(), |out| index.encode(0x40, out))
        } else {
            let index = integer::PrefixedInt::<3>::new(absolute - base);
            self.write(index.encoded_len().get(), |out| index.encode(0, out))
        }
    }

    fn write(
        &mut self,
        len: usize,
        encode: impl FnOnce(&mut frame::Writer<'_>),
    ) -> Result<(), encoder::Error> {
        frame::Output::Owned(Reserved::try_grow(&mut self.lines, len)?)
            .try_write(encode)
            .map_err(Into::into)
    }

    pub(crate) fn encoded<E: field::Encode>(
        &mut self,
        value: E,
        len: usize,
    ) -> Result<&[u8], encoder::Error> {
        let wire = len as u64;
        let prefix = integer::PrefixedInt::<7>::new(wire).pack::<11>(0);
        let expected = sum(prefix.len(), len)?;
        let mut writer = Staged::try_grow(&mut self.lines, expected)?;
        writer
            .write_slice(prefix.as_ref())
            .and_then(|()| value.try_write(|bytes| writer.write_slice(bytes)))
            .map_err(|error| {
                let (required, room) = error.into_parts();
                encoder::Error::WireWrite {
                    expected,
                    written: expected - room,
                    additional: required.get(),
                }
            })?;
        let suffix = writer
            .try_complete()
            .map_err(|error| {
                let (_, actual) = error.into_parts();
                encoder::Error::WireLength { expected, actual }
            })?
            .commit();
        Ok(&suffix[prefix.len()..])
    }

    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.lines
    }
}

fn literal_prefix(indexing: field::Indexing) -> u8 {
    match indexing {
        field::Indexing::Allowed => 0x20,
        field::Indexing::Never => 0x30,
    }
}

fn literal_len<const BITS: u8>(literal: &literal::Raw<'_>) -> Result<usize, encoder::Error>
where
    integer::PrefixedInt<BITS>: integer::Width,
{
    let len = literal.encoded_len();
    let wire = len as u64;
    sum(
        integer::PrefixedInt::<BITS>::new(wire).encoded_len().get(),
        len,
    )
}

fn sum(left: usize, right: usize) -> Result<usize, encoder::Error> {
    left.checked_add(right)
        .ok_or(encoder::Error::Capacity { left, right })
}

const _: () = assert!(usize::BITS <= u64::BITS);
