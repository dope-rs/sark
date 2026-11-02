//! Admitted application response headers and their emission contract.

pub mod head;
mod list;
mod name;
pub mod set;
pub mod value;

pub use list::List;
pub use name::Name;

use crate::field;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Managed,
    Syntax(field::Error),
}

/// An ordinary response field whose name cannot override framework framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Entry<'a>(field::Entry<'a>);

/// A preencoded group whose fields cannot override framework framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Group<'a>(field::Group<'a>);

impl<'a> Group<'a> {
    pub const fn try_new(group: field::Group<'a>) -> Result<Self, Error> {
        let entries = group.entries();
        let mut index = 0;
        while index < entries.len() {
            if let Err(error) = Name::try_bytes(entries[index].name()) {
                return Err(error);
            }
            index += 1;
        }
        Ok(Self(group))
    }

    pub const fn as_fields(&self) -> &field::Group<'a> {
        &self.0
    }
}

impl<'a> Entry<'a> {
    pub const fn new(name: Name<&'a [u8]>, value: field::Value<&'a [u8]>) -> Self {
        Self(field::Entry::new(name.0, value))
    }

    pub const fn with_index(self, indexing: field::Indexing) -> Self {
        Self(self.0.with_index(indexing))
    }

    pub const fn name(self) -> &'a [u8] {
        self.0.name()
    }

    pub const fn value(self) -> field::Value<&'a [u8]> {
        self.0.value()
    }

    pub const fn indexing(self) -> field::Indexing {
        self.0.indexing()
    }

    pub const fn as_field(&self) -> field::Entry<'a> {
        self.0
    }
}

/// Receives admitted ordinary response fields, including generated values.
pub trait Sink<'field> {
    fn entry(&mut self, field: Entry<'field>);

    /// Emits the admitted fields in their original order and indexing policy.
    fn group(&mut self, group: &Group<'field>) {
        for entry in group.0.entries() {
            self.entry(Entry(*entry));
        }
    }

    fn field(&mut self, name: Name<&'field [u8]>, value: field::Value<&'field [u8]>) {
        self.entry(Entry::new(name, value));
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: Name<&'field [u8]>,
        value: E,
        indexing: field::Indexing,
    );

    fn generated<E: field::Encode>(&mut self, name: Name<&'field [u8]>, value: E) {
        self.encoded_with(name, value, field::Indexing::Allowed);
    }
}

/// Emits only ordinary headers whose admission survives every invocation.
pub trait Source {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>);
}

/// Projects response admission into the protocol-neutral field encoder.
#[repr(transparent)]
pub struct Fields<S>(S);

impl<S: Source> Fields<S> {
    pub const fn new(source: S) -> Self {
        Self(source)
    }
}

struct Adapter<'a, S>(&'a mut S);

impl<S: Source> field::Source for Fields<S> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        self.0.emit(&mut Adapter(sink));
    }
}

impl<'field, S: field::Sink<'field>> Sink<'field> for Adapter<'_, S> {
    fn entry(&mut self, field: Entry<'field>) {
        self.0.entry(field.0);
    }

    fn group(&mut self, group: &Group<'field>) {
        self.0.group(group.as_fields());
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: Name<&'field [u8]>,
        value: E,
        indexing: field::Indexing,
    ) {
        self.0.encoded_with(name.0, value, indexing);
    }
}

impl Source for () {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        let fields: &[Entry<'_>] = &[];
        fields.emit(sink);
    }
}

impl Source for Group<'_> {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        sink.group(self);
    }
}

impl<S: Source + ?Sized> Source for &S {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        (**self).emit(sink);
    }
}

impl Source for [Entry<'_>] {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        for entry in self {
            sink.entry(*entry);
        }
    }
}

impl<const N: usize> Source for [Entry<'_>; N] {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        self.as_slice().emit(sink);
    }
}

impl Source for Vec<Entry<'_>> {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        self.as_slice().emit(sink);
    }
}
