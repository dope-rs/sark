use core::{marker::PhantomData, ops};

use insert::write;
use o3::buffer::write::Sink;

use crate::{
    field::metadata::{
        block::{self, buffer::Prefix},
        map::record,
    },
    payload::compress::dynamic::{
        insert,
        insert::{draft, value},
        mode, table,
    },
};

/// Dynamic table ready for an insertion.
pub struct Ready(table::Table<mode::Staged>);

/// Dynamic table writing an insertion name.
pub struct Name(table::Table<mode::Staged>);

/// Dynamic table writing an insertion value.
pub struct Value(table::Table<mode::Staged>);

/// Writable state shared by the name and value insertion stages.
pub trait Write {
    fn written(&self) -> usize;
    fn writer(&mut self) -> insert::Writer<'_>;
    fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error>;
}

/// Result of starting an owned insertion.
pub enum Begin {
    Name(Name),
    Omitted(Ready),
}

impl Ready {
    pub const fn new(table: table::Table<mode::Staged>) -> Self {
        Self(table)
    }

    pub fn begin(mut self) -> Begin {
        match self.0.try_begin() {
            Ok(_) => Begin::Name(Name(self.0)),
            Err(_) => Begin::Omitted(self),
        }
    }
}

impl ops::Deref for Ready {
    type Target = table::Table<mode::Staged>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Ready {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Name {
    pub fn table(&self) -> &table::Table<mode::Staged> {
        &self.0
    }

    pub fn copy(mut self, index: usize) -> Result<Value, (Self, insert::Error)> {
        match self.0.name().copy_name(index) {
            Ok(()) => Ok(Value(self.0)),
            Err(error) => Err((self, error)),
        }
    }

    pub fn set_name(mut self, name: block::Named<'_>) -> Result<Value, (Self, insert::Error)> {
        match self.0.name().replace_name(name) {
            Ok(()) => Ok(Value(self.0)),
            Err(error) => Err((self, error)),
        }
    }

    pub fn span(&self) -> Result<record::Span<'_>, insert::Error> {
        draft::span(&self.0, self.0.storage.state().insert.len.get())
    }

    pub fn prefix(&self) -> Result<Prefix<'_, block::Scan>, insert::Error> {
        Ok(Prefix::from_scanned(self.span()?, self.scan()))
    }

    fn scan(&self) -> block::Scan {
        self.0.storage.state().insert.name
    }

    pub fn finish(mut self) -> Result<Value, (Self, insert::Error)> {
        match self.0.storage.state_mut().insert.finish() {
            Ok(()) => Ok(Value(self.0)),
            Err(error) => Err((self, error)),
        }
    }

    pub fn discard(mut self) -> Ready {
        self.0.rollback();
        Ready(self.0)
    }
}

impl Write for Name {
    fn written(&self) -> usize {
        self.0.storage.state().insert.len.get() as usize
    }

    fn writer(&mut self) -> insert::Writer<'_> {
        self.0.name().into_writer()
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error> {
        self.0.name().push(bytes)
    }
}

impl Value {
    pub fn proof(&self) -> block::Name {
        self.0.storage.state().insert.proof()
    }

    pub fn truncate(&mut self, len: usize) -> Result<(), insert::Error> {
        self.0.value().truncate(len)
    }

    pub fn validation(&self) -> block::Validation {
        self.0.storage.state().insert.value.finish()
    }

    pub fn table(&self) -> &table::Table<mode::Staged> {
        &self.0
    }

    pub fn max_size(&self) -> usize {
        self.0.max_size()
    }

    pub fn name(&self) -> Result<record::Span<'_>, insert::Error> {
        draft::span(&self.0, self.0.storage.state().insert.name_len())
    }

    pub fn named(&self) -> Result<block::Named<'_>, insert::Error> {
        Ok(block::Named::from_scanned(self.name()?, self.proof()))
    }

    pub fn value(&self) -> Result<record::Span<'_>, insert::Error> {
        value::project(&self.0)
    }

    pub fn valued(&self) -> Result<block::Valued<'_>, insert::Error> {
        Ok(block::Valued::from_scanned(
            self.value()?,
            self.validation(),
        ))
    }

    pub fn value_prefix(&self) -> Result<Prefix<'_, block::Value>, insert::Error> {
        Ok(Prefix::from_scanned(
            self.value()?,
            self.0.storage.state().insert.value,
        ))
    }

    pub fn commit(mut self) -> Ready {
        self.0.value().commit();
        Ready(self.0)
    }

    pub fn discard(mut self) -> Ready {
        self.0.rollback();
        Ready(self.0)
    }
}

impl Write for Value {
    fn written(&self) -> usize {
        self.0.storage.state().insert.len.get() as usize
    }

    fn writer(&mut self) -> insert::Writer<'_> {
        self.0.value().into_writer()
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error> {
        self.0.value().push(bytes)
    }
}

/// One table owner whose protocol state retains its current insertion proof.
pub struct Owner<R, N, V> {
    table: table::Table<mode::Staged>,
    phase: View<R, N, V>,
}

pub enum View<R, N, V> {
    Ready(R),
    Name(N),
    Value(V),
}

pub enum Borrowed<'a, R, N, V> {
    Ready(R, Loan<'a, Ready, R, N, V>),
    Name(N, Loan<'a, Name, R, N, V>),
    Value(V, Loan<'a, Value, R, N, V>),
}

/// Exclusive projection of a stage and the same owner's protocol-state slot.
pub struct Loan<'a, S, R, N, V> {
    owner: &'a mut Owner<R, N, V>,
    stage: PhantomData<fn() -> S>,
}

impl<R, N, V> Owner<R, N, V> {
    pub fn new(table: table::Table<mode::Staged>, phase: R) -> Self {
        Self {
            table,
            phase: View::Ready(phase),
        }
    }

    pub fn table(&self) -> &table::Table<mode::Staged> {
        &self.table
    }

    pub fn reset(&mut self, phase: R) {
        self.table.rollback();
        self.phase = View::Ready(phase);
    }

    pub fn writing(
        &mut self,
    ) -> View<(), (&mut N, insert::Draft<'_>), (&mut V, insert::Draft<'_>)> {
        match &mut self.phase {
            View::Ready(_) => View::Ready(()),
            View::Name(value) => View::Name((value, insert::Draft::new(&mut self.table))),
            View::Value(value) => View::Value((value, insert::Draft::new(&mut self.table))),
        }
    }
}

impl<R: Copy, N: Copy, V: Copy> Owner<R, N, V> {
    pub fn view(&mut self) -> Borrowed<'_, R, N, V> {
        match self.phase {
            View::Ready(value) => Borrowed::Ready(value, Loan::new(self)),
            View::Name(value) => Borrowed::Name(value, Loan::new(self)),
            View::Value(value) => Borrowed::Value(value, Loan::new(self)),
        }
    }
}

impl<'a, S, R, N, V> Loan<'a, S, R, N, V> {
    fn new(owner: &'a mut Owner<R, N, V>) -> Self {
        Self {
            owner,
            stage: PhantomData,
        }
    }

    pub fn table(&self) -> &table::Table<mode::Staged> {
        &self.owner.table
    }
}

impl<'a, R, N, V> Loan<'a, Ready, R, N, V> {
    pub fn table_mut(&mut self) -> &mut table::Table<mode::Staged> {
        &mut self.owner.table
    }

    pub fn pending(self, phase: R) {
        self.owner.phase = View::Ready(phase);
    }

    pub fn begin(self) -> Result<Loan<'a, Name, R, N, V>, insert::Capacity> {
        self.owner.table.try_begin()?;
        Ok(Loan::new(self.owner))
    }
}

impl<R, N, V> Loan<'_, Name, R, N, V> {
    pub fn pending(self, phase: N) {
        self.owner.phase = View::Name(phase);
    }

    pub fn copy(self, index: usize, phase: V) -> Result<(), insert::Error> {
        self.owner.table.name().copy_name(index)?;
        self.owner.phase = View::Value(phase);
        Ok(())
    }

    pub fn set_name(self, name: block::Named<'_>, phase: V) -> Result<(), insert::Error> {
        self.owner.table.name().replace_name(name)?;
        self.owner.phase = View::Value(phase);
        Ok(())
    }

    pub fn finish(self, phase: V) -> Result<(), insert::Error> {
        self.owner.table.storage.state_mut().insert.finish()?;
        self.owner.phase = View::Value(phase);
        Ok(())
    }
}

impl<R, N, V> Loan<'_, Value, R, N, V> {
    pub fn pending(self, phase: V) {
        self.owner.phase = View::Value(phase);
    }

    pub fn commit(self, phase: R) {
        self.owner.table.value().commit();
        self.owner.phase = View::Ready(phase);
    }
}

impl<S: Write, R, N, V> Write for Loan<'_, S, R, N, V> {
    fn written(&self) -> usize {
        self.owner.table.storage.state().insert.len.get() as usize
    }

    fn writer(&mut self) -> insert::Writer<'_> {
        insert::Writer::new(&mut self.owner.table)
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error> {
        self.writer().write_slice(bytes)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Ready>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Name>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Value>() == core::mem::size_of::<usize>());
};
