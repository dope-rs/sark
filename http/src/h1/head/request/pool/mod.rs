use o3::buffer::{
    pool,
    pool::{CreateError, LayoutError},
};

mod buf;
mod layout;
mod meta;
mod open;

pub use buf::Buf;
pub(in crate::h1::head::request) use layout::Layout;
pub use meta::Meta;
pub use open::Open;

pub struct Pool {
    inner: pool::Pool,
}

impl Pool {
    pub fn try_new(slots: usize, capacity: usize) -> Result<Self, CreateError> {
        if capacity > Layout::MAX {
            return Err(CreateError::Layout(LayoutError::CapacityTooLarge));
        }
        Ok(Self {
            inner: pool::Pool::try_new(slots, capacity + Layout::BYTES)?,
        })
    }

    pub fn open(&self) -> Option<Open<'_>> {
        self.inner.borrow().map(Open::new)
    }

    pub fn slots(&self) -> usize {
        self.inner.slots()
    }

    pub fn available(&self) -> usize {
        self.inner.available()
    }
}

const _: () = assert!(size_of::<Pool>() == size_of::<usize>());
