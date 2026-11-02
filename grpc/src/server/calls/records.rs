use o3::{alloc, depot::capacity::Count};
use sark_core::{
    life::session,
    manifold::{self, schedule},
};
use sark_h2::stream::Key;

use crate::server::{
    call::{self, input},
    calls::{self, capacity},
    runtime::state::active,
    service::handler::Production,
};

pub(in crate::server) struct Records<'r, K, T, C, B>
where
    K: manifold::raw::Key,
    C: session::Storage,
    B: schedule::Block,
{
    pub(super) inner: schedule::Runtime<K, call::State<'r, T, C>, B>,
}

impl<'r, K, T, C, B> Records<'r, K, T, C, B>
where
    K: manifold::raw::Key,
    C: session::Storage,
    B: schedule::Block,
{
    pub(in crate::server) fn try_new(cap: Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: schedule::Runtime::try_bounded(cap, cap)?,
        })
    }

    pub(in crate::server) fn get_mut(&mut self, key: K) -> Option<&mut call::State<'r, T, C>> {
        self.inner.get_mut(key)
    }

    pub(in crate::server) fn initialize(
        &mut self,
        key: K,
        stream: input::Input<C>,
        app: T,
        output: Production,
    ) -> Result<(), calls::Error> {
        let record = self.get_mut(key).ok_or(calls::Error::State)?;
        record
            .initialize(stream, app, output)
            .map_err(calls::Error::Call)
    }

    pub(in crate::server) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub(in crate::server) fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    pub(in crate::server) fn has_runnable(&self) -> bool {
        self.inner.has_runnable()
    }

    pub(in crate::server) fn wake_egress(&mut self) {
        self.inner.wake_egress();
    }

    pub(in crate::server) fn key_at(&self, index: usize) -> Option<K> {
        self.inner.key_at(index)
    }

    pub(in crate::server) fn entry(&self, key: K) -> Option<active::Entry<'r>> {
        Some(self.inner.get(key)?.entry)
    }

    pub(in crate::server) fn entry_at(&self, index: usize) -> Option<active::Entry<'r>> {
        Some(self.inner.entry_at(index)?.1.entry)
    }

    pub(in crate::server) fn production(&self, key: K) -> Option<Production> {
        self.inner.get(key)?.production()
    }

    pub(in crate::server) fn update<R, E>(
        &mut self,
        key: K,
        run: impl FnOnce(&mut call::State<'r, T, C>) -> Result<(Option<schedule::WorkClass>, R), E>,
    ) -> Result<R, schedule::Error<E>> {
        self.inner.transaction(key, |record| {
            let (class, result) = run(record)?;
            let next = match class {
                Some(schedule::WorkClass::Control) => schedule::Activation::control(None),
                Some(schedule::WorkClass::Data) => schedule::Activation::data(None),
                None => schedule::Activation::Keep,
            };
            Ok((next, result))
        })
    }

    pub(in crate::server) fn resume(
        &mut self,
        poll: impl FnMut() -> Option<schedule::Wake<K>>,
        egress: schedule::Egress,
    ) {
        self.inner.resume(poll, egress);
    }

    pub(in crate::server) fn drive<D>(&mut self, driver: &mut D) -> schedule::Report
    where
        D: schedule::Driver<call::State<'r, T, C>, Key = K, Block = B>,
    {
        self.inner.drive(driver)
    }
}

impl<'r, T, C, B> Records<'r, Key, T, C, B>
where
    C: session::Storage,
    B: schedule::Block,
{
    pub(in crate::server) fn try_reserve(
        &mut self,
        entry: active::Entry<'r>,
    ) -> Result<(), calls::Reserve> {
        let active = self.inner.len();
        let limit = self.inner.capacity();
        if active >= limit {
            return Err(calls::Reserve::Capacity(capacity::Error::new(
                active, limit,
            )));
        }
        let vacant = self
            .inner
            .vacant(entry.key())
            .ok_or(calls::Reserve::Occupied)?;
        vacant.store(call::State::reserved(entry));
        Ok(())
    }
}
