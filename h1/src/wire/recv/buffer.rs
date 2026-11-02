use std::mem;

use o3::buffer::{
    bytes::window::Snapshot,
    storage,
    write::{Sink as _, consume::Consume as _},
};

use crate::{pump, wire::recv::BODY_CAPACITY};

/// Per-connection accumulator allocated only for split frames or task backlog.
#[derive(Default)]
pub(crate) struct Buffer {
    tail: Snapshot<BODY_CAPACITY>,
}

impl Buffer {
    pub(crate) fn len(&self) -> usize {
        self.tail.len()
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.tail.is_empty()
    }

    /// Moves the buffered logical suffix into a shared owner without copying.
    pub(crate) fn take_ready(&mut self) -> Option<storage::Shared> {
        mem::take(&mut self.tail).into_shared()
    }

    /// Combines input under the owner bound; parsing separates head and body bytes.
    pub(crate) fn prepare(
        &mut self,
        incoming: &[u8],
    ) -> Result<Option<storage::Shared>, pump::Reject> {
        if self.tail.is_empty() {
            return Ok(None);
        }
        Self::reject(
            pump::Operation::Prepare,
            self.tail.write_slice(incoming).map_err(Self::allocation),
        )?;
        let retained = self.tail.try_snapshot().map_err(pump::Cause::Retain);
        Self::reject(pump::Operation::Prepare, retained)
    }

    pub(crate) fn consume(&mut self, amount: usize) -> Result<(), pump::Reject> {
        let consumed = self.tail.try_consume(amount).map_err(pump::Cause::Consume);
        Self::reject(pump::Operation::Consume, consumed)?;
        if self.tail.is_empty() {
            self.tail = Snapshot::new();
        }
        Ok(())
    }

    pub(crate) fn backlog(&mut self, op: pump::Operation, src: &[u8]) -> Result<(), pump::Reject> {
        Self::reject(op, self.tail.write_slice(src).map_err(Self::allocation))
    }

    fn allocation(error: storage::AllocError) -> pump::Cause {
        match error {
            storage::AllocError::Capacity(error) => pump::Cause::Capacity(error),
            storage::AllocError::Exhausted(error) => pump::Cause::Exhausted(error),
        }
    }

    fn reject<T>(op: pump::Operation, result: Result<T, pump::Cause>) -> Result<T, pump::Reject> {
        result.map_err(|cause| pump::Reject::new(op, cause))
    }
}

const _: () = assert!(mem::size_of::<Buffer>() == mem::size_of::<Snapshot<BODY_CAPACITY>>());
