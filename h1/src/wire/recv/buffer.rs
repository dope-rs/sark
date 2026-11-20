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
        let operation = pump::Operation::Prepare;
        self.backlog(operation, incoming)?;
        self.tail
            .try_snapshot()
            .map_err(|cause| pump::Reject::Retain { operation, cause })
    }

    pub(crate) fn consume(&mut self, amount: usize) -> Result<(), pump::Reject> {
        self.tail
            .try_consume(amount)
            .map_err(|cause| pump::Reject::Consume {
                operation: pump::Operation::Consume,
                cause,
            })?;
        if self.tail.is_empty() {
            self.tail = Snapshot::new();
        }
        Ok(())
    }

    pub(crate) fn backlog(
        &mut self,
        operation: pump::Operation,
        src: &[u8],
    ) -> Result<(), pump::Reject> {
        self.tail.write_slice(src).map_err(|error| match error {
            storage::AllocError::Capacity(cause) => pump::Reject::Capacity { operation, cause },
            storage::AllocError::Exhausted(cause) => pump::Reject::Exhausted { operation, cause },
        })
    }
}

const _: () = assert!(mem::size_of::<Buffer>() == mem::size_of::<Snapshot<BODY_CAPACITY>>());
