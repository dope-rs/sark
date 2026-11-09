use o3::alloc;
use sark_http::{
    field::metadata::strings,
    payload::compress::dynamic::{hash, mode, plan, table::delta::buffer::Storage},
};

use crate::qpack::{
    dynamics,
    encoder::{Capacity, Error},
    ledger, stream,
};

pub(in crate::qpack) enum Tracking {
    Disabled,
    Active {
        table: dynamics::Table<mode::Direct>,
        ledger: ledger::Ledger,
    },
}

const _: () = assert!(
    core::mem::size_of::<Tracking>()
        == core::mem::size_of::<(dynamics::Table<mode::Direct>, ledger::Ledger)>()
);

pub(crate) struct State {
    pub(super) known_received_count: u64,
    pub(super) max_blocked_streams: u64,
    pub(super) outstanding: Tracking,
    pub(super) encoding: strings::Encoding,
    pub(super) section: Vec<u8>,
    pub(super) staged: Option<Storage>,
}

impl State {
    pub(super) fn new(tracking: Tracking) -> Self {
        Self {
            known_received_count: 0,
            max_blocked_streams: 0,
            outstanding: tracking,
            encoding: strings::Encoding::Plain,
            section: Vec::new(),
            staged: None,
        }
    }

    pub(crate) fn try_plan(
        table: plan::Plan<mode::Direct>,
        sections: ledger::Plan,
        seed: hash::Seed,
    ) -> Result<Self, alloc::Error> {
        let table = dynamics::Table::try_new(table, 0, seed)?;
        Ok(Self::new(Tracking::Active {
            table,
            ledger: ledger::Ledger::try_new(sections)?,
        }))
    }

    pub(crate) fn allow(&mut self, maximum: u64) {
        if let Tracking::Active { table, .. } = &mut self.outstanding {
            table.allow(maximum);
        }
    }

    pub(crate) fn maximum(&self) -> usize {
        match &self.outstanding {
            Tracking::Active { table, .. } => table.maximum(),
            Tracking::Disabled => 0,
        }
    }

    pub(crate) fn insert_count(&self) -> u64 {
        match &self.outstanding {
            Tracking::Active { table, .. } => table.view().insert_count(),
            Tracking::Disabled => 0,
        }
    }

    pub(crate) fn initial(&mut self, stream: u64) -> Option<ledger::Occupied<'_>> {
        match &mut self.outstanding {
            Tracking::Active { ledger, .. } => ledger.initial(stream),
            Tracking::Disabled => None,
        }
    }

    pub(crate) fn discard_initial(&mut self, stream: u64) {
        if let Tracking::Active { ledger, .. } = &mut self.outstanding {
            ledger.discard_initial(stream);
        }
    }

    pub(crate) fn plan_limit(&mut self, capacity: usize) -> Result<Capacity<&mut Self>, Error> {
        let limit = Capacity::new(capacity);
        let floor = self.floor();
        match &self.outstanding {
            Tracking::Active { table, .. } => table.check_floor(capacity, floor)?,
            Tracking::Disabled if capacity != 0 => {
                return Err(dynamics::Capacity::Maximum {
                    requested: capacity,
                    maximum: 0,
                }
                .into());
            }
            Tracking::Disabled => {}
        }
        Ok(limit.bind(self))
    }

    pub(crate) fn set_blocked(&mut self, blocked: u64) {
        self.max_blocked_streams = blocked;
    }

    pub(crate) fn apply(
        &mut self,
        instruction: stream::Instruction,
        published: u64,
    ) -> Result<(), stream::Error> {
        match instruction {
            stream::Instruction::SectionAcknowledgment { stream_id: stream } => {
                let received = self.known_received_count;
                let ledger = match &mut self.outstanding {
                    Tracking::Disabled => {
                        return Err(stream::Error::UnknownSection { stream_id: stream });
                    }
                    Tracking::Active { ledger, .. } => ledger,
                };
                let required = ledger.acknowledge(stream, received)?;
                if required > self.known_received_count {
                    self.known_received_count = required;
                }
            }
            stream::Instruction::StreamCancellation { stream_id: stream } => {
                if let Tracking::Active { ledger, .. } = &mut self.outstanding {
                    ledger.cancel(stream);
                }
            }
            stream::Instruction::InsertCountIncrement { increment: delta } => {
                let known = self.known_received_count;
                let Some(advance) = ledger::Advance::from_step(known, delta.get()) else {
                    return Err(stream::Error::IncrementOverflow {
                        known,
                        increment: delta.get(),
                    });
                };
                let received = advance.received();
                if received > published {
                    return Err(stream::Error::IncrementBeyondTable {
                        known,
                        increment: delta.get(),
                        inserted: published,
                    });
                }
                if let Tracking::Active { ledger, .. } = &mut self.outstanding {
                    ledger.advance(advance);
                }
                self.known_received_count = received;
            }
        }
        Ok(())
    }

    /// Unacknowledged insertions and outstanding references both protect this
    /// absolute index and every newer entry from eviction.
    pub(super) fn floor(&self) -> u64 {
        match &self.outstanding {
            Tracking::Disabled => 0,
            Tracking::Active { ledger, .. } => {
                let received = self.known_received_count;
                ledger.floor().map_or(received, |floor| floor.min(received))
            }
        }
    }
}
