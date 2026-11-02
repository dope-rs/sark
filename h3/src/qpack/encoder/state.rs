use sark_http::payload::compress::dynamic::mode;

use crate::qpack::{dynamics, ledger, stream};

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
}

impl State {
    pub(super) fn new(tracking: Tracking) -> Self {
        Self {
            known_received_count: 0,
            max_blocked_streams: 0,
            outstanding: tracking,
        }
    }

    pub(super) fn apply(&mut self, instruction: stream::Instruction) -> Result<(), stream::Error> {
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
                let inserted = match &self.outstanding {
                    Tracking::Disabled => 0,
                    Tracking::Active { table, .. } => table.view().insert_count(),
                };
                if received > inserted {
                    return Err(stream::Error::IncrementBeyondTable {
                        known,
                        increment: delta.get(),
                        inserted,
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
