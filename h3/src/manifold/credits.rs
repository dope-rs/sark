//! QUIC stream-credit ownership for HTTP/3 manifold sessions.

use std::num::NonZero;

use dope_quic::{
    conn::stream::recv::{self, credit},
    stream,
    wire::stream::Id,
};
use o3::{
    alloc, buffer,
    depot::map::{self, flat},
    mem::Rejected,
};

use crate::manifold::Credit;

pub(super) struct Entry {
    stream_id: Id,
    credit: credit::Detached<credit::Local>,
}

/// Fixed, connection-lifetime bridge between QUIC's linear receive-credit
/// proofs and HTTP/3's byte-oriented parser progress.
pub(super) struct Ledger {
    entries: flat::Table<Entry>,
}

pub(super) enum Hold<'ledger, 'scope> {
    Vacant {
        entry: map::Vacancy<&'ledger mut flat::Table<Entry>, u64>,
        stream_id: Id,
        credit: credit::Credit<'scope>,
    },
    Merge(credit::Merge<'ledger, 'scope>),
}

pub(super) enum Rejection {
    Capacity,
    Mismatch { held: usize },
}

impl Ledger {
    pub(super) fn try_new(plan: map::Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            entries: flat::Table::try_new(plan)?,
        })
    }

    pub(super) fn try_hold<
        'ledger,
        'scope,
        const ID: u8,
        B: stream::Recv,
        const RECONCILE: bool,
        D: buffer::raw::Segment,
    >(
        &'ledger mut self,
        id: Id,
        credit: credit::Credit<'scope>,
        recv: &mut recv::Receive<'_, 'scope, ID, B, RECONCILE, D>,
    ) -> Result<Hold<'ledger, 'scope>, Rejected<credit::Credit<'scope>, Rejection>> {
        let hash = id.number();
        let Some(entry) = self.entries.entry(hash, |entry| entry.stream_id == id) else {
            return Err(Rejected::new(credit, Rejection::Capacity));
        };
        match entry {
            flat::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                let held = entry.credit.len();
                entry
                    .credit
                    .try_admit(credit, recv)
                    .map(Hold::Merge)
                    .map_err(|credit| Rejected::new(credit, Rejection::Mismatch { held }))
            }
            flat::Entry::Vacant(entry) => Ok(Hold::Vacant {
                entry,
                stream_id: id,
                credit,
            }),
        }
    }

    pub(super) fn try_take(
        &mut self,
        id: Id,
        bytes: NonZero<usize>,
    ) -> Result<credit::Detached<credit::Local>, Credit> {
        let hash = id.number();
        let mut entry = self
            .entries
            .occupied(hash, |entry| entry.stream_id == id)
            .ok_or(Credit::Missing {
                stream_id: id,
                requested: bytes,
            })?;
        let held = entry.get().credit.len();
        let credit = entry
            .get_mut()
            .credit
            .take_prefix(bytes)
            .ok_or(Credit::Insufficient {
                stream_id: id,
                requested: bytes,
                held,
            })?;
        if entry.get().credit.is_empty() {
            entry.remove();
        }
        Ok(credit)
    }

    pub(super) fn remove(&mut self, id: Id) -> Option<credit::Detached<credit::Local>> {
        self.entries
            .remove(id.number(), |entry| entry.stream_id == id)
            .map(|entry| entry.credit)
    }
}

impl<'scope> Hold<'_, 'scope> {
    pub(super) fn commit<
        const ID: u8,
        B: stream::Recv,
        const RECONCILE: bool,
        D: buffer::raw::Segment,
    >(
        self,
        recv: &mut recv::Receive<'_, 'scope, ID, B, RECONCILE, D>,
    ) {
        match self {
            Self::Vacant {
                entry,
                stream_id: id,
                credit,
            } => {
                let credit = credit.release_conn(recv).detach(recv);
                entry.insert(Entry {
                    stream_id: id,
                    credit,
                });
            }
            Self::Merge(merge) => merge.commit(recv),
        }
    }
}
