use o3::{buffer::hold, depot::queue::ring};
use sark_core::manifold::stream::Boundary;

use crate::{
    conn,
    conn::io::{
        outbound::{Pending, section, writes},
        write::{self, Entry, Stage as _, budget},
    },
};

pub(in crate::conn) struct Batch<'q, 'd, S, B> {
    pub(super) pending: Pending<'q, 'd, S, B>,
    pub(super) trailing: Option<Entry<'d>>,
}

impl<'q, 'd, S, B> Batch<'q, 'd, S, B> {
    pub(in crate::conn) fn commit(self) -> Result<hold::Charge, conn::Error> {
        let qpack = match self.pending.encode_qpack() {
            Ok(qpack) => qpack,
            Err(error) => {
                self.pending.discard();
                return Err(error);
            }
        };
        if let Err(error) = self.check_retention() {
            self.pending.discard();
            return Err(error);
        }
        writes::commit(
            self,
            qpack.map(|(target, prefix)| {
                (
                    target,
                    Entry::single(budget::WriteClass::Critical, prefix, Boundary::More),
                )
            }),
        )
    }

    pub(in crate::conn) fn defer(mut self) -> Result<conn::io::replies::Pending<'d>, conn::Error> {
        if let Err(error) = self.stage() {
            self.pending.discard();
            return Err(error);
        }
        let section::Section { entry, qpack, .. } = self.pending.fields;
        qpack.defer();
        Ok(conn::io::replies::Pending::new(entry, self.trailing))
    }

    fn stage(&mut self) -> Result<(), conn::Error> {
        let pending = &mut self.pending;
        let mut charge = pending.fields.entry.charge();
        let count = if let Some(trailing) = &self.trailing {
            charge = budget::WriteClass::Regular.try_add(charge, trailing.charge())?;
            2
        } else {
            1
        };
        if count > pending.queues.regular_write_limit {
            return Err(conn::Error::QueueRegular {
                queued: 0,
                additional: count,
                limit: pending.queues.regular_write_limit,
            });
        }
        pending
            .queues
            .write_budget
            .check_pending(charge, budget::WriteClass::Regular)?;
        let Some((target, mut prefix)) = pending.encode_qpack()? else {
            return Ok(());
        };
        if pending
            .pending_encoder
            .as_ref()
            .is_none_or(ring::Fifo::is_empty)
        {
            let rejected = match write::Single::new(pending.queues, target, prefix) {
                Ok(permit) => {
                    permit.commit::<hold::evidence::Off>();
                    return Ok(());
                }
                Err(rejected) => rejected,
            };
            let ([owner], error) = rejected.into_parts();
            if !pending.queues.is_pressure(&error) {
                return Err(error);
            }
            prefix = owner;
        }
        pending
            .queues
            .write_budget
            .check_pending(prefix.charge(), budget::WriteClass::Critical)?;
        if pending.pending_encoder.is_none() {
            *pending.pending_encoder = Some(
                ring::Fifo::try_new(pending.encoder_capacity).map_err(|error| {
                    conn::Error::Encode(crate::qpack::encoder::Error::Allocation(error))
                })?,
            );
        }
        let Some(queue) = pending.pending_encoder.as_mut() else {
            unreachable!("encoder");
        };
        let Some(vacant) = queue.vacant() else {
            // Every queued batch retains at least one unacknowledged insertion.
            // This new insertion fits the same bounded table and cannot evict it.
            unreachable!("encoder");
        };
        vacant.push_back((prefix, pending.fields.qpack.base()));
        Ok(())
    }

    fn check_retention(&self) -> Result<(), conn::Error> {
        let fields = &self.pending.fields.entry;
        if let Some(entry) = self.trailing.as_ref()
            && fields.charge().try_add(entry.charge()).is_err()
        {
            // Preserve retention-error priority when the aggregate cannot fit;
            // fitting wire/resident sums already bound every retained component.
            fields
                .retained()
                .try_add(entry.retained())
                .map_err(conn::Error::Retention)?;
        }
        Ok(())
    }
}
