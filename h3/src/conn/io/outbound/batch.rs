use o3::buffer::hold;
use sark_core::manifold::stream::Boundary;

use crate::{
    conn,
    conn::{
        io::{
            ingress::stream::table::Outgoing,
            outbound::{section, writes},
            write::{self, Entry, budget},
        },
        output,
    },
    stream,
};

pub(in crate::conn) struct Batch<'q, 'd, S, B> {
    pub(super) fields: section::Section<'q, 'd>,
    pub(super) trailing: Option<Entry<'d>>,
    pub(super) queues: &'q mut write::Queues<'d, S, B>,
    pub(super) target: Option<stream::Target>,
    pub(super) outgoing: Option<Outgoing<'q, 'd>>,
}

impl<'q, 'd, S, B> Batch<'q, 'd, S, B> {
    pub(in crate::conn) const fn new(
        fields: section::Section<'q, 'd>,
        trailing: Option<Entry<'d>>,
        queues: &'q mut write::Queues<'d, S, B>,
        target: Option<stream::Target>,
        outgoing: Option<Outgoing<'q, 'd>>,
    ) -> Self {
        Self {
            fields,
            trailing,
            queues,
            target,
            outgoing,
        }
    }

    pub(in crate::conn) fn commit(self) -> Result<hold::Charge, conn::Error> {
        let qpack = match self.encode_qpack() {
            Ok(qpack) => qpack,
            Err(error) => {
                self.discard();
                return Err(error);
            }
        };
        let retained = match self.retained() {
            Ok(retained) => retained,
            Err(error) => {
                self.discard();
                return Err(error);
            }
        };
        writes::commit(self, qpack, retained)
    }

    fn retained(&self) -> Result<hold::Charge, conn::Error> {
        let charge = self.fields.entry.retained();
        match self.trailing.as_ref() {
            Some(entry) => {
                let retained = entry.retained();
                charge.try_add(retained).map_err(conn::Error::Retention)
            }
            None => Ok(charge),
        }
    }

    fn encode_qpack(&self) -> Result<Option<Entry<'d>>, conn::Error> {
        let pending = self.fields.qpack.pending_len();
        let wire = self.fields.qpack.wire_len();
        let len = pending
            .checked_add(wire)
            .ok_or(conn::Error::QpackLength { pending, wire })?;
        if len == 0 {
            return Ok(None);
        }
        let target = self.target.ok_or(conn::Error::EncoderMissing)?;
        let prefix = output::WritePrefix::try_write(len, |writer| {
            self.fields.qpack.encode_wire(writer);
        })?;
        Ok(Some(Entry::single(
            budget::WriteClass::Critical,
            target,
            prefix,
            Boundary::More,
        )))
    }

    fn discard(self) {
        self.fields.discard();
    }
}
