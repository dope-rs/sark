mod batch;
mod section;
mod writes;

use o3::depot::{capacity, queue::ring};

pub(in crate::conn) use batch::Batch;
pub(in crate::conn) use section::Section;

use crate::{
    conn,
    conn::io::{
        ingress::stream::table::Outgoing,
        write::{self, Entry},
    },
    conn::output,
    stream,
};

pub(in crate::conn) struct Pending<'a, 'd, S, B> {
    fields: Section<'a, 'd>,
    queues: &'a mut write::Queues<'d, S, B>,
    qpack: Option<stream::Target>,
    outgoing: Option<Outgoing<'a, 'd>>,
    pending_encoder: &'a mut Option<ring::Fifo<(output::WritePrefix, u64)>>,
    encoder_capacity: capacity::Count,
}

impl<'a, 'd, S, B> Pending<'a, 'd, S, B> {
    pub(in crate::conn) const fn new(
        fields: Section<'a, 'd>,
        queues: &'a mut write::Queues<'d, S, B>,
        qpack: Option<stream::Target>,
        outgoing: Option<Outgoing<'a, 'd>>,
        pending_encoder: &'a mut Option<ring::Fifo<(output::WritePrefix, u64)>>,
        encoder_capacity: capacity::Count,
    ) -> Self {
        Self {
            fields,
            queues,
            qpack,
            outgoing,
            pending_encoder,
            encoder_capacity,
        }
    }

    pub(in crate::conn) fn into_batch(self, trailing: Option<Entry<'d>>) -> Batch<'a, 'd, S, B> {
        Batch {
            pending: self,
            trailing,
        }
    }

    pub(in crate::conn) fn finish(mut self) -> Self {
        self.fields.entry.finish();
        self
    }

    pub(in crate::conn) fn discard(self) {
        self.fields.discard();
    }

    fn encode_qpack(&self) -> Result<Option<(stream::Target, output::WritePrefix)>, conn::Error> {
        let len = self.fields.qpack.wire_len();
        if len == 0 {
            return Ok(None);
        }
        let target = self.qpack.ok_or(conn::Error::EncoderMissing)?;
        let prefix = output::WritePrefix::try_write(len, |writer| {
            self.fields.qpack.encode_wire(writer);
        })?;
        Ok(Some((target, prefix)))
    }
}
