mod batch;
mod section;

use o3::depot::{capacity, queue::ring};
use sark_http::field::metadata::section::{rules, state::Selected};

pub use batch::Batch;
pub(in crate::conn) use section::Section;

use crate::{
    conn,
    conn::io::{ingress::stream::table::Outgoing, write},
    conn::output,
    stream,
};

#[must_use = "a prepared response is invisible until committed"]
pub struct Prepared<'a, 'd, P: rules::Policy = rules::RetainRaw> {
    fields: Section<'a, 'd>,
    queues: &'a mut write::Queues<'d, Selected<P>, P::Block>,
    qpack: Option<stream::Target>,
    outgoing: Option<Outgoing<'a, 'd>>,
    pending_encoder: &'a mut Option<ring::Fifo<(output::WritePrefix, u64)>>,
    encoder_capacity: capacity::Count,
}

impl<'a, 'd, P: rules::Policy> Prepared<'a, 'd, P> {
    pub(in crate::conn) const fn new(
        fields: Section<'a, 'd>,
        queues: &'a mut write::Queues<'d, Selected<P>, P::Block>,
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

    pub fn open(self) -> Batch<'a, 'd, P> {
        Batch {
            pending: self,
            trailing: None,
        }
    }

    pub fn finish(mut self) -> Batch<'a, 'd, P> {
        self.fields.entry.finish();
        self.open()
    }

    pub(crate) fn discard(self) {
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
