mod batch;
mod section;
mod writes;

pub(in crate::conn) use batch::Batch;
pub(in crate::conn) use section::Section;

use crate::{
    conn::io::{
        ingress::stream::table::Outgoing,
        write::{self, Entry},
    },
    stream,
};

pub(in crate::conn) struct Pending<'a, 'd, S, B> {
    fields: Section<'a, 'd>,
    queues: &'a mut write::Queues<'d, S, B>,
    qpack: Option<stream::Target>,
    outgoing: Option<Outgoing<'a, 'd>>,
}

impl<'a, 'd, S, B> Pending<'a, 'd, S, B> {
    pub(in crate::conn) const fn new(
        fields: Section<'a, 'd>,
        queues: &'a mut write::Queues<'d, S, B>,
        qpack: Option<stream::Target>,
        outgoing: Option<Outgoing<'a, 'd>>,
    ) -> Self {
        Self {
            fields,
            queues,
            qpack,
            outgoing,
        }
    }

    pub(in crate::conn) fn into_batch(self, trailing: Option<Entry<'d>>) -> Batch<'a, 'd, S, B> {
        Batch::new(
            self.fields,
            trailing,
            self.queues,
            self.qpack,
            self.outgoing,
        )
    }

    pub(in crate::conn) fn target(&self) -> stream::Target {
        self.fields.target()
    }

    pub(in crate::conn) fn finish(mut self) -> Self {
        self.fields = self.fields.finish();
        self
    }

    pub(in crate::conn) fn discard(self) {
        self.fields.discard();
    }
}
