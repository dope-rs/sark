use core::marker;

use crate::{
    conn::{
        io::write::{Queues, budget},
        {self},
    },
    qpack::{
        self, decoder,
        decoder::state::{Candidate, State},
        stream,
    },
};

pub(in crate::conn) struct Feedback<'a, 'd, S, B> {
    wire: &'a State,
    session: &'a conn::SessionState,
    queues: &'a Queues<'d, S, B>,
}

impl<'a, 'd, S, B> Feedback<'a, 'd, S, B> {
    pub(in crate::conn) const fn new(
        wire: &'a State,
        session: &'a conn::SessionState,
        queues: &'a Queues<'d, S, B>,
    ) -> Self {
        Self {
            wire,
            session,
            queues,
        }
    }

    pub(in crate::conn) fn limit(&self) -> usize {
        let prefix = usize::from(self.session.qpack_decoder.is_none());
        let inline = qpack::FEEDBACK_CAPACITY - prefix;
        let used = self.wire.len().min(inline);
        let available = self.queues.write_budget.wire_left().saturating_sub(prefix);
        used + available.min(inline - used)
    }

    pub(in crate::conn) fn can_prepare(&self, inst: stream::Instruction) -> bool {
        self.wire.can_prepare(inst, self.limit())
    }
}

#[must_use = "prepared feedback must be committed after its protocol transition"]
pub(in crate::conn) struct Prepared<'a, O> {
    feedback: Candidate<'a>,
    budget: budget::Change<'a>,
    operation: marker::PhantomData<O>,
    invariant: marker::PhantomData<fn(&mut O)>,
}

impl<O> Prepared<'_, O> {
    pub(in crate::conn) fn commit(self) {
        self.feedback.commit();
        self.budget.commit();
    }
}

pub(in crate::conn) struct Section;

pub(in crate::conn) struct Cancel;

pub(in crate::conn) struct FeedbackMut<'a> {
    wire: &'a mut State,
    budget: &'a mut budget::WriteBudget,
}

impl<'a> FeedbackMut<'a> {
    pub(in crate::conn) const fn new(
        wire: &'a mut State,
        budget: &'a mut budget::WriteBudget,
    ) -> Self {
        Self { wire, budget }
    }

    pub(in crate::conn) fn try_section(
        self,
        _: qpack::Reference,
        id: u64,
        limit: usize,
    ) -> Result<Prepared<'a, Section>, conn::Error> {
        self.prepare(
            stream::Instruction::SectionAcknowledgment { stream_id: id },
            limit,
        )
    }

    pub(in crate::conn) fn try_cancel(
        self,
        id: u64,
        limit: usize,
    ) -> Result<Prepared<'a, Cancel>, conn::Error> {
        self.prepare(
            stream::Instruction::StreamCancellation { stream_id: id },
            limit,
        )
    }

    fn prepare<O>(
        self,
        inst: stream::Instruction,
        limit: usize,
    ) -> Result<Prepared<'a, O>, conn::Error> {
        let feedback = self
            .wire
            .try_prepare(inst, limit)
            .map_err(|error| match error {
                decoder::state::Error::Capacity(capacity)
                    if usize::from(capacity.additional) > self.budget.wire_limit() =>
                {
                    conn::Error::BudgetWire {
                        retained: self.budget.wire_len(),
                        additional: usize::from(capacity.additional),
                        limit: self.budget.wire_limit(),
                    }
                }
                error => error.into(),
            })?;
        let budget = self.budget.try_retain(feedback.growth())?;
        Ok(Prepared {
            feedback,
            budget,
            operation: marker::PhantomData,
            invariant: marker::PhantomData,
        })
    }

    pub(in crate::conn) fn try_ingest(
        self,
        decoder: &mut decoder::Decoder,
        input: &[u8],
        limit: usize,
    ) -> Result<qpack::Progress, conn::Error> {
        let limit = limit.min(qpack::FEEDBACK_CAPACITY);
        let permit = budget::Allowance::try_new(self.budget, self.wire.len(), limit)?;
        let (result, growth) = decoder.ingest_limit(self.wire, input, limit);
        permit.commit(growth);
        result.map_err(conn::Error::QpackEncoderStream)
    }

    pub(in crate::conn) fn try_prepend(self, prefix: u8) -> Result<(), conn::Error> {
        let used = self.wire.len().min(qpack::FEEDBACK_CAPACITY);
        let limit = used + self.budget.wire_left().min(qpack::FEEDBACK_CAPACITY - used);
        let feedback = self.wire.try_prefix(prefix, limit)?;
        let budget = self.budget.try_retain(feedback.growth())?;
        feedback.commit();
        budget.commit();
        Ok(())
    }
}

const _: () = {
    assert!(
        core::mem::size_of::<Feedback<'static, 'static, (), ()>>()
            == 3 * core::mem::size_of::<usize>()
    );
    assert!(
        core::mem::size_of::<Option<Feedback<'static, 'static, (), ()>>>()
            == core::mem::size_of::<Feedback<'static, 'static, (), ()>>()
    );
    assert!(
        core::mem::size_of::<Prepared<'static, Section>>()
            == core::mem::size_of::<Prepared<'static, Cancel>>()
    );
};
