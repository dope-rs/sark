use core::marker;

use dope_quic::wire::{
    stream::{Id, Kind},
    varint,
};
use sark_core::manifold::stream::Boundary;
use sark_http::field::metadata::section::{rules, state::Selected};

use crate::{
    conn::{
        self, event,
        io::{Resume, ingress::receive::Recv, write},
        output,
    },
    frame,
};

pub(super) struct Parts<'a, 'd, P: rules::Policy, A: rules::Push> {
    pub(super) settings: &'a mut conn::Settings,
    pub(super) qpack: &'a mut conn::QpackState,
    pub(super) session: &'a mut conn::SessionState,
    pub(super) queues: &'a mut write::Queues<'d, Selected<P>, P::Block>,
    pub(super) ingress: &'a mut conn::IngressState<'d>,
    pub(super) push: marker::PhantomData<fn() -> A>,
}

impl<'d, P: rules::Policy, A: rules::Push> Parts<'_, 'd, P, A> {
    pub(super) fn park(&mut self, id: Id, recv: &mut Recv<'_>) -> Result<(), conn::Error> {
        self.queues
            .waiters
            .push_ready(id, recv)
            .map_err(conn::Error::Wait)?;
        Ok(())
    }

    pub(super) fn defer(&mut self, id: Id, recv: &mut Recv<'_>) -> Result<(), conn::Error> {
        self.queues
            .waiters
            .defer(id, recv)
            .map_err(conn::Error::Wait)?;
        Ok(())
    }

    pub(super) fn wake(&mut self) -> Option<Id> {
        let count = self.qpack.qpack_decoder.insert_count();
        self.qpack.blocked.pop_ready(count)
    }

    pub(super) fn cancel_push(&mut self, push_id: varint::VarInt) -> Result<(), conn::Error> {
        use write::budget::WriteClass;

        let target = self.session.control.ok_or(conn::Error::ControlMissing)?;
        let prefix = output::WritePrefix::try_encode(&frame::Control::cancel_push(push_id))?;
        self.queues.enqueue(
            target,
            write::Entry::single(WriteClass::Critical, prefix, Boundary::More),
        )
    }

    pub(super) fn check_goaway(&self, id: u64) -> Result<(), conn::Error> {
        if self.settings.role == event::Role::Client
            && !Id::new(id).is_some_and(|id| id.kind() == Kind::ClientBidi)
        {
            return Err(conn::Error::GoAwayId(id));
        }
        if let Some(previous) = self.session.goaway_id
            && id > previous
        {
            return Err(conn::Error::GoAwayOrder {
                previous,
                actual: id,
            });
        }
        Ok(())
    }

    pub(super) fn release(
        &mut self,
        id: Id,
        bytes: usize,
        resume: Resume,
    ) -> Result<(), conn::Error> {
        if bytes == 0 && matches!(resume, Resume::Idle) {
            return Ok(());
        }
        self.queues
            .try_credit()?
            .push_back((write::Credit::new(id, resume), bytes));
        Ok(())
    }
}
