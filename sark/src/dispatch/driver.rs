use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::listener::recv::ExtendOutcome;
use dope::manifold::listener::{self, SlotEgress};
use dope_net::link;
use dope_net::wire::Wire;

use super::conn_state::{ConnState, DeferredAction};
use super::pipeline::Pipeline;
use super::routing::Routing;

pub struct H1Driver<'a, H> {
    app: Pin<&'a mut H>,
}

impl<'a, H> H1Driver<'a, H> {
    pub fn new(app: Pin<&'a mut H>) -> Self {
        Self { app }
    }

    pub fn run_proj<'d, W, C, P>(
        &mut self,
        bytes: &[u8],
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: P,
    ) -> bool
    where
        H: Routing<'d> + crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        let (fast, close) = Pipeline::fast_path(project(&mut slot.state.conn), bytes);
        if close {
            slot.set_close_after();
        }
        if let Some(result) = fast {
            return result;
        }

        let use_accumulator = project(&mut slot.state.conn).recv.is_accumulating();
        if use_accumulator
            && !bytes.is_empty()
            && matches!(
                project(&mut slot.state.conn).recv.extend_existing(bytes),
                ExtendOutcome::Overrun
            )
        {
            slot.set_close_after();
            return true;
        }

        let peeked = if use_accumulator {
            project(&mut slot.state.conn).recv.snapshot()
        } else {
            None
        };
        let work = match &peeked {
            Some(buffer) => buffer.as_slice(),
            None => bytes,
        };

        project(&mut slot.state.conn).recv_view = peeked.clone();
        let close_after = slot.close_after();
        let mut write = aux.write_buf_for(slot);
        let out = Pipeline::batch(
            project(&mut slot.state.conn),
            self.app.as_mut(),
            work,
            &mut write,
            close_after,
        );
        drop(peeked);
        project(&mut slot.state.conn).recv_view = None;

        let head_pending = out.head_pending;
        let overrun = Pipeline::emit(slot, aux, driver, out, use_accumulator, bytes, &project);
        if !overrun {
            Self::begin_body_discard(slot, driver, &project);
        }
        let deadline = HeadDeadline::new(self.app.as_ref().get_ref());
        let deadline_overrun = deadline.manage(slot, head_pending, driver.turn_now(), &project);
        overrun || deadline_overrun
    }

    pub fn send_complete_proj<'d, W, C, P>(
        &mut self,
        _sent: usize,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: P,
    ) where
        H: Routing<'d> + crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if project(&mut slot.state.conn).async_state.task_stream
            && project(&mut slot.state.conn).async_state.has_task()
        {
            return;
        }
        if let Some(DeferredAction::Close(reason)) =
            project(&mut slot.state.conn).deferred_action.take()
        {
            slot.set_close_after();
            if !reason.is_empty() {
                let buf = aux.write_buf_for(slot);
                let user_data = slot.token();
                slot.submit_split_static(buf, 0, reason, user_data, driver);
            }
            return;
        }
        if project(&mut slot.state.conn).recv.is_accumulating()
            && !project(&mut slot.state.conn).recv.is_frozen()
        {
            let _ = self.run_proj(&[], slot, aux, driver, &project);
        }
    }

    fn begin_body_discard<'d, W, C, P>(
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        driver: &mut DriverContext<'_, 'd>,
        project: &P,
    ) where
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        let remaining = project(&mut slot.state.conn)
            .pipeline
            .discard_body_remaining;
        if remaining > 0 && slot.begin_discard(driver, remaining as u64) {
            project(&mut slot.state.conn)
                .pipeline
                .discard_body_remaining = 0;
        }
    }
}

pub struct HeadDeadline<'a, H> {
    app: &'a H,
}

impl<'a, H> HeadDeadline<'a, H> {
    pub fn new(app: &'a H) -> Self {
        Self { app }
    }

    pub fn poll_proj<'d, W, C, P>(
        &self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: P,
    ) -> bool
    where
        H: crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        let Some(ticket) = project(&mut slot.state.conn).head_deadline else {
            return false;
        };
        let timer = crate::timer::TimerHost::timer(self.app);
        if !timer.is_fired(ticket) {
            return false;
        }
        project(&mut slot.state.conn).head_deadline = None;
        timer.cancel(ticket);
        slot.set_close_after();
        let buf = aux.write_buf_for(slot);
        let user_data = slot.token();
        slot.submit_split_static(buf, 0, crate::CANNED_408, user_data, driver);
        true
    }

    pub fn cancel_proj<'d, W, C, P>(
        &self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: P,
    ) where
        H: crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(self.app).cancel(ticket);
        }
    }

    fn manage<'d, W, C, P>(
        &self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        head_pending: bool,
        now: std::time::Instant,
        project: &P,
    ) -> bool
    where
        H: crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if head_pending {
            if project(&mut slot.state.conn).head_deadline.is_none() {
                let timer = crate::timer::TimerHost::timer(self.app);
                let deadline = now + timer.head_timeout();
                let wake = dope_fiber::Waker::from_ready(slot.driver(), slot.ready_key());
                if let Some(ticket) = timer.arm(deadline, wake) {
                    project(&mut slot.state.conn).head_deadline = Some(ticket);
                } else {
                    return true;
                }
            }
        } else if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(self.app).cancel(ticket);
        }
        false
    }
}
