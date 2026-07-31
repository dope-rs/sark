use std::{pin::Pin, task::Poll};

use dope::{
    DriverContext,
    manifold::listener::{
        egress::SlotEgress,
        state::{EgressCtx, State, WriteBuf},
    },
};
use dope_fiber::abi::Fiber;
use dope_net::{link::slot::Slot, wire::Wire};
use o3::buffer::Shared;
use sark_core::http::{CHUNK_TERMINATOR, OwnedShape};

use super::{
    conn_state::{ConnState, StreamPhase},
    egress::ResponseEgress,
    routes::TaskPoll,
};
use crate::{fiber::FixedSlab, service::RouteSpec};

pub struct TaskRunner<'a> {
    date: &'a [u8; 29],
}

impl<'a> TaskRunner<'a> {
    pub fn new(date: &'a [u8; 29]) -> Self {
        Self { date }
    }

    pub fn finish<'d, R: RouteSpec, W: Wire, C: Default + 'static>(
        &self,
        response: R::AsyncResponse,
        slot: &mut Slot<'d, W, State<C>>,
        egress: &mut EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'd>,
        close: bool,
    ) {
        if matches!(
            R::RESPONSE_BODY_KIND,
            sark_core::http::body_kind::ResponseKind::Stream
        ) {
            unreachable!("stream routes are completed by TaskRunner::poll");
        }
        let response = response.into_shape();
        let outcome = {
            let mut write = egress.write_buf_for(slot);
            ResponseEgress::new(&mut write, self.date).plain(response, close)
        };
        outcome.apply(slot, egress, driver);
    }

    pub fn poll<'d, T, Tag, W, C, PJ, Classify, const N: usize>(
        &self,
        mut tasks: Pin<&mut FixedSlab<'d, T, N, Tag>>,
        slot: &mut Slot<'d, W, State<C>>,
        egress: &mut EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
        mut classify: Classify,
    ) -> usize
    where
        T: Fiber<'d>,
        W: Wire,
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut ConnState,
        Classify: FnMut(
            T::Output,
            &mut Slot<'d, W, State<C>>,
            &mut EgressCtx<'_, '_>,
            &mut DriverContext<'_, 'd>,
            &[u8; 29],
            bool,
        ) -> TaskPoll,
    {
        use crate::fiber::TaskId;
        let Some(task) = project(&mut slot.state.conn).async_state.task.take() else {
            return 0;
        };
        let task = TaskId::<Tag>::from_erased(task);
        let mut cursor = 0;
        loop {
            let next = {
                let conn = project(&mut slot.state.conn);
                match conn.async_state.stream_pending.take() {
                    Some(stashed) => Some((
                        stashed,
                        conn.async_state.stream_phase == StreamPhase::Terminating,
                    )),
                    None => match conn.async_state.stream_phase {
                        StreamPhase::Terminating => {
                            Some((Shared::from_static(CHUNK_TERMINATOR), true))
                        }
                        StreamPhase::Streaming => None,
                    },
                }
            };
            let (framed, terminating) = match next {
                Some(next) => next,
                None => {
                    let poll = {
                        use std::pin::pin;
                        let mut context = pin!(dope_fiber::raw::task::Context::from_ready(
                            slot.driver(),
                            slot.ready_key(),
                            driver.reborrow(),
                        ));
                        tasks.as_mut().poll(&task, context.as_mut())
                    };
                    let Some(poll) = poll else {
                        debug_assert!(false, "live task must exist in fiber slab");
                        Self::release_connection(slot, &project);
                        return 0;
                    };
                    match poll {
                        Poll::Pending => {
                            project(&mut slot.state.conn).async_state.task = Some(task.erase());
                            return cursor;
                        }
                        Poll::Ready(output) => {
                            let close = project(&mut slot.state.conn).deferred_close;
                            match classify(output, slot, egress, driver, self.date, close) {
                                TaskPoll::Complete => {
                                    let removed = tasks.as_mut().remove(task);
                                    debug_assert!(removed, "live task must be removable");
                                    Self::release_connection(slot, &project);
                                    return 0;
                                }
                                TaskPoll::Stream(Some(raw)) => {
                                    if raw.is_empty() {
                                        continue;
                                    }
                                    (sark_core::http::codec::Wire::chunk_frame(raw), false)
                                }
                                TaskPoll::Stream(None) => {
                                    project(&mut slot.state.conn).async_state.stream_phase =
                                        StreamPhase::Terminating;
                                    continue;
                                }
                            }
                        }
                    }
                }
            };
            let capacity = egress.write_buf_for(slot).len();
            if capacity.saturating_sub(cursor) < framed.len() {
                if framed.len() > capacity {
                    let buffer = egress.write_buf_for(slot);
                    let token = slot.token();
                    slot.submit_split_shared(buffer, cursor, framed, token, driver);
                    if terminating {
                        let removed = tasks.as_mut().remove(task);
                        debug_assert!(removed, "live task must be removable");
                        Self::release_connection(slot, &project);
                    } else {
                        project(&mut slot.state.conn).async_state.task = Some(task.erase());
                    }
                    return 0;
                }
                let conn = project(&mut slot.state.conn);
                conn.async_state.task = Some(task.erase());
                conn.async_state.stream_pending = Some(framed);
                return cursor;
            }
            let end = cursor + framed.len();
            egress.write_buf_for(slot)[cursor..end].copy_from_slice(framed.as_ref());
            cursor = end;
            if terminating {
                let removed = tasks.as_mut().remove(task);
                debug_assert!(removed, "live task must be removable");
                Self::release_connection(slot, &project);
                return cursor;
            }
        }
    }

    pub fn write_buf<'d, 'slot, 'pool, W: Wire, C: Default + 'static>(
        &self,
        slot: &mut Slot<'d, W, State<C>>,
        egress: &'slot mut EgressCtx<'_, 'pool>,
    ) -> WriteBuf<'slot, 'pool> {
        egress.write_buf_for(slot)
    }

    fn release_connection<W, C, PJ>(slot: &mut Slot<'_, W, State<C>>, project: &PJ)
    where
        W: Wire,
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut ConnState,
    {
        let deferred_close = {
            let conn = project(&mut slot.state.conn);
            conn.async_state.task_stream = false;
            conn.async_state.stream_phase = StreamPhase::Streaming;
            conn.recv.unfreeze();
            conn.deferred_close
        };
        if deferred_close {
            slot.set_close_after();
        }
    }
}
