use std::pin::Pin;
use std::task::Poll;

use dope::DriverContext;
use dope::manifold::listener::{self, SlotEgress};
use dope_net::link;
use dope_net::wire::Wire;
use o3::buffer::Shared;
use sark_core::http::{CHUNK_TERMINATOR, OwnedShape};

use super::conn_state::{ConnState, StreamPhase};
use super::egress::ResponseEgress;
use super::routes::TaskPoll;
use crate::service::RouteSpec;

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
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
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
            let mut write = aux.write_buf_for(slot);
            ResponseEgress::new(&mut write, self.date).plain(response, close)
        };
        outcome.apply(slot, aux, driver);
    }

    pub fn poll<'d, T, Tag, W, C, PJ, Classify, const N: usize>(
        &self,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
        mut classify: Classify,
    ) -> usize
    where
        T: dope_fiber::Fiber<'d> + 'd,
        W: Wire,
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut ConnState,
        Classify: FnMut(
            T::Output,
            &mut link::slot::Slot<'d, W, listener::State<C>>,
            &mut listener::Aux,
            &mut DriverContext<'_, 'd>,
            &[u8; 29],
            bool,
        ) -> TaskPoll,
    {
        let conn_ptr: *mut ConnState = project(&mut slot.state.conn);
        let conn = unsafe { &mut *conn_ptr };
        let Some(task) = conn.async_state.task.take() else {
            return 0;
        };
        let task = crate::fiber::TaskId::<Tag>::from_erased(task);
        let mut cursor = 0;
        loop {
            let (framed, terminating) = match conn.async_state.stream_pending.take() {
                Some(stashed) => (
                    stashed,
                    conn.async_state.stream_phase == StreamPhase::Terminating,
                ),
                None => match conn.async_state.stream_phase {
                    StreamPhase::Terminating => (Shared::from_static(CHUNK_TERMINATOR), true),
                    StreamPhase::Streaming => {
                        let poll = {
                            let mut context = std::pin::pin!(dope_fiber::Context::from_ready(
                                slot.driver(),
                                slot.ready_key(),
                                driver.reborrow(),
                            ));
                            tasks.as_mut().poll(&task, context.as_mut())
                        };
                        let Some(poll) = poll else {
                            debug_assert!(false, "live task must exist in fiber slab");
                            Self::release_connection(conn, slot);
                            return 0;
                        };
                        match poll {
                            Poll::Pending => {
                                conn.async_state.task = Some(task.erase());
                                return cursor;
                            }
                            Poll::Ready(output) => match classify(
                                output,
                                slot,
                                aux,
                                driver,
                                self.date,
                                conn.deferred_close,
                            ) {
                                TaskPoll::Complete => {
                                    let removed = tasks.as_mut().remove(task);
                                    debug_assert!(removed, "live task must be removable");
                                    Self::release_connection(conn, slot);
                                    return 0;
                                }
                                TaskPoll::Stream(Some(raw)) => {
                                    if raw.is_empty() {
                                        continue;
                                    }
                                    (sark_core::http::codec::Wire::chunk_frame(raw), false)
                                }
                                TaskPoll::Stream(None) => {
                                    conn.async_state.stream_phase = StreamPhase::Terminating;
                                    continue;
                                }
                            },
                        }
                    }
                },
            };
            let capacity = aux.write_buf_for(slot).len();
            if capacity.saturating_sub(cursor) < framed.len() {
                if framed.len() > capacity {
                    let buffer = aux.write_buf_for(slot);
                    let token = slot.token();
                    slot.submit_split_shared(buffer, cursor, framed, token, driver);
                    if terminating {
                        let removed = tasks.as_mut().remove(task);
                        debug_assert!(removed, "live task must be removable");
                        Self::release_connection(conn, slot);
                    } else {
                        conn.async_state.task = Some(task.erase());
                    }
                    return 0;
                }
                conn.async_state.task = Some(task.erase());
                conn.async_state.stream_pending = Some(framed);
                return cursor;
            }
            let end = cursor + framed.len();
            aux.write_buf_for(slot)[cursor..end].copy_from_slice(framed.as_ref());
            cursor = end;
            if terminating {
                let removed = tasks.as_mut().remove(task);
                debug_assert!(removed, "live task must be removable");
                Self::release_connection(conn, slot);
                return cursor;
            }
        }
    }

    pub fn write_buf<'d, 'slot, W: Wire, C: Default + 'static>(
        &self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &'slot mut listener::Aux,
    ) -> listener::WriteBuf<'slot> {
        aux.write_buf_for(slot)
    }

    fn release_connection<W: Wire, C: Default + 'static>(
        conn: &mut ConnState,
        slot: &mut link::slot::Slot<'_, W, listener::State<C>>,
    ) {
        conn.async_state.task_stream = false;
        conn.async_state.stream_phase = StreamPhase::Streaming;
        conn.recv.unfreeze();
        if conn.deferred_close {
            slot.set_close_after();
        }
    }
}
