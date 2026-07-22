use dope::DriverContext;
use dope::manifold::listener;
use dope::manifold::listener::SlotEgress;
use dope::manifold::listener::recv::ExtendOutcome;
use dope::manifold::listener::send::WRITE_BUF_CAP;
use dope_net::link;
use dope_net::wire::Wire;
use std::pin::Pin;

use super::conn_state::{
    ConnState, ConsumeOutcome, Consumption, DeferredAction, DispatchPermit, NeedMore, Outcome,
    PendingFrame,
};
use super::routing::Routing;

const MAX_PIPELINE_BATCH: u32 = 32;
const WRITE_HEADROOM: usize = WRITE_BUF_CAP - 1024;
const RESERVE_CAP: usize = 64 * 1024;

enum SplitBody {
    Static(&'static [u8]),
    Shared(o3::buffer::Shared),
    Pooled(o3::buffer::Pooled),
}

struct LoopOutcome {
    off: usize,
    cursor: usize,
    batched: u32,
    final_action: Option<Outcome>,
    split: Option<(usize, SplitBody)>,
    close_after: bool,
    head_pending: bool,
}

#[derive(Default)]
pub struct Pipeline {
    pub(super) pending_frame: PendingFrame,
    pub(super) discard_body_remaining: usize,
}

impl Pipeline {
    fn absorb(state: &mut ConnState, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        if matches!(state.recv.extend(bytes), ExtendOutcome::Overrun) {
            return true;
        }
        false
    }

    fn drain_consumed(state: &mut ConnState, off: usize) {
        state.recv.advance(off);
    }

    fn finish_frame(&mut self, recv: &mut super::conn_state::Recv) {
        self.pending_frame = PendingFrame::Head;
        recv.restrict_to_head();
    }

    fn await_frame(
        &mut self,
        recv: &mut super::conn_state::Recv,
        need: NeedMore,
        available: usize,
    ) -> bool {
        match need {
            NeedMore::Head => {
                self.pending_frame = PendingFrame::Head;
                recv.restrict_to_head();
                true
            }
            NeedMore::FixedBody(total) => {
                let frame = PendingFrame::FixedBody(total);
                let changed = self.pending_frame != frame;
                self.pending_frame = frame;
                recv.permit_body();
                if changed && total > available {
                    let _ = recv.try_reserve_to(total.min(RESERVE_CAP));
                }
                false
            }
            NeedMore::ChunkedBody => {
                self.pending_frame = PendingFrame::ChunkedBody;
                recv.permit_body();
                false
            }
        }
    }

    fn fast_path(state: &mut ConnState, bytes: &[u8]) -> (Option<bool>, bool) {
        if state.recv.is_frozen() {
            let close = matches!(state.recv.extend_backlog(bytes), ExtendOutcome::Overrun);
            return (Some(false), close);
        }
        if state.pipeline.discard_body_remaining > 0 {
            let take = bytes.len().min(state.pipeline.discard_body_remaining);
            state.pipeline.discard_body_remaining -= take;
            if take < bytes.len() && Self::absorb(state, &bytes[take..]) {
                return (Some(true), true);
            }
            return (Some(false), false);
        }
        if state.deferred_action.is_some() {
            let overrun = !bytes.is_empty()
                && matches!(state.recv.extend_backlog(bytes), ExtendOutcome::Overrun);
            return (Some(overrun), false);
        }
        (None, false)
    }

    fn consume_frame(
        state: &mut ConnState,
        consumption: Consumption,
        available: usize,
        off: &mut usize,
    ) -> bool {
        let discarding = match consumption {
            Consumption::Buffered(consumed) => {
                *off += consumed;
                false
            }
            Consumption::Discard { head, body } => {
                *off += head;
                let buffered_body = (available - *off).min(body);
                *off += buffered_body;
                state.pipeline.discard_body_remaining = body - buffered_body;
                state.pipeline.discard_body_remaining > 0
            }
        };
        let ConnState { pipeline, recv, .. } = state;
        pipeline.finish_frame(recv);
        discarding
    }

    fn batch<H: Routing>(
        state: &mut ConnState,
        mut app: Pin<&mut H>,
        work_buf: &[u8],
        write_buf: &mut [u8],
        close_after: bool,
    ) -> LoopOutcome {
        let mut out = LoopOutcome {
            off: 0,
            cursor: 0,
            batched: 0,
            final_action: None,
            split: None,
            close_after,
            head_pending: false,
        };
        let mut permit = DispatchPermit::new();
        loop {
            if out.batched >= MAX_PIPELINE_BATCH || out.cursor > WRITE_HEADROOM || out.close_after {
                break;
            }
            let rest = &work_buf[out.off..];
            if rest.is_empty() {
                break;
            }
            let outcome =
                app.as_mut()
                    .try_consume(permit, rest, &mut write_buf[out.cursor..], state);
            permit = match outcome {
                ConsumeOutcome::NeedMore { state: need, .. } => {
                    let ConnState { pipeline, recv, .. } = state;
                    out.head_pending = pipeline.await_frame(recv, need, rest.len());
                    break;
                }
                ConsumeOutcome::Complete {
                    permit: p,
                    consumption,
                    response,
                    conn_close,
                } => {
                    let discarding =
                        Self::consume_frame(state, consumption, work_buf.len(), &mut out.off);
                    out.close_after |= conn_close;
                    out.batched += 1;
                    match response {
                        Outcome::Send {
                            written,
                            close_after,
                        } => {
                            out.cursor += written;
                            out.close_after |= close_after;
                            if discarding {
                                break;
                            }
                            p
                        }
                        Outcome::SendStatic {
                            hdr_written,
                            body,
                            close_after,
                        } => {
                            out.close_after |= close_after;
                            let body_start = out.cursor + hdr_written;
                            let body_end = body_start + body.len();
                            if body_end <= write_buf.len() {
                                write_buf[body_start..body_end].copy_from_slice(body);
                                out.cursor = body_end;
                                if discarding {
                                    break;
                                }
                                p
                            } else {
                                out.split = Some((hdr_written, SplitBody::Static(body)));
                                break;
                            }
                        }
                        Outcome::SendSplit {
                            hdr_written,
                            body,
                            close_after,
                        } => {
                            out.close_after |= close_after;
                            out.split = Some((hdr_written, SplitBody::Shared(body)));
                            break;
                        }
                        Outcome::SendPooled {
                            hdr_written,
                            body,
                            close_after,
                        } => {
                            out.close_after |= close_after;
                            out.split = Some((hdr_written, SplitBody::Pooled(body)));
                            break;
                        }
                        Outcome::Park | Outcome::Close(_) => {
                            unreachable!("Complete only carries encoded responses")
                        }
                    }
                }
                ConsumeOutcome::Streamed {
                    consumed,
                    written,
                    close,
                } => {
                    out.close_after |= close;
                    out.cursor += written;
                    out.off += consumed;
                    let ConnState { pipeline, recv, .. } = state;
                    pipeline.finish_frame(recv);
                    out.final_action = Some(Outcome::Send {
                        written,
                        close_after: close,
                    });
                    break;
                }
                ConsumeOutcome::Park { consumed, close } => {
                    out.close_after |= close;
                    out.off += consumed;
                    let ConnState { pipeline, recv, .. } = state;
                    pipeline.finish_frame(recv);
                    out.final_action = Some(Outcome::Park);
                    break;
                }
                ConsumeOutcome::Close(reason) => {
                    out.final_action = Some(Outcome::Close(reason));
                    break;
                }
            };
        }
        out
    }

    fn emit<'d, W: Wire, C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        out: LoopOutcome,
        use_accumulator: bool,
        plaintext: &[u8],
        project: &P,
    ) -> bool {
        let pending = project(&mut slot.state.conn).async_state.has_task();
        let will_freeze =
            pending && matches!(out.final_action, Some(Outcome::Park | Outcome::Send { .. }));

        if will_freeze {
            if use_accumulator {
                Self::drain_consumed(project(&mut slot.state.conn), out.off);
            } else if Self::absorb(project(&mut slot.state.conn), &plaintext[out.off..]) {
                slot.set_close_after();
                return true;
            }
        } else if use_accumulator {
            Self::drain_consumed(project(&mut slot.state.conn), out.off);
        } else if Self::absorb(project(&mut slot.state.conn), &plaintext[out.off..]) {
            slot.set_close_after();
            return true;
        }

        if will_freeze {
            project(&mut slot.state.conn).deferred_close = out.close_after;
            project(&mut slot.state.conn).recv.freeze();
            let accepted = match out.final_action {
                Some(Outcome::Send { written, .. }) => {
                    let buf = aux.write_buf_for(slot);
                    let ud = slot.token();
                    slot.submit_buffered(buf, written, ud, driver)
                }
                Some(Outcome::Park) => {
                    slot.mark_ready();
                    true
                }
                _ => true,
            };
            return !accepted;
        }

        if let Some((split_hdr, body)) = out.split {
            if out.close_after {
                slot.set_close_after();
            }
            let buf = aux.write_buf_for(slot);
            let ud = slot.token();
            let accepted = match body {
                SplitBody::Static(body) => {
                    slot.submit_split_static(buf, out.cursor + split_hdr, body, ud, driver)
                }
                SplitBody::Shared(body) => {
                    slot.submit_split_shared(buf, out.cursor + split_hdr, body, ud, driver)
                }
                SplitBody::Pooled(body) => {
                    slot.submit_split_pooled(buf, out.cursor + split_hdr, body, ud, driver)
                }
            };
            !accepted
        } else if out.cursor > 0 {
            if let Some(Outcome::Close(reason)) = out.final_action {
                project(&mut slot.state.conn).deferred_action = Some(DeferredAction::Close(reason));
            }
            if out.close_after {
                slot.set_close_after();
            }
            let buf = aux.write_buf_for(slot);
            let ud = slot.token();
            !slot.submit_buffered(buf, out.cursor, ud, driver)
        } else if let Some(act) = out.final_action {
            let act = match act {
                Outcome::Send {
                    written,
                    close_after,
                } => Outcome::Send {
                    written,
                    close_after: out.close_after | close_after,
                },
                Outcome::SendStatic {
                    hdr_written,
                    body,
                    close_after,
                } => Outcome::SendStatic {
                    hdr_written,
                    body,
                    close_after: out.close_after | close_after,
                },
                other => other,
            };
            let close = matches!(act, Outcome::Close(r) if r.is_empty());
            close || !act.apply(slot, aux, driver)
        } else {
            false
        }
    }

    pub fn run<'d, H, W>(
        app: Pin<&mut H>,
        bytes: &[u8],
        slot: &mut link::slot::Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool
    where
        H: Routing + crate::timer::TimerHost<'d>,
        W: Wire,
    {
        Self::run_proj(app, bytes, slot, aux, driver, identity_mut)
    }

    pub fn run_proj<'d, H, W, C, P>(
        mut app: Pin<&mut H>,
        bytes: &[u8],
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: P,
    ) -> bool
    where
        H: Routing + crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        let (fast, close) = Self::fast_path(project(&mut slot.state.conn), bytes);
        if close {
            slot.set_close_after();
        }
        if let Some(ret) = fast {
            return ret;
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

        let peeked: Option<o3::buffer::Shared> = if use_accumulator {
            project(&mut slot.state.conn).recv.snapshot()
        } else {
            None
        };
        let work_buf: &[u8] = match &peeked {
            Some(s) => s.as_slice(),
            None => bytes,
        };

        project(&mut slot.state.conn).recv_view = peeked.clone();
        let close_after = slot.close_after();
        let mut write_buf = aux.write_buf_for(slot);
        let out = Self::batch(
            project(&mut slot.state.conn),
            app.as_mut(),
            work_buf,
            &mut write_buf,
            close_after,
        );
        drop(peeked);
        project(&mut slot.state.conn).recv_view = None;

        let head_pending = out.head_pending;
        let overrun = Self::emit(slot, aux, driver, out, use_accumulator, bytes, &project);
        if !overrun {
            Self::begin_body_discard(slot, driver, &project);
        }
        let deadline_overrun = Self::manage_head_deadline(
            app.as_ref().get_ref(),
            slot,
            head_pending,
            driver.turn_now(),
            &project,
        );
        overrun || deadline_overrun
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

    fn manage_head_deadline<'d, H, W, C, P>(
        app: &H,
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
                let timer = crate::timer::TimerHost::timer(app);
                let deadline = now + timer.head_timeout();
                let wake = dope_fiber::Waker::from_ready(slot.driver(), slot.ready_key());
                if let Some(ticket) = timer.arm(deadline, wake) {
                    project(&mut slot.state.conn).head_deadline = Some(ticket);
                } else {
                    return true;
                }
            }
        } else if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(app).cancel(ticket);
        }
        false
    }

    pub fn poll_head_deadline<'d, H, W>(
        app: &H,
        slot: &mut link::slot::Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool
    where
        H: crate::timer::TimerHost<'d>,
        W: Wire,
    {
        Self::poll_head_deadline_proj(app, slot, aux, driver, identity_mut)
    }

    pub fn poll_head_deadline_proj<'d, H, W, C, P>(
        app: &H,
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
        let timer = crate::timer::TimerHost::timer(app);
        if !timer.is_fired(ticket) {
            return false;
        }
        project(&mut slot.state.conn).head_deadline = None;
        timer.cancel(ticket);
        slot.set_close_after();
        let buf = aux.write_buf_for(slot);
        let ud = slot.token();
        slot.submit_split_static(buf, 0, crate::CANNED_408, ud, driver);
        true
    }

    pub fn cancel_head_deadline_proj<'d, H, W, C, P>(
        app: &H,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: P,
    ) where
        H: crate::timer::TimerHost<'d>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(app).cancel(ticket);
        }
    }

    pub fn send_complete<'d, H, W>(
        app: Pin<&mut H>,
        sent: usize,
        slot: &mut link::slot::Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        H: Routing + crate::timer::TimerHost<'d>,
        W: Wire,
    {
        Self::send_complete_proj(app, sent, slot, aux, driver, identity_mut)
    }

    pub fn send_complete_proj<'d, H, W, C, P>(
        mut app: Pin<&mut H>,
        _sent: usize,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: P,
    ) where
        H: Routing + crate::timer::TimerHost<'d>,
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
                let ud = slot.token();
                slot.submit_split_static(buf, 0, reason, ud, driver);
            }
            return;
        }
        if project(&mut slot.state.conn).recv.is_accumulating()
            && !project(&mut slot.state.conn).recv.is_frozen()
        {
            let _ = Self::run_proj(app.as_mut(), &[], slot, aux, driver, &project);
        }
    }
}

pub use sark_core::identity_mut;
