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

pub(super) struct LoopOutcome {
    off: usize,
    cursor: usize,
    batched: u32,
    final_action: Option<Outcome>,
    split: Option<(usize, SplitBody)>,
    close_after: bool,
    pub(super) head_pending: bool,
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

    pub(super) fn fast_path(state: &mut ConnState, bytes: &[u8]) -> (Option<bool>, bool) {
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

    pub(super) fn batch<'d, H: Routing<'d>>(
        state: &mut ConnState,
        mut app: Pin<&mut H>,
        scope: dope_fiber::FiberScope<'d>,
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
                    .try_consume(scope, permit, rest, &mut write_buf[out.cursor..], state);
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

    pub(super) fn emit<'d, W: Wire, C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
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
}

pub use sark_core::identity_mut;
