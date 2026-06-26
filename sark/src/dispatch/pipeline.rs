use dope::Driver;
use dope::manifold::listener;
use dope::manifold::listener::recv::ExtendOutcome;
use dope::manifold::listener::send::WRITE_BUF_CAP;
use dope::transport::link;
use dope::transport::wire::Wire;

use super::conn_state::{ConnState, ConsumeOutcome, DeferredAction, DispatchPermit, Outcome};
use super::routing::Routing;

const MAX_PIPELINE_BATCH: u32 = 32;
const WRITE_HEADROOM: usize = WRITE_BUF_CAP - 1024;
const RESERVE_CAP: usize = 64 * 1024;

struct LoopOutcome {
    off: usize,
    cursor: usize,
    batched: u32,
    final_action: Option<Outcome>,
    split: Option<(usize, o3::buffer::Shared)>,
    close_after: bool,
    head_pending: bool,
}

pub struct Pipeline;

impl Pipeline {
    fn absorb(state: &mut ConnState, core: &mut link::Core, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        if matches!(state.recv.extend_accum(bytes), ExtendOutcome::Overrun) {
            core.set_close_after();
            return true;
        }
        false
    }

    fn drain_consumed(state: &mut ConnState, off: usize) {
        if let Some(accum) = state.recv.accum.as_mut() {
            accum.advance(off);
            if accum.is_empty() {
                state.recv.accum = None;
            } else {
                accum.compact();
            }
        }
    }

    fn fast_path(state: &mut ConnState, core: &mut link::Core, bytes: &[u8]) -> Option<bool> {
        if state.recv.is_frozen() {
            Self::absorb(state, core, bytes);
            return Some(false);
        }
        if state.pipeline.stream_body_remaining > 0 {
            let take =
                (bytes.len() as u64).min(state.pipeline.stream_body_remaining as u64) as usize;
            state.pipeline.stream_body_remaining -= take as u32;
            if take < bytes.len() && Self::absorb(state, core, &bytes[take..]) {
                return Some(true);
            }
            return Some(false);
        }
        if core.is_send_inflight() || state.deferred_action.is_some() {
            let overrun = !bytes.is_empty()
                && matches!(state.recv.extend_accum(bytes), ExtendOutcome::Overrun);
            return Some(overrun);
        }
        None
    }

    fn batch<H: Routing>(
        state: &mut ConnState,
        app: &mut H,
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
            let outcome = app.try_consume(permit, rest, &mut write_buf[out.cursor..], state);
            permit = match outcome {
                ConsumeOutcome::NeedMore { content_length, .. } => {
                    out.head_pending = content_length.is_none();
                    if let Some(total) = content_length
                        && state.pipeline.expected_total.is_none()
                        && total > work_buf.len()
                    {
                        state.pipeline.expected_total = Some(total.min(u32::MAX as usize) as u32);
                        state.recv.set_body_limit(total);
                        let _ = state.recv.reserve_accum(total.min(RESERVE_CAP));
                    }
                    break;
                }
                ConsumeOutcome::Done {
                    permit: p,
                    consumed,
                    written,
                    close,
                } => {
                    out.cursor += written;
                    out.close_after |= close;
                    out.off += consumed;
                    out.batched += 1;
                    p
                }
                ConsumeOutcome::DoneStatic {
                    permit: p,
                    consumed,
                    hdr_written,
                    body,
                    close,
                } => {
                    let body_start = out.cursor + hdr_written;
                    let body_end = body_start + body.len();
                    if body_end <= WRITE_BUF_CAP {
                        write_buf[body_start..body_end].copy_from_slice(body);
                        out.cursor = body_end;
                        out.close_after |= close;
                        out.off += consumed;
                        out.batched += 1;
                        p
                    } else if out.cursor == 0 {
                        out.close_after |= close;
                        out.off += consumed;
                        out.final_action = Some(Outcome::SendStatic {
                            hdr_written,
                            body,
                            close_after: close,
                        });
                        break;
                    } else {
                        break;
                    }
                }
                ConsumeOutcome::DoneSplit {
                    permit: _,
                    consumed,
                    hdr_written,
                    body,
                    close,
                } => {
                    out.close_after |= close;
                    out.off += consumed;
                    out.split = Some((hdr_written, body));
                    break;
                }
                ConsumeOutcome::Streamed {
                    consumed,
                    written,
                    close,
                } => {
                    out.close_after |= close;
                    out.cursor += written;
                    out.off += consumed;
                    out.final_action = Some(Outcome::Send {
                        written,
                        close_after: close,
                    });
                    break;
                }
                ConsumeOutcome::StreamArmed {
                    permit: _,
                    head_consumed,
                    body_total,
                    written,
                    close,
                } => {
                    out.cursor += written;
                    out.close_after |= close;
                    out.off += head_consumed;
                    let already_in_buf = work_buf.len().saturating_sub(out.off);
                    let body_pre = already_in_buf.min(body_total);
                    out.off += body_pre;
                    state.pipeline.stream_body_remaining =
                        (body_total - body_pre).min(u32::MAX as usize) as u32;
                    state.pipeline.expected_total = None;
                    state.recv.reset_limit();
                    state.recv.accum = None;
                    out.batched += 1;
                    out.final_action = Some(Outcome::Send {
                        written: out.cursor,
                        close_after: close,
                    });
                    break;
                }
                ConsumeOutcome::Park { consumed, close } => {
                    out.close_after |= close;
                    out.off += consumed;
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

    fn emit<W: Wire, C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        out: LoopOutcome,
        use_accum: bool,
        plaintext: &[u8],
        project: &P,
    ) -> bool {
        let pending = project(&mut slot.state.conn)
            .async_state
            .pending_wake
            .is_some()
            || project(&mut slot.state.conn)
                .async_state
                .stream_slot
                .is_some();
        let will_freeze =
            pending && matches!(out.final_action, Some(Outcome::Park | Outcome::Send { .. }));

        if will_freeze {
            // The in-flight request now borrows conn.retained_req, not the recv buffer,
            // so drop its consumed bytes here instead of replaying them after the await.
            if use_accum {
                Self::drain_consumed(project(&mut slot.state.conn), out.off);
                project(&mut slot.state.conn).pipeline.expected_total = None;
                project(&mut slot.state.conn).recv.reset_limit();
            } else if Self::absorb(
                project(&mut slot.state.conn),
                &mut slot.core,
                &plaintext[out.off..],
            ) {
                return true;
            }
        } else if use_accum {
            Self::drain_consumed(project(&mut slot.state.conn), out.off);
            if out.batched > 0 {
                project(&mut slot.state.conn).pipeline.expected_total = None;
                project(&mut slot.state.conn).recv.reset_limit();
            }
        } else if Self::absorb(
            project(&mut slot.state.conn),
            &mut slot.core,
            &plaintext[out.off..],
        ) {
            return true;
        }

        if will_freeze {
            project(&mut slot.state.conn).deferred_close = out.close_after;
            project(&mut slot.state.conn).recv.freeze();
            match out.final_action {
                Some(Outcome::Send { written, .. }) => {
                    let buf = aux.write_buf_for(slot);
                    let ud = slot.token();
                    slot.submit_buffered(buf, written, ud, driver);
                }
                Some(Outcome::Park) => {
                    slot.park(driver);
                }
                _ => {}
            }
            return false;
        }

        if let Some((split_hdr, body)) = out.split {
            if out.close_after {
                slot.core.set_close_after();
            }
            let buf = aux.write_buf_for(slot);
            let ud = slot.token();
            slot.submit_split_shared(buf, out.cursor + split_hdr, body, ud, driver);
            false
        } else if out.cursor > 0 {
            if let Some(Outcome::Close(reason)) = out.final_action {
                project(&mut slot.state.conn).deferred_action = Some(DeferredAction::Close(reason));
            }
            if out.close_after {
                slot.core.set_close_after();
            }
            let buf = aux.write_buf_for(slot);
            let ud = slot.token();
            slot.submit_buffered(buf, out.cursor, ud, driver);
            false
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
            act.apply(slot, aux, driver);
            close
        } else {
            false
        }
    }

    pub fn run<'t, H, W>(
        app: &mut H,
        bytes: &[u8],
        slot: &mut link::Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> bool
    where
        H: Routing + crate::timer::TimerHost<'t>,
        W: Wire,
    {
        Self::run_proj(app, bytes, slot, aux, driver, identity_mut)
    }

    pub fn run_proj<'t, H, W, C, P>(
        app: &mut H,
        bytes: &[u8],
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        project: P,
    ) -> bool
    where
        H: Routing + crate::timer::TimerHost<'t>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if let Some(ret) = Self::fast_path(project(&mut slot.state.conn), &mut slot.core, bytes) {
            return ret;
        }

        let use_accum = project(&mut slot.state.conn).recv.accum.is_some();
        if use_accum
            && !bytes.is_empty()
            && matches!(
                project(&mut slot.state.conn).recv.extend_existing(bytes),
                ExtendOutcome::Overrun
            )
        {
            slot.core.set_close_after();
            return true;
        }

        let peeked: Option<o3::buffer::Shared> = if use_accum {
            project(&mut slot.state.conn)
                .recv
                .accum
                .as_ref()
                .and_then(|a| a.peek())
        } else {
            None
        };
        let work_buf: &[u8] = match &peeked {
            Some(s) => s.as_slice(),
            None => bytes,
        };

        project(&mut slot.state.conn).recv_view = peeked.clone();
        let close_after = slot.core.close_after();
        let write_buf = aux.write_buf_for(slot);
        let out = Self::batch(
            project(&mut slot.state.conn),
            app,
            work_buf,
            write_buf,
            close_after,
        );
        drop(peeked);
        project(&mut slot.state.conn).recv_view = None;

        let head_pending = out.head_pending;
        let overrun = Self::emit(slot, aux, driver, out, use_accum, bytes, &project);
        Self::manage_head_deadline(app, slot, driver, head_pending, &project);
        overrun
    }

    fn head_still_pending(state: &ConnState) -> bool {
        match state.recv.accum.as_ref().and_then(|a| a.peek()) {
            Some(s) => memchr::memmem::find(s.as_slice(), b"\r\n\r\n").is_none(),
            None => false,
        }
    }

    fn manage_head_deadline<'t, H, W, C, P>(
        app: &H,
        slot: &mut link::Slot<W, listener::State<C>>,
        driver: &mut Driver,
        head_pending: bool,
        project: &P,
    ) where
        H: crate::timer::TimerHost<'t>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if head_pending && Self::head_still_pending(project(&mut slot.state.conn)) {
            if project(&mut slot.state.conn).head_deadline.is_none() {
                let waker = slot.make_waker(driver);
                let timer = crate::timer::TimerHost::timer(app);
                let deadline = std::time::Instant::now() + timer.head_timeout();
                if let Some(ticket) = timer.arm(deadline, &waker) {
                    project(&mut slot.state.conn).head_deadline = Some(ticket);
                }
            }
        } else if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(app).cancel(ticket);
        }
    }

    pub fn poll_head_deadline<'t, H, W>(
        app: &H,
        slot: &mut link::Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> bool
    where
        H: crate::timer::TimerHost<'t>,
        W: Wire,
    {
        Self::poll_head_deadline_proj(app, slot, aux, driver, identity_mut)
    }

    pub fn poll_head_deadline_proj<'t, H, W, C, P>(
        app: &H,
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        project: P,
    ) -> bool
    where
        H: crate::timer::TimerHost<'t>,
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
        slot.core.set_close_after();
        let buf = aux.write_buf_for(slot);
        let ud = slot.token();
        slot.submit_split_static(buf, 0, crate::CANNED_408, ud, driver);
        true
    }

    pub fn cancel_head_deadline_proj<'t, H, W, C, P>(
        app: &H,
        slot: &mut link::Slot<W, listener::State<C>>,
        project: P,
    ) where
        H: crate::timer::TimerHost<'t>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if let Some(ticket) = project(&mut slot.state.conn).head_deadline.take() {
            crate::timer::TimerHost::timer(app).cancel(ticket);
        }
    }

    pub fn on_send_complete<'t, H, W>(
        app: &mut H,
        sent: usize,
        slot: &mut link::Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) where
        H: Routing + crate::timer::TimerHost<'t>,
        W: Wire,
    {
        Self::on_send_complete_proj(app, sent, slot, aux, driver, identity_mut)
    }

    pub fn on_send_complete_proj<'t, H, W, C, P>(
        app: &mut H,
        _sent: usize,
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        project: P,
    ) where
        H: Routing + crate::timer::TimerHost<'t>,
        W: Wire,
        C: Default + 'static,
        P: Fn(&mut C) -> &mut ConnState,
    {
        if project(&mut slot.state.conn)
            .async_state
            .stream_slot
            .is_some()
        {
            return;
        }
        if let Some(DeferredAction::Close(reason)) =
            project(&mut slot.state.conn).deferred_action.take()
        {
            slot.core.set_close_after();
            if !reason.is_empty() {
                let buf = aux.write_buf_for(slot);
                let ud = slot.token();
                slot.submit_split_static(buf, 0, reason, ud, driver);
            }
            return;
        }
        if project(&mut slot.state.conn).recv.accum.is_some()
            && !project(&mut slot.state.conn).recv.is_frozen()
        {
            let _ = Self::run_proj(app, &[], slot, aux, driver, &project);
        }
    }
}

pub use sark_core::identity_mut;
