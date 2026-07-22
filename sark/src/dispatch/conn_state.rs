use dope::DriverContext;
use dope::manifold::listener::{self, SlotEgress, recv};
use dope_net::link;
use dope_net::wire::Wire;

pub const RECV_HEAD_CAP: usize = 64 * 1024;
pub const RECV_BODY_CAP: usize = 4 * 1024 * 1024;
pub type Recv = recv::State<RECV_HEAD_CAP, RECV_BODY_CAP>;

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamPhase {
    #[default]
    Streaming,
    Terminating,
}

#[derive(Default)]
pub struct AsyncConnState {
    pub task: Option<crate::fiber::ErasedTaskId>,
    pub task_route: u16,
    pub task_stream: bool,
    pub stream_pending: Option<o3::buffer::Shared>,
    pub stream_phase: StreamPhase,
}

impl AsyncConnState {
    pub fn has_task(&self) -> bool {
        self.task.is_some()
    }
}

pub enum Outcome {
    Send {
        written: usize,
        close_after: bool,
    },
    SendStatic {
        hdr_written: usize,
        body: &'static [u8],
        close_after: bool,
    },
    SendSplit {
        hdr_written: usize,
        body: o3::buffer::Shared,
        close_after: bool,
    },
    SendPooled {
        hdr_written: usize,
        body: o3::buffer::Pooled,
        close_after: bool,
    },
    Park,
    Close(&'static [u8]),
}

impl Outcome {
    pub fn into_consume(
        self,
        permit: DispatchPermit,
        consumption: Consumption,
        conn_close: bool,
    ) -> ConsumeOutcome {
        match self {
            response @ (Outcome::Send { .. }
            | Outcome::SendStatic { .. }
            | Outcome::SendSplit { .. }
            | Outcome::SendPooled { .. }) => ConsumeOutcome::Complete {
                permit,
                consumption,
                response,
                conn_close,
            },
            Outcome::Park => match consumption {
                Consumption::Buffered(consumed) => ConsumeOutcome::Park {
                    consumed,
                    close: conn_close,
                },
                Consumption::Discard { .. } => ConsumeOutcome::Close(crate::CANNED_500),
            },
            Outcome::Close(reason) => ConsumeOutcome::Close(reason),
        }
    }

    pub fn apply<'d, W: Wire, C: Default + 'static>(
        self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let close_after = match &self {
            Outcome::Park => return true,
            Outcome::Close(reason) => {
                slot.set_close_after();
                if !reason.is_empty() {
                    let buf = aux.write_buf_for(slot);
                    let user_data = slot.token();
                    return slot.submit_split_static(buf, 0, reason, user_data, driver);
                }
                return true;
            }
            Outcome::Send { close_after, .. }
            | Outcome::SendStatic { close_after, .. }
            | Outcome::SendSplit { close_after, .. }
            | Outcome::SendPooled { close_after, .. } => *close_after,
        };
        if close_after {
            slot.set_close_after();
        }
        let buf = aux.write_buf_for(slot);
        let user_data = slot.token();
        match self {
            Outcome::Send { written, .. } => slot.submit_buffered(buf, written, user_data, driver),
            Outcome::SendStatic {
                hdr_written, body, ..
            } => slot.submit_split_static(buf, hdr_written, body, user_data, driver),
            Outcome::SendSplit {
                hdr_written, body, ..
            } => slot.submit_split_shared(buf, hdr_written, body, user_data, driver),
            Outcome::SendPooled {
                hdr_written, body, ..
            } => slot.submit_split_pooled(buf, hdr_written, body, user_data, driver),
            Outcome::Park | Outcome::Close(_) => unreachable!(),
        }
    }
}

pub enum DeferredAction {
    Close(&'static [u8]),
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum PendingFrame {
    #[default]
    Head,
    FixedBody(usize),
    ChunkedBody,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NeedMore {
    Head,
    FixedBody(usize),
    ChunkedBody,
}

pub enum Consumption {
    Buffered(usize),
    Discard { head: usize, body: usize },
}

pub struct DispatchPermit {
    _priv: (),
}

impl Default for DispatchPermit {
    fn default() -> Self {
        Self::new()
    }
}

impl DispatchPermit {
    pub const fn new() -> Self {
        Self { _priv: () }
    }
}

pub enum ConsumeOutcome {
    NeedMore {
        permit: DispatchPermit,
        state: NeedMore,
    },
    Complete {
        permit: DispatchPermit,
        consumption: Consumption,
        response: Outcome,
        conn_close: bool,
    },
    Streamed {
        consumed: usize,
        written: usize,
        close: bool,
    },
    Park {
        consumed: usize,
        close: bool,
    },
    Close(&'static [u8]),
}

pub struct ConnState {
    pub recv: Recv,
    pub async_state: AsyncConnState,
    pub deferred_action: Option<DeferredAction>,
    pub deferred_close: bool,
    pub conn_id: ::dope::driver::token::Token,
    pub recv_view: Option<o3::buffer::Shared>,
    pub pipeline: super::pipeline::Pipeline,
    pub head_deadline: Option<crate::timer::Ticket>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            recv: Recv::default(),
            async_state: AsyncConnState::default(),
            deferred_action: None,
            deferred_close: false,
            conn_id: ::dope::driver::token::Token::new(
                0,
                ::dope::driver::token::SlotIndex::new(0),
                ::dope::driver::token::Epoch::INITIAL,
            ),
            recv_view: None,
            pipeline: super::pipeline::Pipeline::default(),
            head_deadline: None,
        }
    }
}
