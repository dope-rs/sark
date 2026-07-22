use dope::manifold::listener::recv;

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

#[derive(Default)]
pub struct PipelineState {
    pub pending_frame: PendingFrame,
    pub discard_body_remaining: usize,
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
    pub pipeline: PipelineState,
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
            pipeline: PipelineState::default(),
            head_deadline: None,
        }
    }
}
