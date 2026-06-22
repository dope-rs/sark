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
    pub stream_slot: Option<(u8, ::dope::fiber::TaskId)>,
    pub stream_pending: Option<o3::buffer::Shared>,
    pub stream_phase: StreamPhase,
    pub pending_wake: Option<(u8, ::dope::fiber::TaskId)>,
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
    Park,
    Close(&'static [u8]),
}

pub enum DeferredAction {
    Close(&'static [u8]),
}

#[derive(Default)]
pub struct PipelineState {
    pub expected_total: Option<u32>,
    pub stream_body_remaining: u32,
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
        content_length: Option<usize>,
    },
    Done {
        permit: DispatchPermit,
        consumed: usize,
        written: usize,
        close: bool,
    },
    DoneStatic {
        permit: DispatchPermit,
        consumed: usize,
        hdr_written: usize,
        body: &'static [u8],
        close: bool,
    },
    DoneSplit {
        permit: DispatchPermit,
        consumed: usize,
        hdr_written: usize,
        body: o3::buffer::Shared,
        close: bool,
    },
    Streamed {
        consumed: usize,
        written: usize,
        close: bool,
    },
    StreamArmed {
        permit: DispatchPermit,
        head_consumed: usize,
        body_total: usize,
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
    pub conn_id: ::dope::runtime::token::Token,
    pub chunked_body: Option<o3::buffer::Shared>,
    pub pipeline: PipelineState,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            recv: Recv::default(),
            async_state: AsyncConnState::default(),
            deferred_action: None,
            deferred_close: false,
            conn_id: ::dope::runtime::token::Token::new(
                0,
                ::dope::runtime::token::LocalIdx::new(0),
                ::dope::runtime::token::Epoch::INITIAL,
            ),
            chunked_body: None,
            pipeline: PipelineState::default(),
        }
    }
}
