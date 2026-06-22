#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u32);

impl StreamId {
    pub const CONNECTION: Self = Self(0);
    pub const MAX: u32 = 0x7fff_ffff;

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn is_client(self) -> bool {
        self.0 != 0 && self.0 % 2 == 1
    }

    pub fn is_server(self) -> bool {
        self.0 != 0 && self.0.is_multiple_of(2)
    }

    pub fn from_u32_masked(raw: u32) -> Self {
        Self(raw & Self::MAX)
    }

    pub fn masked(self) -> u32 {
        self.0 & Self::MAX
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    ReservedLocal,
    ReservedRemote,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Side {
    Local,
    Remote,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Headers { end_stream: bool },
    PushPromise,
    Data { end_stream: bool },
    RstStream,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransitionError {
    Protocol,
    StreamClosed,
}

impl State {
    pub fn send(self, ev: Event) -> Result<Self, TransitionError> {
        use Event::*;
        use State::*;
        use TransitionError::*;
        match (self, ev) {
            (_, RstStream) => Ok(Closed),

            (Idle, Headers { end_stream: false }) => Ok(Open),
            (Idle, Headers { end_stream: true }) => Ok(HalfClosedLocal),
            (Idle, PushPromise) => Err(Protocol),
            (Idle, Data { .. }) => Err(Protocol),

            (ReservedLocal, Headers { end_stream: false }) => Ok(HalfClosedRemote),
            (ReservedLocal, Headers { end_stream: true }) => Ok(Closed),
            (ReservedLocal, PushPromise) => Err(Protocol),
            (ReservedLocal, Data { .. }) => Err(Protocol),

            (ReservedRemote, Headers { .. }) => Err(Protocol),
            (ReservedRemote, PushPromise) => Err(Protocol),
            (ReservedRemote, Data { .. }) => Err(Protocol),

            (Open, Headers { end_stream: false }) => Ok(Open),
            (Open, Headers { end_stream: true }) => Ok(HalfClosedLocal),
            (Open, Data { end_stream: false }) => Ok(Open),
            (Open, Data { end_stream: true }) => Ok(HalfClosedLocal),
            (Open, PushPromise) => Ok(Open),

            (HalfClosedLocal, Headers { .. }) => Err(StreamClosed),
            (HalfClosedLocal, Data { .. }) => Err(StreamClosed),
            (HalfClosedLocal, PushPromise) => Err(StreamClosed),

            (HalfClosedRemote, Headers { end_stream: false }) => Ok(HalfClosedRemote),
            (HalfClosedRemote, Headers { end_stream: true }) => Ok(Closed),
            (HalfClosedRemote, Data { end_stream: false }) => Ok(HalfClosedRemote),
            (HalfClosedRemote, Data { end_stream: true }) => Ok(Closed),
            (HalfClosedRemote, PushPromise) => Ok(HalfClosedRemote),

            (Closed, Headers { .. }) => Err(StreamClosed),
            (Closed, Data { .. }) => Err(StreamClosed),
            (Closed, PushPromise) => Err(StreamClosed),
        }
    }

    pub fn recv(self, ev: Event) -> Result<Self, TransitionError> {
        use Event::*;
        use State::*;
        use TransitionError::*;
        match (self, ev) {
            (_, RstStream) => Ok(Closed),

            (Idle, Headers { end_stream: false }) => Ok(Open),
            (Idle, Headers { end_stream: true }) => Ok(HalfClosedRemote),
            (Idle, PushPromise) => Err(Protocol),
            (Idle, Data { .. }) => Err(Protocol),

            (ReservedLocal, Headers { .. }) => Err(Protocol),
            (ReservedLocal, PushPromise) => Err(Protocol),
            (ReservedLocal, Data { .. }) => Err(Protocol),

            (ReservedRemote, Headers { end_stream: false }) => Ok(HalfClosedLocal),
            (ReservedRemote, Headers { end_stream: true }) => Ok(Closed),
            (ReservedRemote, PushPromise) => Err(Protocol),
            (ReservedRemote, Data { .. }) => Err(Protocol),

            (Open, Headers { end_stream: false }) => Ok(Open),
            (Open, Headers { end_stream: true }) => Ok(HalfClosedRemote),
            (Open, Data { end_stream: false }) => Ok(Open),
            (Open, Data { end_stream: true }) => Ok(HalfClosedRemote),
            (Open, PushPromise) => Ok(Open),

            (HalfClosedLocal, Headers { end_stream: false }) => Ok(HalfClosedLocal),
            (HalfClosedLocal, Headers { end_stream: true }) => Ok(Closed),
            (HalfClosedLocal, Data { end_stream: false }) => Ok(HalfClosedLocal),
            (HalfClosedLocal, Data { end_stream: true }) => Ok(Closed),
            (HalfClosedLocal, PushPromise) => Ok(HalfClosedLocal),

            (HalfClosedRemote, Headers { .. }) => Err(StreamClosed),
            (HalfClosedRemote, Data { .. }) => Err(StreamClosed),
            (HalfClosedRemote, PushPromise) => Err(StreamClosed),

            (Closed, Headers { .. }) => Err(StreamClosed),
            (Closed, Data { .. }) => Err(StreamClosed),
            (Closed, PushPromise) => Err(StreamClosed),
        }
    }

    pub fn step(self, ev: Event, side: Side) -> Result<Self, TransitionError> {
        match side {
            Side::Local => self.send(ev),
            Side::Remote => self.recv(ev),
        }
    }
}

pub struct Stream {
    pub id: StreamId,
    pub state: State,
    pub peer_headers_received: bool,
}

impl Stream {
    pub fn new(id: StreamId) -> Self {
        Self {
            id,
            state: State::Idle,
            peer_headers_received: false,
        }
    }

    pub fn reserve_local(id: StreamId) -> Self {
        Self {
            id,
            state: State::ReservedLocal,
            peer_headers_received: false,
        }
    }

    pub fn reserve_remote(id: StreamId) -> Self {
        Self {
            id,
            state: State::ReservedRemote,
            peer_headers_received: false,
        }
    }

    pub fn send(&mut self, ev: Event) -> Result<State, TransitionError> {
        let next = self.state.send(ev)?;
        self.state = next;
        Ok(next)
    }

    pub fn recv(&mut self, ev: Event) -> Result<State, TransitionError> {
        let next = self.state.recv(ev)?;
        self.state = next;
        Ok(next)
    }
}

pub struct IdGen {
    next: u32,
    step: u32,
}

impl IdGen {
    pub fn new(first: u32) -> Self {
        Self {
            next: first,
            step: 2,
        }
    }

    pub fn next_id(&mut self) -> Option<StreamId> {
        if self.next > StreamId::MAX {
            return None;
        }
        let id = StreamId(self.next);
        self.next = self.next.wrapping_add(self.step);
        Some(id)
    }

    pub fn peek(&self) -> StreamId {
        StreamId(self.next)
    }
}
