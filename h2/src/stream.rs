use o3::num::BoundedU32;

type StreamIdValue = BoundedU32<0, 0x7fff_ffff>;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(u32);

impl StreamId {
    pub const CONNECTION: Self = Self(0);
    pub const FIRST_CLIENT: Self = Self(1);
    pub const FIRST_SERVER: Self = Self(2);
    pub const MAX: u32 = 0x7fff_ffff;

    pub const fn new(raw: u32) -> Option<Self> {
        match StreamIdValue::new(raw) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn is_client(self) -> bool {
        self.0 != 0 && self.0 % 2 == 1
    }

    pub const fn is_server(self) -> bool {
        self.0 != 0 && self.0 % 2 == 0
    }

    pub(crate) const fn from_wire(raw: u32) -> Self {
        Self(raw & Self::MAX)
    }

    pub(crate) const fn wire_bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
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

macro_rules! directional_transitions {
    (
        $(
            fn $name:ident {
                own: { reserved: $own_reserved:ident, half_closed: $own_half:ident },
                peer: { reserved: $peer_reserved:ident, half_closed: $peer_half:ident },
            }
        )+
    ) => {
        impl State {
            $(
                pub fn $name(self, ev: Event) -> Result<Self, TransitionError> {
                    use Event::*;
                    use State::*;
                    use TransitionError::*;
                    match (self, ev) {
                        (_, RstStream) => Ok(Closed),

                        (Idle, Headers { end_stream: false }) => Ok(Open),
                        (Idle, Headers { end_stream: true }) => Ok($own_half),
                        (Idle, PushPromise) => Err(Protocol),
                        (Idle, Data { .. }) => Err(Protocol),

                        ($own_reserved, Headers { end_stream: false }) => Ok($peer_half),
                        ($own_reserved, Headers { end_stream: true }) => Ok(Closed),
                        ($own_reserved, PushPromise) => Err(Protocol),
                        ($own_reserved, Data { .. }) => Err(Protocol),

                        ($peer_reserved, Headers { .. }) => Err(Protocol),
                        ($peer_reserved, PushPromise) => Err(Protocol),
                        ($peer_reserved, Data { .. }) => Err(Protocol),

                        (Open, Headers { end_stream: false }) => Ok(Open),
                        (Open, Headers { end_stream: true }) => Ok($own_half),
                        (Open, Data { end_stream: false }) => Ok(Open),
                        (Open, Data { end_stream: true }) => Ok($own_half),
                        (Open, PushPromise) => Ok(Open),

                        ($own_half, Headers { .. }) => Err(StreamClosed),
                        ($own_half, Data { .. }) => Err(StreamClosed),
                        ($own_half, PushPromise) => Err(StreamClosed),

                        ($peer_half, Headers { end_stream: false }) => Ok($peer_half),
                        ($peer_half, Headers { end_stream: true }) => Ok(Closed),
                        ($peer_half, Data { end_stream: false }) => Ok($peer_half),
                        ($peer_half, Data { end_stream: true }) => Ok(Closed),
                        ($peer_half, PushPromise) => Ok($peer_half),

                        (Closed, Headers { .. }) => Err(StreamClosed),
                        (Closed, Data { .. }) => Err(StreamClosed),
                        (Closed, PushPromise) => Err(StreamClosed),
                    }
                }
            )+
        }

        impl Stream {
            $(
                pub fn $name(&mut self, ev: Event) -> Result<State, TransitionError> {
                    let next = self.state.$name(ev)?;
                    self.state = next;
                    Ok(next)
                }
            )+
        }
    };
}

impl State {
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

directional_transitions! {
    fn send {
        own: { reserved: ReservedLocal, half_closed: HalfClosedLocal },
        peer: { reserved: ReservedRemote, half_closed: HalfClosedRemote },
    }
    fn recv {
        own: { reserved: ReservedRemote, half_closed: HalfClosedRemote },
        peer: { reserved: ReservedLocal, half_closed: HalfClosedLocal },
    }
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
}

pub struct IdGen {
    next: Option<StreamId>,
    step: u32,
}

impl IdGen {
    pub fn new(first: StreamId) -> Self {
        Self {
            next: Some(first),
            step: 2,
        }
    }

    pub fn next_id(&mut self) -> Option<StreamId> {
        let id = self.next?;
        self.next = id.as_u32().checked_add(self.step).and_then(StreamId::new);
        Some(id)
    }

    pub fn peek(&self) -> Option<StreamId> {
        self.next
    }
}
