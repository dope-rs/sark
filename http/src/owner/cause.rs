/// Why protocol staging acquired another independently disposable owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Acquire {
    Committed,
    Split,
    StreamChunk,
}

/// Why an owner left protocol staging without becoming a transport owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Staging {
    TransportAccepted,
    Buffered,
    Reset,
    Closed,
    PeerRetired,
}

/// Why a transport released its physical response owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Transport {
    Sent,
    Acked,
    Stopped,
    Reset,
    Closed,
    PeerRetired,
}
