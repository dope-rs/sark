#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

impl StreamId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn index(self) -> u64 {
        self.0 >> 2
    }

    pub const fn kind(self) -> StreamKind {
        match self.0 & 0x3 {
            0x0 => StreamKind::ClientBidi,
            0x1 => StreamKind::ServerBidi,
            0x2 => StreamKind::ClientUni,
            _ => StreamKind::ServerUni,
        }
    }

    pub const fn is_bidi(self) -> bool {
        matches!(self.kind(), StreamKind::ClientBidi | StreamKind::ServerBidi)
    }

    pub const fn is_client_bidi(self) -> bool {
        matches!(self.kind(), StreamKind::ClientBidi)
    }

    pub const fn is_server_uni(self) -> bool {
        matches!(self.kind(), StreamKind::ServerUni)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StreamKind {
    ClientBidi,
    ServerBidi,
    ClientUni,
    ServerUni,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum UniStreamType {
    Control,
    Push,
    QpackEncoder,
    QpackDecoder,
    Unknown(u64),
}

impl UniStreamType {
    pub const fn from_wire(value: u64) -> Self {
        match value {
            crate::frame::STREAM_TYPE_CONTROL => Self::Control,
            crate::frame::STREAM_TYPE_PUSH => Self::Push,
            crate::frame::STREAM_TYPE_QPACK_ENCODER => Self::QpackEncoder,
            crate::frame::STREAM_TYPE_QPACK_DECODER => Self::QpackDecoder,
            value => Self::Unknown(value),
        }
    }

    pub const fn is_critical(self) -> bool {
        matches!(
            self,
            Self::Control | Self::QpackEncoder | Self::QpackDecoder
        )
    }
}
