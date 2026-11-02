use o3::buffer::write;

use crate::{
    frame::{self, controls, layout, numbers},
    stream,
};

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct PushPromise<'a> {
    stream: stream::Nonzero,
    promised: stream::Nonzero,
    chain: frame::Block,
    block: &'a [u8],
    length: numbers::Length,
}

impl<'a> PushPromise<'a> {
    pub fn new(
        stream: stream::Id,
        promised: stream::Id,
        chain: frame::Block,
        block: &'a [u8],
    ) -> Option<Self> {
        Self::from(stream.nonzero()?, promised.nonzero()?, chain, block)
    }

    fn from(
        stream: stream::Nonzero,
        promised: stream::Nonzero,
        chain: frame::Block,
        block: &'a [u8],
    ) -> Option<Self> {
        let length = 4usize.checked_add(block.len())?;
        Some(Self {
            stream,
            promised,
            chain,
            block,
            length: numbers::Length::from_usize(length)?,
        })
    }

    pub fn parse(header: layout::Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        let stream = header.try_stream()?;
        let unpadded = header.flags.strip(payload)?;
        if unpadded.len() < 4 {
            return Err(numbers::ParseError::FrameMinimum {
                kind: numbers::Type::PushPromise,
                actual: unpadded.len(),
                minimum: 4,
            });
        }
        let promised = stream::Id::from_wire(u32::from_be_bytes([
            unpadded[0],
            unpadded[1],
            unpadded[2],
            unpadded[3],
        ]));
        let promised = promised.nonzero().ok_or(numbers::ParseError::Promise {
            stream: header.stream_id,
            promised,
        })?;
        let chain = if header.flags.contains(numbers::Flags::END_HEADERS) {
            frame::Block::End
        } else {
            frame::Block::More
        };
        Self::from(stream, promised, chain, &unpadded[4..]).ok_or(numbers::ParseError::FrameLimit {
            kind: numbers::Type::PushPromise,
            actual: unpadded.len(),
            limit: numbers::Length::MAX as usize,
        })
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = match self.chain {
            frame::Block::More => 0,
            frame::Block::End => numbers::Flags::END_HEADERS,
        };
        let header = layout::Header {
            length: self.length,
            kind: numbers::Type::PushPromise,
            flags: numbers::Flags::new(flags),
            stream_id: self.stream.get(),
        }
        .wire_bytes();
        out.write_slices([&header, &self.promised.get().wire_bytes(), self.block])
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Ping {
    pub ack: frame::Ack,
    pub opaque: [u8; 8],
}

impl Ping {
    pub fn parse(header: layout::Header, payload: &[u8]) -> Result<Self, numbers::ParseError> {
        if payload.len() != 8 {
            return Err(numbers::ParseError::FrameSize {
                kind: numbers::Type::Ping,
                actual: payload.len(),
                expected: 8,
            });
        }
        header.check_conn()?;
        let mut opaque = [0u8; 8];
        opaque.copy_from_slice(payload);
        Ok(Self {
            ack: frame::Ack::from(header.flags),
            opaque,
        })
    }

    pub(crate) fn wire_bytes(&self) -> [u8; frame::HEADER_LEN + 8] {
        let header = self.header();
        let mut bytes = [0; frame::HEADER_LEN + 8];
        let (head, opaque) = bytes.split_at_mut(frame::HEADER_LEN);
        head.copy_from_slice(&header);
        opaque.copy_from_slice(&self.opaque);
        bytes
    }

    fn header(&self) -> [u8; frame::HEADER_LEN] {
        layout::Header {
            length: numbers::Length::EIGHT,
            kind: numbers::Type::Ping,
            flags: numbers::Flags::new(self.ack.flags()),
            stream_id: stream::Id::CONNECTION,
        }
        .wire_bytes()
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slices([&self.header(), &self.opaque])
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct GoAway<'a> {
    last: stream::Id,
    error: numbers::ErrorCode,
    debug: &'a [u8],
    length: numbers::Length,
}

impl<'a> GoAway<'a> {
    pub(crate) fn terminal(
        last: stream::Id,
        error: numbers::ErrorCode,
    ) -> [u8; frame::HEADER_LEN + 8] {
        Self::head(last, error, numbers::Length::EIGHT)
    }

    pub fn new(last: stream::Id, error: numbers::ErrorCode, debug: &'a [u8]) -> Option<Self> {
        let length = 8usize.checked_add(debug.len())?;
        Some(Self {
            last,
            error,
            debug,
            length: numbers::Length::from_usize(length)?,
        })
    }

    pub const fn last(&self) -> stream::Id {
        self.last
    }

    pub const fn error(&self) -> numbers::ErrorCode {
        self.error
    }

    pub fn parse(header: layout::Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        header.check_conn()?;
        if payload.len() < 8 {
            return Err(numbers::ParseError::FrameMinimum {
                kind: numbers::Type::GoAway,
                actual: payload.len(),
                minimum: 8,
            });
        }
        let last = stream::Id::from_wire(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]));
        let err_raw = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        Self::new(last, numbers::ErrorCode::from_u32(err_raw), &payload[8..]).ok_or(
            numbers::ParseError::FrameLimit {
                kind: numbers::Type::GoAway,
                actual: payload.len(),
                limit: numbers::Length::MAX as usize,
            },
        )
    }

    pub(crate) fn wire_parts(&self) -> ([u8; frame::HEADER_LEN + 8], &'a [u8]) {
        (Self::head(self.last, self.error, self.length), self.debug)
    }

    fn head(
        last: stream::Id,
        error: numbers::ErrorCode,
        length: numbers::Length,
    ) -> [u8; frame::HEADER_LEN + 8] {
        let header = layout::Header {
            length,
            kind: numbers::Type::GoAway,
            flags: numbers::Flags::new(0),
            stream_id: stream::Id::CONNECTION,
        }
        .wire_bytes();
        let mut bytes = [0; frame::HEADER_LEN + 8];
        let (head, payload) = bytes.split_at_mut(frame::HEADER_LEN);
        head.copy_from_slice(&header);
        let (stream, code) = payload.split_at_mut(4);
        stream.copy_from_slice(&last.wire_bytes());
        code.copy_from_slice(&(error as u32).to_be_bytes());
        bytes
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let (head, debug) = self.wire_parts();
        out.write_slices([&head, debug])
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct WindowUpdate {
    pub stream_id: stream::Id,
    pub increment: numbers::Increment,
}

impl WindowUpdate {
    pub fn parse(header: layout::Header, payload: &[u8]) -> Result<Self, numbers::ParseError> {
        if payload.len() != 4 {
            return Err(numbers::ParseError::FrameSize {
                kind: numbers::Type::WindowUpdate,
                actual: payload.len(),
                expected: 4,
            });
        }
        let amount = numbers::Increment::from_wire(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]))
        .ok_or(numbers::ParseError::ZeroIncrement)?;
        Ok(Self {
            stream_id: header.stream_id,
            increment: amount,
        })
    }

    pub(crate) fn wire_bytes(self) -> [u8; frame::HEADER_LEN + 4] {
        let header = layout::Header {
            length: numbers::Length::FOUR,
            kind: numbers::Type::WindowUpdate,
            flags: numbers::Flags::new(0),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        let amount = self.increment.wire_bytes();
        let mut wire = [0; frame::HEADER_LEN + 4];
        wire[..frame::HEADER_LEN].copy_from_slice(&header);
        wire[frame::HEADER_LEN..].copy_from_slice(&amount);
        wire
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Continuation<'a> {
    stream: stream::Nonzero,
    chain: frame::Block,
    block: &'a [u8],
    length: numbers::Length,
}

impl<'a> Continuation<'a> {
    pub fn new(stream: stream::Id, chain: frame::Block, block: &'a [u8]) -> Option<Self> {
        Self::from(stream.nonzero()?, chain, block)
    }

    fn from(stream: stream::Nonzero, chain: frame::Block, block: &'a [u8]) -> Option<Self> {
        Some(Self {
            stream,
            chain,
            block,
            length: numbers::Length::from_usize(block.len())?,
        })
    }

    pub fn parse(header: layout::Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        let chain = if header.flags.contains(numbers::Flags::END_HEADERS) {
            frame::Block::End
        } else {
            frame::Block::More
        };
        Self::from(header.try_stream()?, chain, payload).ok_or(numbers::ParseError::FrameLimit {
            kind: numbers::Type::Continuation,
            actual: payload.len(),
            limit: numbers::Length::MAX as usize,
        })
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = match self.chain {
            frame::Block::More => 0,
            frame::Block::End => numbers::Flags::END_HEADERS,
        };
        let header = layout::Header {
            length: self.length,
            kind: numbers::Type::Continuation,
            flags: numbers::Flags::new(flags),
            stream_id: self.stream.get(),
        }
        .wire_bytes();
        out.write_slices([&header, self.block])
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Payload<'a> {
    Data(layout::Data<'a>),
    Headers(layout::Headers<'a>),
    Priority(controls::Priority),
    RstStream(controls::RstStream),
    Settings(controls::Settings<'a>),
    PushPromise(PushPromise<'a>),
    Ping(Ping),
    GoAway(GoAway<'a>),
    WindowUpdate(WindowUpdate),
    Continuation(Continuation<'a>),
}

impl<'a> Payload<'a> {
    pub fn parse(buf: &'a [u8]) -> Result<(Self, usize), numbers::ParseError> {
        let header = layout::Header::parse(buf)?;
        let total = frame::HEADER_LEN + header.length.into_usize();
        if buf.len() < total {
            return Err(numbers::ParseError::Need {
                actual: buf.len(),
                required: total,
            });
        }
        let payload = &buf[frame::HEADER_LEN..total];
        let frame = match header.kind {
            numbers::Type::Data => Self::Data(layout::Data::parse(header, payload)?),
            numbers::Type::Headers => Self::Headers(layout::Headers::parse(header, payload)?),
            numbers::Type::Priority => Self::Priority(controls::Priority::parse(header, payload)?),
            numbers::Type::RstStream => {
                Self::RstStream(controls::RstStream::parse(header, payload)?)
            }
            numbers::Type::Settings => Self::Settings(controls::Settings::parse(header, payload)?),
            numbers::Type::PushPromise => Self::PushPromise(PushPromise::parse(header, payload)?),
            numbers::Type::Ping => Self::Ping(Ping::parse(header, payload)?),
            numbers::Type::GoAway => Self::GoAway(GoAway::parse(header, payload)?),
            numbers::Type::WindowUpdate => {
                Self::WindowUpdate(WindowUpdate::parse(header, payload)?)
            }
            numbers::Type::Continuation => {
                Self::Continuation(Continuation::parse(header, payload)?)
            }
        };
        Ok((frame, total))
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        match self {
            Self::Data(f) => f.encode(out),
            Self::Headers(f) => f.encode(out),
            Self::Priority(f) => f.encode(out),
            Self::RstStream(f) => out.write_slice(&f.wire_bytes()),
            Self::Settings(f) => f.encode(out),
            Self::PushPromise(f) => f.encode(out),
            Self::Ping(f) => f.encode(out),
            Self::GoAway(f) => f.encode(out),
            Self::WindowUpdate(f) => out.write_slice(&f.wire_bytes()),
            Self::Continuation(f) => f.encode(out),
        }
    }
}
