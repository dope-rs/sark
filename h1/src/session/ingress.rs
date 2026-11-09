//! Connection-local request ingress ownership.
use std::{convert::Infallible, mem};

use sark_core::{body::admit::Retention, life::audit};

use crate::{
    pump,
    wire::{
        Failure,
        body::{chunked, fixed, ingress},
        parsing, recv,
    },
};

#[derive(Default)]
pub(super) enum Protocol<'d, P, R, H> {
    #[default]
    Head,
    Body(P),
    Working(P),
    Ready(R),
    PendingHead(H),
    Line(parsing::Line),
    Prefix(sark_http::preface::Prefix),
    Reject(parsing::Rejected<'d>),
}

impl<'d, P, R, H> Protocol<'d, P, R, H> {
    pub(super) const fn window(&self) -> recv::Window {
        match self {
            Self::Head
            | Self::Ready(_)
            | Self::PendingHead(_)
            | Self::Line(_)
            | Self::Prefix(_)
            | Self::Reject(_) => recv::Window::Head,
            Self::Body(_) | Self::Working(_) => recv::Window::Body,
        }
    }

    /// Takes the next frame decision together with the pending body it selected.
    ///
    /// Reading only the frame kind and taking the body later would be two
    /// derivations of one decision; the value returned here is that decision.
    pub(super) fn take_frame(&mut self, input: crate::Input<'_>) -> Frame<'d, P, R, H> {
        match mem::replace(self, Self::Head) {
            Self::Head => Frame::Head(HeadFrame::New),
            Self::PendingHead(head) => Frame::Head(HeadFrame::Pending(head)),
            Self::Line(line) => Frame::Head(HeadFrame::Line(line.advance(input))),
            Self::Prefix(prefix) => {
                Frame::Head(HeadFrame::Line(parsing::Line::prefixed(prefix, input)))
            }
            Self::Reject(reject) => Frame::Head(HeadFrame::Rejected(reject)),
            Self::Body(pending) => Frame::Body(pending),
            Self::Working(pending) => Frame::Working(pending),
            Self::Ready(pending) => Frame::Ready(pending),
        }
    }
}

/// One frame decision that owns the pending body it selected.
pub(super) enum Frame<'d, P, R, H> {
    Head(HeadFrame<'d, H>),
    Body(P),
    Working(P),
    Ready(R),
}

pub(super) enum HeadFrame<'d, H> {
    New,
    Pending(H),
    Line(Result<parsing::LineProgress, parsing::LineError<'d>>),
    Rejected(parsing::Rejected<'d>),
}

enum FramingState {
    Empty(Vec<(u32, u32)>),
    Fixed(fixed::Fixed, Vec<(u32, u32)>),
    Chunked(chunked::Chunked),
}

const _: () = assert!(mem::size_of::<FramingState>() == mem::size_of::<chunked::Chunked>());

impl Default for FramingState {
    fn default() -> Self {
        Self::Empty(Vec::new())
    }
}

impl FramingState {
    /// The fixed decoder, or `None` when this connection committed to chunked framing.
    fn fixed(&mut self) -> Option<&mut fixed::Fixed> {
        if let Self::Empty(ranges) = self {
            *self = Self::Fixed(fixed::Fixed::default(), mem::take(ranges));
        }
        match self {
            Self::Fixed(state, _) => Some(state),
            Self::Empty(_) | Self::Chunked(_) => None,
        }
    }

    /// The chunked decoder, or `None` when this connection committed to fixed framing.
    fn chunked(&mut self) -> Option<&mut chunked::Chunked> {
        if let Self::Empty(ranges) = self {
            *self = Self::Chunked(chunked::Chunked::new(mem::take(ranges)));
        }
        match self {
            Self::Chunked(state) => Some(state),
            Self::Empty(_) | Self::Fixed(..) => None,
        }
    }

    fn reset(&mut self) {
        let ranges = match mem::take(self) {
            Self::Empty(ranges) | Self::Fixed(_, ranges) => ranges,
            Self::Chunked(state) => state.into_ranges(),
        };
        *self = Self::Empty(ranges);
    }
}

/// Exact request owners and framing state for one connection.
pub struct Ingress<'d, Pending, Ready = Infallible, Heads = Infallible> {
    pub(crate) recv: recv::Buffer,
    framing: FramingState,
    pub(crate) payload: ingress::Ingress<'d>,
    pub(super) protocol: Protocol<'d, Pending, Ready, Heads>,
    pub(crate) discard_remaining: usize,
}

impl<'d, Pending, Ready, Heads> Default for Ingress<'d, Pending, Ready, Heads> {
    fn default() -> Self {
        Self::new(Protocol::default())
    }
}

impl<'d, Pending, Ready, Heads> Ingress<'d, Pending, Ready, Heads> {
    pub(super) fn new(protocol: Protocol<'d, Pending, Ready, Heads>) -> Self {
        Self {
            recv: recv::Buffer::default(),
            framing: FramingState::default(),
            payload: ingress::Ingress::default(),
            protocol,
            discard_remaining: 0,
        }
    }

    /// A continuation still owns work which does not require another input byte.
    #[doc(hidden)]
    pub fn is_ready(&self) -> bool {
        matches!(self.protocol, Protocol::Ready(_) | Protocol::Working(_))
    }

    /// Whether admitted request bytes still belong to the HTTP discard path.
    #[doc(hidden)]
    pub fn is_discard(&self) -> bool {
        self.discard_remaining != 0
    }

    /// Releases discard ownership after transport handoff or terminal input.
    #[doc(hidden)]
    pub fn end_discard(&mut self) {
        self.discard_remaining = 0;
    }

    /// Removes the exact continuation during connection retirement.
    pub fn take_pending(&mut self) -> Option<Pending> {
        match mem::replace(&mut self.protocol, Protocol::Head) {
            Protocol::Body(body) | Protocol::Working(body) => Some(body),
            pending => {
                self.protocol = pending;
                None
            }
        }
    }

    /// Removes a framed continuation during connection retirement.
    pub fn take_ready(&mut self) -> Option<Ready> {
        match mem::replace(&mut self.protocol, Protocol::Head) {
            Protocol::Ready(ready) => Some(ready),
            pending => {
                self.protocol = pending;
                None
            }
        }
    }

    /// Drops protocol-local ingress after route cancellation.
    pub fn retire(&mut self) {
        if matches!(
            self.protocol,
            Protocol::PendingHead(_)
                | Protocol::Line(_)
                | Protocol::Prefix(_)
                | Protocol::Reject(_)
        ) {
            self.protocol = Protocol::Head;
        }
        self.recv = recv::Buffer::default();
        self.end_discard();
        self.framing.reset();
        self.payload.reset();
    }

    /// Samples exact ingress roots with admitted `u32` callback lengths.
    /// o3's 64-bit domain fits `(u32::MAX + 1) * PAGE_BYTES` plus bounded recv/head.
    /// Projection preserves totals; subsequent appends reset storage.
    pub fn audit(
        &self,
        head: impl FnOnce(&Pending) -> u32,
        ready: impl FnOnce(&Ready) -> u32,
        pending: impl FnOnce(&Heads) -> u32,
    ) -> audit::Snapshot {
        let (roots, head) = match &self.protocol {
            Protocol::Head => (0, 0),
            Protocol::Body(pending) | Protocol::Working(pending) => (1, head(pending) as usize),
            Protocol::Ready(pending) => (1, ready(pending) as usize),
            Protocol::PendingHead(value) => (1, pending(value) as usize),
            Protocol::Line(line) => (1, line.len()),
            Protocol::Prefix(prefix) => (1, prefix.as_ref().len()),
            Protocol::Reject(reject) => (1, reject.len()),
        };
        audit::Snapshot::default().request(
            roots,
            self.recv.len() + self.payload.retained_len() + head,
            usize::from(self.recv.len() != 0)
                + self.payload.audit_chunks()
                + usize::from(head != 0),
            roots,
        )
    }

    pub(crate) fn begin_fixed(
        &mut self,
        input: crate::Input<'_>,
        head: usize,
        total: usize,
        keep: Retention,
    ) -> Result<usize, fixed::Error> {
        self.framing.fixed().ok_or(fixed::Error::Bad)?.begin(
            input,
            head,
            total,
            keep,
            &mut self.payload,
        )
    }

    pub(crate) fn decode<'frame>(
        &'frame mut self,
        input: crate::Input<'_>,
        head: usize,
        limit: usize,
        keep: Retention,
    ) -> Result<Option<chunked::Complete<'frame, 'd>>, chunked::Decode> {
        self.framing
            .chunked()
            .ok_or(chunked::Decode::Framing)?
            .decode(input, head, limit, keep, &mut self.payload)
    }

    pub(super) fn resume_frame(&mut self, wire: &[u8]) -> Result<Option<super::Complete>, Failure> {
        let done = match &mut self.framing {
            FramingState::Empty(_) => return Err(Failure::BadRequest),
            FramingState::Fixed(state, _) => state
                .resume(wire, &mut self.payload)
                .map_err(Failure::Fixed)?,
            FramingState::Chunked(state) => state
                .resume(wire, &mut self.payload)
                .map_err(Failure::Chunk)?,
        };
        Ok(done.map(|done| super::Complete {
            consumed: done.consumed,
            body_len: done.body_len,
        }))
    }

    pub(crate) fn absorb(&mut self, bytes: &[u8]) -> Result<(), pump::Reject> {
        self.recv.backlog(pump::Operation::Append, bytes)
    }

    pub(crate) fn recv_window(&self) -> recv::Window {
        self.protocol.window()
    }

    pub fn finish_frame(&mut self) {
        self.protocol = Protocol::Head;
        self.framing.reset();
        self.payload.reset();
    }
}
