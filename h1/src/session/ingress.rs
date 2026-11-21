//! Connection-local request ingress ownership.
use std::{convert::Infallible, mem};

use o3::mem::slot::Storage;
use sark_core::{body::admit::Retention, life::audit};

use crate::{
    pump, session,
    wire::{
        Failure,
        body::{Frame, chunked, fixed, ingress},
        parsing, recv,
    },
};

pub(super) enum Protocol<'d, P, R, H> {
    Body(P),
    Working(P),
    Ready(R),
    PendingHead(H),
    Line(parsing::Line),
    Prefix(sark_http::preface::Prefix),
    Reject(parsing::Rejected<'d>),
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

    fn finish(&mut self, payload: &mut ingress::Ingress<'_>) {
        let ranges = match self {
            Self::Empty(_) => return,
            Self::Fixed(_, ranges) => mem::take(ranges),
            Self::Chunked(state) => state.take_ranges(),
        };
        *self = Self::Empty(ranges);
        payload.reset();
    }
}

/// Exact request owners and framing state for one connection.
pub struct Ingress<'d, Pending, Ready = Infallible, Heads = Infallible> {
    pub(crate) recv: recv::Buffer,
    framing: FramingState,
    pub(crate) payload: ingress::Ingress<'d>,
    pub(super) protocol: Storage<Protocol<'d, Pending, Ready, Heads>>,
    pub(crate) discard_remaining: usize,
}

impl<'d, Pending, Ready, Heads> Default for Ingress<'d, Pending, Ready, Heads> {
    fn default() -> Self {
        Self {
            recv: recv::Buffer::default(),
            framing: FramingState::default(),
            payload: ingress::Ingress::default(),
            protocol: Storage::new(()),
            discard_remaining: 0,
        }
    }
}

impl<'d, Pending, Ready, Heads> Ingress<'d, Pending, Ready, Heads> {
    pub(super) fn park(&mut self, pending: Protocol<'d, Pending, Ready, Heads>) {
        self.protocol = Storage::filled(pending, ());
    }

    pub(super) fn window(&self) -> recv::Window {
        match self.protocol.get() {
            Some(Protocol::Body(_) | Protocol::Working(_)) => recv::Window::Body,
            _ => recv::Window::Head,
        }
    }

    /// A continuation still owns work which does not require another input byte.
    #[doc(hidden)]
    pub fn is_ready(&self) -> bool {
        matches!(
            self.protocol.get(),
            Some(Protocol::Ready(_) | Protocol::Working(_))
        )
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
        let (pending, vacant) = self.protocol.occupied()?.take();
        match pending {
            Protocol::Body(body) | Protocol::Working(body) => Some(body),
            pending => {
                let _ = vacant.insert(pending);
                None
            }
        }
    }

    /// Removes a framed continuation during connection retirement.
    pub fn take_ready(&mut self) -> Option<Ready> {
        let (pending, vacant) = self.protocol.occupied()?.take();
        match pending {
            Protocol::Ready(ready) => Some(ready),
            pending => {
                let _ = vacant.insert(pending);
                None
            }
        }
    }

    /// Drops protocol-local ingress after route cancellation.
    pub fn retire(&mut self) {
        if let Some(pending) = self.protocol.occupied()
            && matches!(
                pending.get(),
                Protocol::PendingHead(_)
                    | Protocol::Line(_)
                    | Protocol::Prefix(_)
                    | Protocol::Reject(_)
            )
        {
            pending.clear();
        }
        self.recv = recv::Buffer::default();
        self.end_discard();
        self.framing.finish(&mut self.payload);
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
        let (roots, head) = match self.protocol.get() {
            None => (0, 0),
            Some(Protocol::Body(pending) | Protocol::Working(pending)) => {
                (1, head(pending) as usize)
            }
            Some(Protocol::Ready(pending)) => (1, ready(pending) as usize),
            Some(Protocol::PendingHead(value)) => (1, pending(value) as usize),
            Some(Protocol::Line(line)) => (1, line.len()),
            Some(Protocol::Prefix(prefix)) => (1, prefix.as_ref().len()),
            Some(Protocol::Reject(reject)) => (1, reject.len()),
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
    ) -> Result<Option<Frame<'frame, 'd>>, chunked::Decode> {
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

    pub(super) fn consume(
        &mut self,
        frame: session::Consumption,
        window: &mut crate::Input<'_>,
    ) -> Result<bool, Failure> {
        let used = match frame {
            session::Consumption::Buffered(used) => used,
            session::Consumption::Discard(discard) => discard.head,
        };
        window.advance(used).map_err(Failure::Input)?;
        let complete = match frame {
            session::Consumption::Buffered(_) => true,
            session::Consumption::Discard(discard) => {
                self.discard_remaining = discard.body - window.consume_max(discard.body);
                self.discard_remaining == 0
            }
        };
        self.finish_frame();
        Ok(complete)
    }

    pub fn finish_frame(&mut self) {
        self.protocol = Storage::new(());
        self.framing.finish(&mut self.payload);
    }
}
