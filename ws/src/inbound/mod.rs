//! Role-aware WebSocket ingress admission.
//!
//! Admission borrows ingress state until commit or mandatory-frame release.

use std::{cell, marker, num, ops};

use crate::{fragment, frame};

mod close;
mod fault;
mod valid;

pub use close::Close;
pub(crate) use close::Code;
pub use fault::Fault;
use valid::Kind;

pub enum ClientPeer {}
pub enum ServerPeer {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mask {
    Clear,
    Masked,
}

pub trait Peer {
    const MASK: Mask;

    /// Mask witness carried by every frame this role admits.
    type Witness: Copy;

    /// Admits the wire mask once, returning the witness later layers reuse.
    fn witness(mask: Option<[u8; 4]>) -> Result<Self::Witness, Fault>;
}

impl Peer for ClientPeer {
    const MASK: Mask = Mask::Masked;

    type Witness = [u8; 4];

    fn witness(mask: Option<[u8; 4]>) -> Result<[u8; 4], Fault> {
        mask.ok_or(Fault::Mask {
            expected: Self::MASK,
            actual: Mask::Clear,
        })
    }
}

impl Peer for ServerPeer {
    const MASK: Mask = Mask::Clear;

    type Witness = ();

    fn witness(mask: Option<[u8; 4]>) -> Result<(), Fault> {
        match mask {
            None => Ok(()),
            Some(_) => Err(Fault::Mask {
                expected: Self::MASK,
                actual: Mask::Masked,
            }),
        }
    }
}

pub struct Inbound<P> {
    fragments: fragment::Buffer,
    pending_end: Option<num::NonZero<usize>>,
    peer: marker::PhantomData<cell::Cell<P>>,
}

impl<P: Peer> Inbound<P> {
    pub fn new(max: usize) -> Self {
        Self {
            fragments: fragment::Buffer::new(max),
            pending_end: None,
            peer: marker::PhantomData,
        }
    }

    /// Closes the admitted frame's pending window.
    ///
    /// Every commit path funnels through this single step, so the next frame
    /// derives its own bound instead of inheriting the committed one.
    fn settle(&mut self) {
        Pending::<P>::new(&mut self.pending_end).settle();
    }

    /// Exact complete frame length after its variable-width header is known.
    pub const fn required(&self) -> Option<usize> {
        match self.pending_end {
            Some(end) => Some(end.get()),
            None => None,
        }
    }

    /// Admits one peer-role frame with bounded pending-payload work.
    pub fn decode<'state>(
        &'state mut self,
        bytes: &[u8],
        max: usize,
    ) -> Result<Decode<'state, P>, Fault> {
        if let Some(end) = self.pending_end
            && bytes.len() < end.get()
        {
            return Ok(Decode::NeedMore);
        }

        let Some(head) = frame::Head::parse(bytes, 0, usize::MAX)? else {
            if let Some(end) = self.pending_end {
                return Err(Fault::Changed {
                    expected: end.get(),
                    actual: bytes.len(),
                });
            }
            return Ok(Decode::NeedMore);
        };

        let (kind, mask) = valid::check::<P>(&self.fragments, &head)?;
        if head.payload_len() > max {
            return Err(Fault::Frame(frame::Error::Payload {
                actual: head.payload_len(),
                maximum: max,
            }));
        }

        if let Some(end) = self.pending_end
            && end != head.payload_end()
        {
            return Err(Fault::Changed {
                expected: end.get(),
                actual: head.payload_end().get(),
            });
        }

        self.pending_end = Some(head.payload_end());

        if bytes.len() < head.payload_end().get() {
            return Ok(Decode::NeedMore);
        }

        Ok(Decode::Ready(Admission {
            inbound: self,
            head,
            kind,
            mask,
        }))
    }
}

pub enum Decode<'state, P: Peer> {
    NeedMore,
    Ready(Admission<'state, P>),
}

#[must_use = "an admitted frame must be finished with its matching payload"]
/// Admission is linearly tied to its fragmentation state.
///
pub struct Admission<'state, P: Peer> {
    inbound: &'state mut Inbound<P>,
    head: frame::Head,
    kind: Kind,
    mask: P::Witness,
}

impl<'state, P: Peer> Admission<'state, P> {
    pub fn frame_len(&self) -> num::NonZero<usize> {
        self.head.payload_end()
    }

    pub fn range(&self) -> ops::Range<usize> {
        self.head.start().get()..self.head.payload_end().get()
    }

    pub fn payload_len(&self) -> usize {
        self.head.payload_len()
    }

    /// Validates the payload without committing fragmentation or frame progress.
    /// Mandatory control frames are returned as [`Prepared::Required`] and can
    /// only commit through their response-delivery barrier.
    pub fn prepare<'payload>(
        self,
        data: &'payload [u8],
    ) -> Result<Prepared<'state, 'payload, P>, Fault> {
        if data.len() != self.head.payload_len() {
            return Err(Fault::Payload {
                expected: self.head.payload_len(),
                actual: data.len(),
            });
        }

        Ok(match self.kind {
            Kind::Data(proof) => {
                let proof = self.inbound.fragments.prepare(proof, data)?;
                let pending = Pending::new(&mut self.inbound.pending_end);
                match proof {
                    fragment::Prepared::Message(complete) => {
                        Prepared::Message(Message { pending, complete })
                    }
                    fragment::Prepared::Fragment(partial) => {
                        Prepared::Fragment(Fragment { pending, partial })
                    }
                }
            }
            Kind::Close => Prepared::Required(Required {
                inbound: self.inbound,
                kind: Event::Close(valid::decode(data)?),
            }),
            Kind::Ping => Prepared::Required(Required {
                inbound: self.inbound,
                kind: Event::Ping(data),
            }),
            Kind::Pong => Prepared::Pong(Pong {
                pending: Pending::new(&mut self.inbound.pending_end),
                payload: data,
            }),
        })
    }
}

impl Admission<'_, ClientPeer> {
    pub fn mask(&self) -> [u8; 4] {
        self.mask
    }
}

/// A fully validated frame awaiting its causal commit edge.
pub enum Prepared<'state, 'payload, P> {
    Message(Message<'state, 'payload, P>),
    Fragment(Fragment<'state, 'payload, P>),
    Pong(Pong<'state, 'payload, P>),
    Required(Required<'state, 'payload, P>),
}

/// A validated complete message awaiting output admission.
pub struct Message<'state, 'payload, P> {
    pending: Pending<'state, P>,
    complete: fragment::Ready<'state, 'payload>,
}

impl<'payload, P: Peer> Message<'_, 'payload, P> {
    /// The exact byte length of the message this frame completes.
    pub(crate) const fn len(&self) -> usize {
        self.complete.len()
    }

    /// Commits the message its own admission already bounded.
    pub fn commit(self) -> fragment::Message<'payload> {
        let message = self.complete.commit();
        self.pending.settle();
        message
    }
}

#[derive(Clone, Copy)]
pub struct Reply<'payload> {
    opcode: u8,
    payload: &'payload [u8],
}

pub(crate) struct Request<'reply, 'payload> {
    reply: &'reply Reply<'payload>,
}

/// Proof tied to the exact mandatory reply request supplied by ingress.
pub(crate) struct Receipt<'reply, 'payload> {
    reply: marker::PhantomData<&'reply Reply<'payload>>,
}

impl<'payload> Reply<'payload> {
    /// Rebuilds the exact reply an admitted mandatory control frame owes.
    pub(crate) const fn owed(opcode: u8, payload: &'payload [u8]) -> Self {
        Self { opcode, payload }
    }

    pub const fn opcode(self) -> u8 {
        self.opcode
    }

    pub const fn payload(self) -> &'payload [u8] {
        self.payload
    }
}

impl<'reply, 'payload> Request<'reply, 'payload> {
    pub(crate) const fn opcode(&self) -> u8 {
        self.reply.opcode
    }

    pub(crate) const fn payload(&self) -> &'payload [u8] {
        self.reply.payload
    }

    pub(crate) const fn admit(self) -> Receipt<'reply, 'payload> {
        Receipt {
            reply: marker::PhantomData,
        }
    }
}

pub(crate) struct Pending<'state, P> {
    end: &'state mut Option<num::NonZero<usize>>,
    peer: marker::PhantomData<cell::Cell<P>>,
}

impl<'state, P> Pending<'state, P> {
    fn new(end: &'state mut Option<num::NonZero<usize>>) -> Self {
        Self {
            end,
            peer: marker::PhantomData,
        }
    }

    fn settle(self) {
        *self.end = None;
    }
}

pub struct Fragment<'state, 'payload, P> {
    pending: Pending<'state, P>,
    partial: fragment::Pending<'state, 'payload>,
}

impl<P: Peer> Fragment<'_, '_, P> {
    pub fn commit(self) {
        self.partial.commit();
        self.pending.settle();
    }
}

pub struct Pong<'state, 'payload, P> {
    pending: Pending<'state, P>,
    payload: &'payload [u8],
}

impl<'payload, P: Peer> Pong<'_, 'payload, P> {
    pub fn commit(self) -> &'payload [u8] {
        self.pending.settle();
        self.payload
    }
}

pub struct Required<'state, 'payload, P> {
    inbound: &'state mut Inbound<P>,
    kind: Event<'payload>,
}

pub struct Deferred<'payload> {
    pub reply: Reply<'payload>,
    pub event: Event<'payload>,
}

/// A mandatory control payload; [`Required`] retains its pending commit authority.
pub enum Event<'payload> {
    Close(Close<'payload>),
    Ping(&'payload [u8]),
}

impl<'payload, P: Peer> Required<'_, 'payload, P> {
    pub(crate) fn commit_with<E>(
        self,
        send: impl for<'reply> FnOnce(Request<'reply, 'payload>) -> Result<Receipt<'reply, 'payload>, E>,
    ) -> Result<Event<'payload>, E> {
        let reply = self.reply();
        send(Request { reply: &reply })?;
        self.inbound.settle();
        Ok(self.kind)
    }

    pub(crate) fn reply(&self) -> Reply<'payload> {
        match &self.kind {
            Event::Close(close) => Reply {
                opcode: 0x8,
                payload: close.payload(),
            },
            Event::Ping(data) => Reply {
                opcode: 0xa,
                payload: data,
            },
        }
    }

    /// Releases a complete mandatory frame for causal delivery.
    pub fn defer(self) -> Deferred<'payload> {
        let reply = self.reply();
        self.inbound.settle();
        Deferred {
            reply,
            event: self.kind,
        }
    }
}
