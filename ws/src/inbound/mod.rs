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
                Prepared::Ready(Ready {
                    pending: Pending::new(&mut self.inbound.pending_end),
                    kind: ReadyKind::Data(proof),
                })
            }
            Kind::Close => Prepared::Required(Required {
                inbound: self.inbound,
                kind: Event::Close(valid::decode(data)?),
            }),
            Kind::Ping => Prepared::Required(Required {
                inbound: self.inbound,
                kind: Event::Ping(data),
            }),
            Kind::Pong => Prepared::Ready(Ready {
                pending: Pending::new(&mut self.inbound.pending_end),
                kind: ReadyKind::Pong(data),
            }),
        })
    }
}

impl Admission<'_, ClientPeer> {
    pub fn mask(&self) -> [u8; 4] {
        self.mask
    }
}

pub enum ReadyEvent<'payload> {
    Data(fragment::Push<'payload>),
    Pong(&'payload [u8]),
}

/// A fully validated frame awaiting its causal commit edge.
pub enum Prepared<'state, 'payload, P> {
    Ready(Ready<'state, 'payload, P>),
    Required(Required<'state, 'payload, P>),
}

impl<'state, 'payload, P> Prepared<'state, 'payload, P> {
    /// Splits this prepared frame exactly once into the causal path it owns: the
    /// admission that completed a message carries the exact bounded length into
    /// [`Message`], and neither path re-derives completeness.
    pub(crate) fn split(self) -> Split<'state, 'payload, P> {
        match self {
            Self::Ready(Ready {
                pending,
                kind: ReadyKind::Data(proof),
            }) => match proof {
                fragment::Prepared::Message(complete) => {
                    Split::Message(Message { pending, complete })
                }
                fragment::Prepared::Fragment(partial) => {
                    Split::Control(Control::Fragment(pending, partial))
                }
            },
            Self::Ready(Ready {
                pending,
                kind: ReadyKind::Pong(_),
            }) => Split::Control(Control::Pong(pending)),
            Self::Required(need) => Split::Control(Control::Required(need)),
        }
    }
}

/// One prepared frame split into the single causal path it can commit.
pub(crate) enum Split<'state, 'payload, P> {
    Message(Message<'state, 'payload, P>),
    Control(Control<'state, 'payload, P>),
}

/// A data frame that completes exactly one message of [`Message::len`] bytes.
pub(crate) struct Message<'state, 'payload, P> {
    pending: Pending<'state, P>,
    complete: fragment::Ready<'state, 'payload>,
}

impl<'payload, P: Peer> Message<'_, 'payload, P> {
    /// The exact byte length of the message this frame completes.
    pub(crate) const fn len(&self) -> usize {
        self.complete.len()
    }

    /// Commits the message its own admission already bounded.
    pub(crate) fn commit(self) -> fragment::Message<'payload> {
        let message = self.complete.commit();
        self.pending.settle();
        message
    }
}

/// Every prepared frame that is not a complete data message.
pub(crate) enum Control<'state, 'payload, P> {
    /// A data fragment that awaits more input.
    Fragment(Pending<'state, P>, fragment::Pending<'state, 'payload>),
    /// A pong ready to commit; its payload stays owned by the inbound state.
    Pong(Pending<'state, P>),
    /// A mandatory close or ping that commits only with its staged reply.
    Required(Required<'state, 'payload, P>),
}

impl<'payload, P: Peer> Control<'_, 'payload, P> {
    /// Commits a mandatory frame only with proof minted by the live response
    /// sink that staged its exact reply.
    pub(crate) fn commit_with<E>(
        self,
        send: impl for<'reply> FnOnce(Request<'reply, 'payload>) -> Result<Receipt<'reply, 'payload>, E>,
    ) -> Result<ops::ControlFlow<()>, E> {
        let pending = match self {
            Self::Fragment(pending, partial) => {
                partial.commit();
                pending
            }
            Self::Pong(pending) => pending,
            Self::Required(need) => {
                let reply = need.reply();
                send(Request { reply: &reply })?;
                let event = need.kind;
                need.inbound.settle();
                return Ok(match event {
                    Event::Close(_) => ops::ControlFlow::Break(()),
                    Event::Ping(_) => ops::ControlFlow::Continue(()),
                });
            }
        };
        pending.settle();
        Ok(ops::ControlFlow::Continue(()))
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

pub struct Ready<'state, 'payload, P> {
    pending: Pending<'state, P>,
    kind: ReadyKind<'state, 'payload>,
}

impl<'payload, P: Peer> Ready<'_, 'payload, P> {
    pub fn commit(self) -> ReadyEvent<'payload> {
        let event = match self.kind {
            ReadyKind::Data(proof) => ReadyEvent::Data(proof.commit()),
            ReadyKind::Pong(data) => ReadyEvent::Pong(data),
        };
        self.pending.settle();
        event
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

enum ReadyKind<'state, 'payload> {
    Data(fragment::Prepared<'state, 'payload>),
    Pong(&'payload [u8]),
}
