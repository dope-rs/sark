use std::{cell, num, pin, task};

use dope::{
    core::driver::route::Identity,
    manifold::client::{conn, port},
    net::link::egress::{self, metadata::batch},
    runtime::random::{self, Entropy},
};
use dope_fiber::{abi, context};
use o3::{base::cell::region, buffer::storage};

use crate::{
    client::{self, channel},
    frame, inbound,
};

pub(super) const MAX_FRAME_OVERHEAD: usize = 14;

#[derive(Default)]
pub(super) struct Sequence {
    stream: cell::Cell<Option<Stream>>,
}

const MASK_DOMAIN: random::Domain = random::Namespace::new(*b"sark\0ws\0").fixed();
const MASK_LANE: u64 = u64::from_be_bytes(*b"mask key");

struct Tag;

impl random::Purpose for Tag {
    const DOMAIN: random::Domain = MASK_DOMAIN;
}

/// One OS-seeded keyed stream issuing a fresh masking key per frame. The
/// process secret is never derived from the wire, so consecutive keys stay
/// unpredictable to the peer.
struct Stream {
    root: random::Process<Tag>,
    cursor: u64,
}

impl Stream {
    fn try_new() -> Result<Self, client::Error> {
        Ok(Self {
            root: random::Process::<Tag>::random().map_err(client::Error::Entropy)?,
            cursor: 0,
        })
    }

    fn advance(&mut self) -> [u8; 4] {
        let cursor = self.cursor;
        self.cursor = cursor.wrapping_add(1);
        let [first, second, third, fourth, ..] = self.root.word::<MASK_LANE>(cursor).into_bytes();
        [first, second, third, fourth]
    }
}

#[derive(Clone, Copy)]
pub(super) struct Plan<'a> {
    frames: frame::Frames<'a>,
    batch: batch::Plan,
}

impl<'a> Plan<'a> {
    pub(super) fn close(close: inbound::Close<'a>) -> Result<Self, client::Error> {
        Self::one(0x8, close.payload())
    }

    pub(super) fn reply(reply: inbound::Reply<'a>) -> Result<Self, client::Error> {
        Self::one(reply.opcode(), reply.payload())
    }

    fn new(frames: frame::Frames<'a>) -> Result<Self, client::Error> {
        let (count, wire, tail) = frames.lengths();
        let full = count.get() - 1;
        let bytes = wire
            .checked_mul(full)
            .and_then(|bytes| bytes.checked_add(tail))
            .ok_or(client::Error::Resident {
                entries: full,
                frame: wire,
                tail,
            })?;
        Ok(Self {
            frames,
            batch: batch::Plan::new(
                egress::Entries::new(u32::try_from(count.get()).map_err(|cause| {
                    client::Error::Entries {
                        actual: count.get(),
                        cause,
                    }
                })?),
                egress::Resident::try_from(bytes).map_err(client::Error::Retention)?,
            )?,
        })
    }

    fn one(opcode: u8, data: &'a [u8]) -> Result<Self, client::Error> {
        Self::new(frame::Frames::new(
            opcode,
            data,
            num::NonZero::<usize>::MAX,
        )?)
    }

    pub(super) const fn batch(self) -> batch::Plan {
        self.batch
    }

    fn is_close(self) -> bool {
        self.frames.opcode() == 0x8
    }

    fn admits(self, state: channel::Connection) -> Result<(), client::Error> {
        if state == channel::Connection::Closing && self.frames.opcode() <= 0x8 {
            Err(client::Error::Closing)
        } else {
            Ok(())
        }
    }

    pub(super) fn encode(
        self,
        masks: &Sequence,
        mut push: impl FnMut(egress::NonEmpty<storage::Shared>) -> Result<(), client::Error>,
    ) -> Result<(), client::Error> {
        let mut stream = match masks.stream.take() {
            Some(stream) => stream,
            None => Stream::try_new()?,
        };
        let result = (move |stream: &mut Stream| {
            for frame in self.frames {
                let mask = stream.advance();
                let frame = egress::NonEmpty::try_new(frame.bind(mask).try_owned()?)
                    .map_err(client::Error::Load)?;
                push(frame)?;
            }
            Ok(())
        })(&mut stream);
        masks.stream.set(Some(stream));
        result
    }
}

impl<'e, 'd, ID: Identity> channel::Port<'e, 'd, ID> {
    pub(super) fn send<'b>(
        &'b self,
        opcode: u8,
        data: &'b [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), client::Error>> + 'b
    where
        'd: 'b,
    {
        abi::poll_fn(move |mut cx: pin::Pin<&mut context::Context<'_, 'd>>| {
            task::Poll::Ready(
                self.try_plan(opcode, data)
                    .and_then(|plan| self.try_commit(cx.as_mut().region_token(), plan)),
            )
        })
    }

    pub(super) fn close<'b>(
        &'b self,
        close: inbound::Close<'b>,
    ) -> impl abi::Fiber<'d, Output = Result<(), client::Error>> + 'b
    where
        'd: 'b,
    {
        abi::poll_fn(move |mut cx: pin::Pin<&mut context::Context<'_, 'd>>| {
            task::Poll::Ready(
                Plan::close(close)
                    .and_then(|plan| self.try_commit(cx.as_mut().region_token(), plan)),
            )
        })
    }

    pub(super) fn try_plan<'a>(
        &self,
        opcode: u8,
        data: &'a [u8],
    ) -> Result<Plan<'a>, client::Error> {
        if data.len() > self.codec.config.max_message_size.get() {
            return Err(client::Error::Payload {
                actual: data.len(),
                maximum: self.codec.config.max_message_size.get(),
            });
        }
        let frames =
            frame::Frames::new(opcode, data, self.codec.config.max_outbound_frame_payload)?;
        Plan::new(frames)
    }

    pub(super) fn try_commit(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        plan: Plan<'_>,
    ) -> Result<(), client::Error> {
        use port::batch;

        let shared = &self.shared;
        let (conn, state) = shared
            .conn_id
            .current()
            .ok_or_else(|| shared.unavailable())?;
        plan.admits(state)?;
        let Some(result) = self.io.with_sender(conn, |sender| {
            match sender.try_batch(region, plan.batch(), |mut sink| {
                plan.encode(&shared.rng, |frame| {
                    sink.try_push(frame).map_err(client::Error::Rejected)
                })
            }) {
                Ok(()) => Ok(()),
                Err(batch::Error::Inactive) => Err(client::Error::Inactive),
                Err(batch::Error::Admission(cause)) => Err(client::Error::Admission(cause)),
                Err(batch::Error::Build(error)) => Err(error),
                Err(batch::Error::Mismatch { remaining: credit }) => {
                    Err(client::Error::Incomplete(credit))
                }
            }
        }) else {
            return Err(shared.unavailable());
        };
        if matches!(result, Err(client::Error::Inactive)) {
            shared.conn_id.clear(conn);
            shared.wake();
        }
        if result.is_ok() && plan.is_close() {
            shared.close(conn);
        }
        result
    }

    pub(super) fn try_final<const IOV: usize>(
        &self,
        conn: conn::Id<'d, ID>,
        sink: &egress::Queue<'_, '_, 'd, IOV>,
        region: &mut region::Token<region::Identity<'d>>,
        plan: Plan<'_>,
    ) -> Result<(), client::Error> {
        if let Some((current, state)) = self.shared.conn_id.current()
            && current == conn
        {
            plan.admits(state)?;
        }
        match sink.try_batch(region, plan.batch(), |mut sink| {
            plan.encode(&self.shared.rng, |frame| {
                sink.try_push(frame).map_err(client::Error::Rejected)
            })
        }) {
            Ok(()) => {
                if plan.is_close() {
                    self.shared.close(conn);
                }
                Ok(())
            }
            Err(batch::Error::Admission(cause)) => Err(client::Error::Admission(cause)),
            Err(batch::Error::Build(error)) => Err(error),
            Err(batch::Error::Mismatch { remaining: credit }) => {
                Err(client::Error::Incomplete(credit))
            }
        }
    }
}
