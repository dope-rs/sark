use dynamic::{hash, mode, plan};
use o3::{
    buffer::{bytes::window::reserve, pool, write::consume},
    depot::{capacity::Count, queue::group::capacity},
};
use sark_core::life::session;
use sark_http::payload::compress::dynamic;

use crate::conn::{self, build, config};

pub(crate) mod headers;
pub(crate) mod limits;
pub(crate) mod modes;
mod owned;
pub(crate) mod payloads;
pub(crate) mod retained;
pub(crate) mod transfer;
mod wire;
use crate::{
    Endpoint, flow,
    frame::{controls, layout, numbers, payload},
    stream,
};

pub(crate) enum Full {
    Append(consume::Error),
    Barrier,
    Payloads {
        used: usize,
        limit: usize,
    },
    Bytes {
        used: usize,
        incoming: usize,
        limit: usize,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::conn) enum Barrier {
    Open,
    Blocked,
}

const _: () = {
    assert!(core::mem::size_of::<Barrier>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Barrier>() == core::mem::align_of::<bool>());
};

pub(in crate::conn) trait Reset {
    fn write(&mut self, stream: stream::Key, error: numbers::ErrorCode) -> Result<(), Full>;

    fn set_barrier(&mut self, barrier: Barrier);
}

pub(crate) struct Egress<C: session::Storage> {
    pub(crate) outbound: Outbound<C>,
    pub(crate) headers: headers::Encoder,
}

pub(crate) struct Outbound<C: session::Storage> {
    pub(in crate::conn) bytes: reserve::Ring<{ limits::Capacity::TERMINAL_SLACK }>,
    pub(in crate::conn) payloads: payloads::Ledger<C>,
    pub(in crate::conn) head: payloads::Mark,
    terminal_barrier: Barrier,
}

const _: () = {
    use core::mem::size_of;

    use o3::{buffer::storage, depot::queue::group};

    assert!(
        size_of::<Outbound<storage::Shared>>()
            == size_of::<(
                reserve::Ring<{ limits::Capacity::TERMINAL_SLACK }>,
                group::Ledger<payloads::Shared<storage::Shared>, 1>,
                usize,
                usize,
                payloads::Mark,
                Barrier,
            )>()
    );
};

impl<C: session::Storage> Egress<C> {
    pub(crate) fn try_new<R: crate::Role>(
        initial: limits::Initial<R>,
        payloads: Count,
        table: plan::Plan<mode::Direct>,
        block: limits::Block,
        wire: pool::Layout,
        seed: hash::Seed,
    ) -> Result<Self, build::Egress> {
        let outbound = Outbound::try_new(initial, payloads).map_err(build::Egress::Outbound)?;
        Ok(Self {
            outbound,
            headers: headers::Encoder::try_new(table, block, wire, seed)
                .map_err(build::Egress::Headers)?,
        })
    }
}

impl<C: session::Storage> Outbound<C> {
    pub(crate) fn try_new<R: crate::Role>(
        initial: limits::Initial<R>,
        payloads: Count,
    ) -> Result<Self, build::Outbound> {
        let (capacity, settings, target) = initial.into_parts();
        let mut outbound = Self {
            bytes: reserve::Ring::try_new(capacity.storage()).map_err(build::Outbound::Buffer)?,
            payloads: payloads::Ledger::try_new(capacity::Count::new(payloads))
                .map_err(build::Outbound::Payloads)?,
            head: payloads::Mark::ZERO,
            terminal_barrier: Barrier::Open,
        };
        outbound
            .initialize::<R>(settings, target)
            .map_err(build::Outbound::Initial)?;
        Ok(outbound)
    }

    pub(crate) fn transfer(&mut self) -> transfer::Transfer<'_, C> {
        transfer::Transfer::new(self)
    }

    pub(crate) fn retained(&mut self) -> retained::Retained<'_, C> {
        retained::Retained::new(self)
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len() + self.payloads.bytes()
    }

    pub(in crate::conn) fn front_prefix(&self) -> usize {
        match self.payloads.front_prefix(self.head) {
            Some(prefix) => prefix,
            None => self.bytes.len(),
        }
    }

    pub(in crate::conn) fn room<M: modes::Mode>(&self) -> Option<usize> {
        if matches!(self.terminal_barrier, Barrier::Blocked) {
            return None;
        }
        if M::RETAINED && self.payloads.is_full() {
            return None;
        }
        self.limit().checked_sub(self.len())
    }

    fn full<M: modes::Mode>(&self, incoming: usize) -> Full {
        if matches!(self.terminal_barrier, Barrier::Blocked) {
            return Full::Barrier;
        }
        if M::RETAINED && self.payloads.is_full() {
            let (used, limit) = self.payloads.bounds();
            return Full::Payloads { used, limit };
        }
        Full::Bytes {
            used: self.len(),
            incoming,
            limit: self.limit(),
        }
    }

    pub(crate) fn check(&self) -> Result<(), Full> {
        let used = self.len();
        let limit = self.limit();
        if used > limit {
            Err(Full::Bytes {
                used,
                incoming: 0,
                limit,
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn try_append<'a, 'source, const N: usize>(
        &'a mut self,
        slices: [&'source [u8]; N],
    ) -> Result<reserve::Append<'a, 'source, { limits::Capacity::TERMINAL_SLACK }, N>, Full> {
        if matches!(self.terminal_barrier, Barrier::Blocked) {
            return Err(Full::Barrier);
        }
        self.append(slices, self.limit()).map_err(Full::Append)
    }

    fn append<'a, 'source, const N: usize>(
        &'a mut self,
        slices: [&'source [u8]; N],
        limit: usize,
    ) -> Result<reserve::Append<'a, 'source, { limits::Capacity::TERMINAL_SLACK }, N>, consume::Error>
    {
        consume::Error::check(self.payloads.bytes(), limit)?;
        reserve::Append::try_new(&mut self.bytes, slices, limit - self.payloads.bytes())
    }

    pub(in crate::conn) fn limit(&self) -> usize {
        self.bytes.capacity() - limits::Capacity::RESET_SLACK
    }

    pub(in crate::conn) fn set_barrier(&mut self, barrier: Barrier) {
        self.terminal_barrier = barrier;
    }

    fn initialize<R: crate::Role>(
        &mut self,
        settings: config::Wire,
        target: flow::Target,
    ) -> Result<(), consume::Error> {
        use layout::Header;
        use numbers::{Flags, Type};

        let header = Header {
            length: numbers::Length::from_wire(0, 0, settings.len() as u8),
            kind: Type::Settings,
            flags: Flags(0),
            stream_id: stream::Id::CONNECTION,
        }
        .wire_bytes();
        let window = target.increment().map(|step| {
            payload::WindowUpdate {
                stream_id: stream::Id::CONNECTION,
                increment: step,
            }
            .wire_bytes()
        });
        let preface: &[u8] = match R::ENDPOINT {
            Endpoint::Client => conn::CLIENT_PREFACE,
            Endpoint::Server => &[],
        };
        let window = match window.as_ref() {
            Some(window) => window.as_slice(),
            None => &[],
        };
        let slices = [preface, &header, settings.as_ref(), window];
        let limit = self.limit();
        reserve::Append::try_new(&mut self.bytes, slices, limit)?.commit();
        Ok(())
    }

    pub(crate) fn data_control(
        &mut self,
        stream: stream::Nonzero,
        conn_up: Option<numbers::Increment>,
        peer_up: Option<numbers::Increment>,
        reset: Option<numbers::ErrorCode>,
    ) -> Result<(), conn::Error> {
        let update = |id, step: Option<numbers::Increment>| {
            step.map(|step| {
                payload::WindowUpdate {
                    stream_id: id,
                    increment: step,
                }
                .wire_bytes()
            })
        };
        let global = update(stream::Id::CONNECTION, conn_up);
        let peer = update(stream.get(), peer_up);
        let bytes;
        let reset = match reset {
            Some(error) => {
                bytes = controls::RstStream::from_stream(stream, error).wire_bytes();
                bytes.as_slice()
            }
            None => &[],
        };
        let global = match global.as_ref() {
            Some(wire) => wire.as_slice(),
            None => &[],
        };
        let peer = match peer.as_ref() {
            Some(wire) => wire.as_slice(),
            None => &[],
        };
        let added = global.len() + peer.len() + reset.len();
        if added == 0 {
            return Ok(());
        }
        self.try_append([global, peer, reset])?.commit();
        Ok(())
    }

    pub(crate) fn reset(
        &mut self,
        id: stream::Id,
        error: numbers::ErrorCode,
    ) -> Result<(), conn::Error> {
        let wire = controls::RstStream::new(id, error)
            .ok_or(conn::Error::ParseError(numbers::ParseError::Stream {
                kind: numbers::Type::RstStream,
                actual: id,
            }))?
            .wire_bytes();
        self.append([&wire], self.bytes.capacity())
            .map_err(conn::Error::OutputAppend)?
            .commit();
        self.check()?;
        Ok(())
    }
}

impl<C: session::Storage> Reset for Outbound<C> {
    fn write(&mut self, stream: stream::Key, error: numbers::ErrorCode) -> Result<(), Full> {
        let wire = controls::RstStream::from_stream(stream.nonzero(), error).wire_bytes();
        self.append([&wire], self.bytes.capacity())
            .map_err(Full::Append)?
            .commit();
        Ok(())
    }

    fn set_barrier(&mut self, barrier: Barrier) {
        self.terminal_barrier = barrier;
    }
}
