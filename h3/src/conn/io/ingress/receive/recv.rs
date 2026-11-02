use std::ops;

use dope_quic::wire::{stream::Id, varint};
use o3::buffer::{resident, segments};
use sark_core::manifold::stream::Boundary;
use sark_http::payload::response::Body as _;

use crate::{
    conn::{
        self, event,
        io::ingress::{
            receive::buf::{self, Buf},
            wait,
        },
    },
    frame,
};

pub(in crate::conn) struct Recv<'d> {
    pub(in crate::conn) buf: Buf<'d>,
    pub(in crate::conn) decoder: frame::Decoder,
    pub(in crate::conn) token: event::Token,
    pub(in crate::conn) boundary: Boundary,
    credit: conn::CreditMode,
    pub(in crate::conn) wait: Option<wait::Member>,
    pub(in crate::conn) wait_lane: wait::Lane,
}

const _: () = assert!(size_of::<Recv<'static>>() == 7 * size_of::<usize>());

impl<'d> Recv<'d> {
    pub(in crate::conn) fn try_new(
        pool: &segments::Pool<resident::Shared<'d>>,
        token: event::Token,
    ) -> Result<Self, conn::Error> {
        Ok(Self {
            buf: Buf::try_new(pool)?,
            decoder: frame::Decoder::default(),
            token,
            boundary: Boundary::More,
            credit: conn::CreditMode::Open,
            wait: None,
            wait_lane: wait::Lane::Ready,
        })
    }

    pub(in crate::conn) fn merge(&mut self, boundary: Boundary) {
        if matches!(boundary, Boundary::End) {
            self.boundary = Boundary::End;
        }
    }

    pub(in crate::conn) fn verify(
        &self,
        supplied: Option<event::Token>,
    ) -> Result<(), conn::Error> {
        if let Some(actual) = supplied.filter(|token| *token != self.token) {
            return Err(conn::Error::TokenMismatch {
                expected: self.token.raw(),
                actual: actual.raw(),
            });
        }
        Ok(())
    }

    pub(in crate::conn) const fn has_credit(&self) -> bool {
        matches!(self.credit, conn::CreditMode::Held)
    }

    pub(in crate::conn) const fn credit(&self) -> conn::CreditMode {
        self.credit
    }

    pub(in crate::conn) fn decode_frame(
        &self,
        stream: Id,
        limit: usize,
    ) -> Result<Option<frame::Read<'_, buf::Chunks<'_, 'd>>>, conn::Error> {
        let cause = match self.decoder.decode(&self.buf, limit) {
            Ok(frame) if !frame.next.is_pending() || matches!(self.boundary, Boundary::More) => {
                return Ok(Some(frame));
            }
            Ok(_) => frame::ParseError::NeedMore,
            Err(
                cause @ (frame::ParseError::NeedMore
                | frame::ParseError::VarInt(varint::Error::Underflow { .. })),
            ) => cause,
            Err(error) => return Err(conn::Error::Parse(error)),
        };
        match self.boundary {
            Boundary::End => Err(conn::Error::Incomplete {
                stream_id: stream.get(),
                buffered: crate::Source::len(&self.buf),
                cause,
            }),
            Boundary::More => Ok(None),
        }
    }

    pub(in crate::conn) fn append(&mut self, source: &[u8]) -> Result<(), conn::Error> {
        self.check_hold(source.is_empty())?;
        self.buf.append(source)
    }

    pub(in crate::conn) fn append_owned(&mut self, source: Vec<u8>) -> Result<(), conn::Error> {
        self.check_hold(source.is_empty())?;
        self.buf.append_owned(source)
    }

    pub(in crate::conn) fn append_pool(
        &mut self,
        source: resident::Shared<'d>,
    ) -> Result<(), conn::Error> {
        self.check_hold(source.is_empty())?;
        self.buf.push(source)
    }

    pub(in crate::conn) fn append_held(
        &mut self,
        source: resident::Shared<'d>,
    ) -> Result<(), conn::Error> {
        if source.is_empty() {
            return Ok(());
        }
        if !self.buf.is_empty() && self.credit != conn::CreditMode::Held {
            return Err(conn::Error::ReceiveState {
                buffered: crate::Source::len(&self.buf),
                held: self.credit,
            });
        }
        self.buf.push(source)?;
        self.credit = conn::CreditMode::Held;
        Ok(())
    }

    pub(in crate::conn) fn consume(
        &mut self,
        amount: usize,
        released: &mut usize,
        budget: &mut conn::ByteBudget,
    ) -> Result<(), conn::Error> {
        let credit = self.try_advance(amount)?;
        budget.release(amount)?;
        *released = conn::try_add_credit(*released, credit)?;
        Ok(())
    }

    /// Releases the admitted frame header while queued DATA retains its payload charge.
    pub(in crate::conn) fn take_payload(
        &mut self,
        range: ops::Range<usize>,
        len: usize,
        mode: conn::CreditMode,
        budget: &mut conn::ByteBudget,
    ) -> Result<(crate::Payload<'d>, usize), conn::Error> {
        let payload = self.buf.take_payload(range, len)?;
        let header = len - payload.len();
        budget.release(header)?;
        let release = match (self.credit, mode) {
            (conn::CreditMode::Open, _) => 0,
            (conn::CreditMode::Held, conn::CreditMode::Open) => len,
            (conn::CreditMode::Held, conn::CreditMode::Held) => header,
        };
        if self.buf.is_empty() {
            self.credit = conn::CreditMode::Open;
        }
        Ok((payload, release))
    }

    pub(in crate::conn) fn clear(
        &mut self,
        budget: &mut conn::ByteBudget,
    ) -> Result<usize, conn::Error> {
        let len = crate::Source::len(&self.buf);
        budget.release(len)?;
        let release = match self.credit {
            conn::CreditMode::Open => 0,
            conn::CreditMode::Held => len,
        };
        self.buf.clear();
        self.credit = conn::CreditMode::Open;
        Ok(release)
    }

    fn try_advance(&mut self, amount: usize) -> Result<usize, conn::Error> {
        self.buf.try_advance(amount).map_err(conn::Error::Consume)?;
        let release = match self.credit {
            conn::CreditMode::Open => 0,
            conn::CreditMode::Held => amount,
        };
        if self.buf.is_empty() {
            self.credit = conn::CreditMode::Open;
        }
        Ok(release)
    }

    fn check_hold(&self, empty: bool) -> Result<(), conn::Error> {
        if !empty && self.credit == conn::CreditMode::Held {
            return Err(conn::Error::ReceiveState {
                buffered: crate::Source::len(&self.buf),
                held: self.credit,
            });
        }
        Ok(())
    }
}
