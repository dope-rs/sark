use o3::{
    alloc,
    buffer::{pool, storage},
};
use sark_core::{
    life::session,
    manifold::{
        client::{
            self, Connection,
            domain::{self, Plan},
            event::Reset,
            io,
        },
        schedule,
        stream::Boundary,
    },
};
use sark_h2::{conn, stream};

use crate::Reason;

mod buffer;
mod driver;

use buffer::Buffer;
pub(super) use driver::{Driver, Pending, Tag};
use state::calls;

use crate::{
    client::{call, config, state},
    details::RequestError,
    message,
};

pub(in crate::client) trait RequestBody: AsRef<[u8]> + Sized {
    fn try_send<C: client::Connection>(
        &self,
        conn: &mut C,
        key: C::Key,
        header: &[u8; message::HEADER_LEN],
        boundary: Boundary,
    ) -> Result<io::Outcome<C::Block>, C::Error>;
    fn try_advance(&mut self, total: usize, used: usize) -> Result<usize, RequestError>;
    fn into_pending(
        self,
        header: [u8; message::HEADER_LEN],
        total: usize,
        used: usize,
        boundary: Boundary,
    ) -> Result<driver::Pending, crate::Status>;
}

impl RequestBody for storage::Shared {
    fn try_send<C: client::Connection>(
        &self,
        conn: &mut C,
        key: C::Key,
        header: &[u8; message::HEADER_LEN],
        boundary: Boundary,
    ) -> Result<io::Outcome<C::Block>, C::Error> {
        conn.send_owned(key, header, self, 0, boundary)
    }

    fn try_advance(&mut self, total: usize, used: usize) -> Result<usize, RequestError> {
        driver::advance(0, total, used)
    }

    fn into_pending(
        self,
        header: [u8; message::HEADER_LEN],
        total: usize,
        used: usize,
        boundary: Boundary,
    ) -> Result<driver::Pending, crate::Status> {
        Ok(driver::Pending::message(
            header, self, total, used, boundary,
        ))
    }
}

pub(in crate::client) struct Output {
    request_pool: pool::Pool,
    max_message_len: usize,
}

impl Output {
    pub(in crate::client) fn try_new(limits: config::Limits) -> Result<Self, alloc::Error> {
        Ok(Self {
            request_pool: pool::Pool::try_from(limits.layout())?,
            max_message_len: limits.bytes(),
        })
    }

    /// Admits and initializes one actual request before opening its stream.
    pub(in crate::client) fn reserve<C: crate::Codec>(
        &self,
        codec: &mut C,
        value: &C::Encode,
    ) -> call::Admission<storage::Shared> {
        let Some(lease) = self.request_pool.acquire() else {
            return call::Admission::Pending;
        };
        match codec.encode(value, lease) {
            Ok(payload) => call::Admission::Ready(payload),
            Err(status) => call::Admission::Rejected(status),
        }
    }

    pub(in crate::client) fn available(&self) -> usize {
        self.request_pool.available()
    }

    pub(in crate::client) fn finish<P, C>(
        &mut self,
        calls: &mut calls::Store<P, C>,
        conn: &mut P::Connection,
        stream: P::Key,
        retire: &mut impl FnMut(calls::Record<C>),
    ) -> Result<(), crate::Status>
    where
        C: session::Storage,
        P: Plan<C, Key = stream::Key, Connection: Connection<Error = conn::Error>>,
    {
        let result = calls.transaction(
            stream,
            |record, space| {
                if matches!(record.boundary, Boundary::End) {
                    return Ok((domain::Activation::Keep, driver::Commit::Idle));
                }
                let space = match space.tail(&record.requests) {
                    calls::Tail::Empty(space) => space,
                    calls::Tail::Pending(tail) => {
                        return Ok((
                            domain::Activation::Data(None),
                            driver::Commit::Close { tail },
                        ));
                    }
                };

                match conn.send(stream, &[], &[], 0, Boundary::End) {
                    Ok(io::Outcome::Written(used)) => {
                        driver::advance(0, 0, used)?;
                        Ok((
                            domain::Activation::Keep,
                            driver::Commit::Direct {
                                boundary: Boundary::End,
                            },
                        ))
                    }
                    Ok(io::Outcome::Blocked(block)) => {
                        let permit = space.try_permit()?;
                        let item = driver::Pending::End;
                        Ok((
                            domain::Activation::Data(Some(block)),
                            driver::Commit::queued(
                                permit,
                                item,
                                Boundary::End,
                                driver::Drive::Idle,
                            ),
                        ))
                    }
                    Err(error) => Err(crate::Status::from(error)),
                }
            },
            |record, commit| commit.apply(record),
        );
        let drive = driver::Commit::resolve(result)?;
        if let driver::Drive::Ready = drive {
            calls.drive(conn, schedule::Egress::Unchanged, retire)?;
        }
        calls.reclaim(stream)
    }

    pub(in crate::client) fn send_bytes<P, C>(
        &self,
        calls: &mut calls::Store<P, C>,
        conn: &mut P::Connection,
        stream: P::Key,
        body: &[u8],
        boundary: Boundary,
        retire: &mut impl FnMut(calls::Record<C>),
    ) -> Result<(), crate::Status>
    where
        C: session::Storage,
        P: Plan<C, Key = stream::Key, Connection: Connection<Error = conn::Error>>,
    {
        self.send(
            calls,
            conn,
            stream,
            Buffer::new(body, &self.request_pool),
            boundary,
            retire,
        )
    }

    pub(in crate::client) fn send<P, C, B>(
        &self,
        calls: &mut calls::Store<P, C>,
        conn: &mut P::Connection,
        stream: P::Key,
        mut body: B,
        boundary: Boundary,
        retire: &mut impl FnMut(calls::Record<C>),
    ) -> Result<(), crate::Status>
    where
        B: RequestBody,
        C: session::Storage,
        P: Plan<C, Key = stream::Key, Connection: Connection<Error = conn::Error>>,
    {
        let len = body.as_ref().len();
        if len > self.max_message_len {
            return Err(Self::exhausted(Reason::MessageLarge));
        }
        let header = message::Frame::header(message::Compression::Plain, len as u32);
        let total = header.len() + len;
        let force = calls.has_work();
        let result = calls.transaction(
            stream,
            |record, space| {
                if matches!(record.boundary, Boundary::End) {
                    return Err(crate::Status::new(
                        crate::Code::Internal,
                        Reason::RequestClosed,
                    ));
                }
                let (used, wait, drive) = if !record.requests.is_empty() || force {
                    (0, None, driver::Drive::Ready)
                } else {
                    let (used, wait) = match body.try_send(conn, stream, &header, boundary) {
                        Ok(io::Outcome::Written(used)) if used == total => {
                            return Ok((
                                domain::Activation::Keep,
                                driver::Commit::Direct { boundary },
                            ));
                        }
                        Ok(io::Outcome::Written(used)) => (body.try_advance(total, used)?, None),
                        Ok(io::Outcome::Blocked(block)) => (0, Some(block)),
                        Err(error) => return Err(crate::Status::from(error)),
                    };
                    (used, wait, driver::Drive::Idle)
                };

                let permit = space.try_permit()?;
                let item = body.into_pending(header, total, used, boundary)?;
                Ok((
                    domain::Activation::Data(wait),
                    driver::Commit::queued(permit, item, boundary, drive),
                ))
            },
            |record, commit| commit.apply(record),
        );
        let drive = driver::Commit::resolve(result)?;
        if let driver::Drive::Ready = drive {
            calls.drive(conn, schedule::Egress::Unchanged, retire)?;
        }
        calls.reclaim(stream)
    }

    pub(in crate::client) fn cancel<P, C>(
        &mut self,
        calls: &mut calls::Store<P, C>,
        conn: &mut P::Connection,
        stream: P::Key,
    ) -> Option<calls::Record<C>>
    where
        C: session::Storage,
        P: Plan<C, Connection: Connection<Error = conn::Error>>,
    {
        calls.get(stream)?;
        conn.cancel(stream, Reset::Cancelled);
        calls.remove(stream)
    }

    fn exhausted(reason: crate::Reason) -> crate::Status {
        crate::Status::new(crate::Code::ResourceExhausted, reason)
    }
}
