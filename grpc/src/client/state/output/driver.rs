use o3::buffer::{storage, write::consume};
use sark_core::{
    life::session,
    manifold::{
        client::{self, domain, event::Reset, io},
        schedule,
        stream::Boundary,
    },
};
use sark_h2::conn;

use crate::{Reason, client::state::calls, details::RequestError, message};

pub(in crate::client) enum Tag {}

pub(in crate::client) enum Pending {
    Message {
        header: [u8; message::HEADER_LEN],
        body: storage::Shared,
        total: usize,
        pos: usize,
        boundary: Boundary,
    },
    End,
}

impl Pending {
    pub(super) const fn message(
        header: [u8; message::HEADER_LEN],
        body: storage::Shared,
        total: usize,
        pos: usize,
        boundary: Boundary,
    ) -> Self {
        Self::Message {
            header,
            body,
            total,
            pos,
            boundary,
        }
    }

    fn finish(&mut self) {
        match self {
            Self::Message { boundary, .. } => *boundary = Boundary::End,
            Self::End => {}
        }
    }

    fn turn<C>(
        &mut self,
        conn: &mut C,
        key: C::Key,
    ) -> Result<schedule::Turn<C::Block>, RequestError>
    where
        C: client::Connection<Error = conn::Error>,
    {
        let sent = match self {
            Self::Message {
                header,
                body,
                pos,
                boundary,
                ..
            } => {
                let (prefix, offset) = if *pos < header.len() {
                    (&header[*pos..], 0)
                } else {
                    (&[][..], *pos - header.len())
                };
                conn.send_owned(key, prefix, body, offset, *boundary)
                    .map_err(RequestError::Connection)?
            }
            Self::End => conn
                .send(key, &[], &[], 0, Boundary::End)
                .map_err(RequestError::Connection)?,
        };
        match (sent, self) {
            (io::Outcome::Written(n), Self::End) => {
                advance(0, 0, n)?;
                Ok(schedule::Turn::Complete)
            }
            (io::Outcome::Written(n), Self::Message { total, pos, .. }) => {
                *pos = advance(*pos, *total, n)?;
                Ok(if *pos == *total {
                    schedule::Turn::Complete
                } else {
                    schedule::Turn::Progress
                })
            }
            (io::Outcome::Blocked(block), _) => Ok(schedule::Turn::Blocked(block)),
        }
    }
}

/// `pos <= total` is retained by every pending request.
pub(super) fn advance(pos: usize, total: usize, used: usize) -> Result<usize, RequestError> {
    consume::Error::check(used, total - pos).map_err(RequestError::Progress)?;
    Ok(pos + used)
}

pub(super) enum Commit<'a> {
    Idle,
    Direct {
        boundary: Boundary,
    },
    Close {
        tail: &'a mut Pending,
    },
    Queued {
        claim: calls::Claim<'a>,
        boundary: Boundary,
        drive: Drive,
    },
}

pub(super) enum Drive {
    Idle,
    Ready,
}

impl<'a> Commit<'a> {
    pub(super) fn queued(
        permit: calls::Permit<'a>,
        item: Pending,
        boundary: Boundary,
        drive: Drive,
    ) -> Self {
        Self::Queued {
            claim: permit.claim(item),
            boundary,
            drive,
        }
    }

    pub(super) fn apply<C: session::Storage>(self, record: &mut calls::Record<C>) -> Drive {
        match self {
            Self::Idle => Drive::Idle,
            Self::Direct { boundary } => {
                record.boundary = boundary;
                Drive::Idle
            }
            Self::Close { tail } => {
                tail.finish();
                record.boundary = Boundary::End;
                Drive::Ready
            }
            Self::Queued {
                claim,
                boundary,
                drive,
            } => {
                claim.push(&mut record.requests);
                record.boundary = boundary;
                drive
            }
        }
    }

    pub(super) fn resolve<T>(
        result: Result<T, schedule::Error<crate::Status>>,
    ) -> Result<T, crate::Status> {
        match result {
            Ok(value) => Ok(value),
            Err(schedule::Error::Missing) => Err(crate::Status::new(
                crate::Code::Internal,
                Reason::RequestUnknown,
            )),
            Err(schedule::Error::Rejected(status)) => Err(status),
        }
    }
}

const _: () = assert!(core::mem::size_of::<Commit<'static>>() <= core::mem::size_of::<Pending>());

pub(in crate::client) struct Driver<'a, R> {
    requests: &'a calls::Requests,
    on_retire: &'a mut R,
}

impl<'a, R> Driver<'a, R> {
    pub(in crate::client) fn new(reqs: &'a calls::Requests, retire: &'a mut R) -> Self {
        Self {
            requests: reqs,
            on_retire: retire,
        }
    }
}

impl<Cn, C, R> domain::Driver<Cn, calls::Record<C>> for Driver<'_, R>
where
    C: session::Storage,
    Cn: client::Connection<Owner = C, Error = conn::Error>,
    R: FnMut(calls::Record<C>),
{
    type Error = RequestError;

    fn data(
        &mut self,
        conn: &mut Cn,
        key: Cn::Key,
        record: &mut calls::Record<C>,
    ) -> Result<schedule::Turn<Cn::Block>, Self::Error> {
        let Some(mut front) = self.requests.front(&mut record.requests) else {
            return Ok(schedule::Turn::Complete);
        };
        let turn = front.get_mut().turn(conn, key)?;
        if turn != schedule::Turn::Complete {
            return Ok(turn);
        }

        front.discard();
        if record.requests.is_empty() {
            Ok(schedule::Turn::Complete)
        } else {
            Ok(schedule::Turn::Progress)
        }
    }

    fn settle(
        &mut self,
        conn: &mut Cn,
        key: Cn::Key,
        record: &mut calls::Record<C>,
        term: schedule::Terminal<Self::Error>,
    ) -> domain::Settlement<Self::Error, Cn::Key> {
        match term {
            schedule::Terminal::Complete => {
                if record.response.is_none() && matches!(record.boundary, Boundary::End) {
                    domain::Settlement::Remove
                } else {
                    domain::Settlement::Keep
                }
            }
            schedule::Terminal::Idle => {
                self.requests.clear(&mut record.requests);
                match conn.reset(key, Reset::Internal) {
                    Ok(()) => domain::Settlement::Remove,
                    Err(error) => domain::Settlement::Failed {
                        disposition: domain::Disposition::Keep,
                        error: domain::Failure::Idle(error),
                    },
                }
            }
            schedule::Terminal::Failed(cause) => {
                self.requests.clear(&mut record.requests);
                match conn.reset(key, Reset::Internal) {
                    Ok(()) => domain::Settlement::Failed {
                        disposition: domain::Disposition::Remove,
                        error: domain::Failure::Turn(cause),
                    },
                    Err(error) => domain::Settlement::Failed {
                        disposition: domain::Disposition::Keep,
                        error: domain::Failure::Settle {
                            terminal: cause,
                            error,
                        },
                    },
                }
            }
        }
    }

    fn removed(&mut self, mut record: calls::Record<C>) {
        self.requests.clear(&mut record.requests);
        (self.on_retire)(record);
    }
}
