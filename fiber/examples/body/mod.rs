use std::{iter, num::TryFromIntError, ops::ControlFlow};

use cartel_pg::{
    api::{
        results::{self, source},
        tx::FinishError,
    },
    protocol::Reason,
    reply,
};
use o3::{buffer::write::Sink, mem::Rejected};
use sark::http::payload::response::status;
use sark_json::writing::{self, TryEncode, Writer};

#[sark_json::json(try_encode)]
pub(super) struct Record<'a> {
    id: i64,
    name: &'a [u8],
}

impl<'a> From<crate::UserView<'a>> for Record<'a> {
    fn from(user: crate::UserView<'a>) -> Self {
        Self {
            id: user.id,
            name: user.name.into(),
        }
    }
}

pub(super) struct Rows<T>(T);

impl<'a, I: Iterator<Item = crate::UserView<'a>>> Iterator for Rows<I> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Record::from)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, T: Data> IntoIterator for &'a Rows<T> {
    type Item = Record<'a>;
    type IntoIter = T::Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.records()
    }
}

#[sark_json::json(try_encode)]
pub(super) struct Body<T: Data> {
    ok: bool,
    #[field(seq, nested)]
    users: Rows<T>,
    error: sark_json::Token<0>,
}

#[derive(Debug)]
pub(super) enum Size {
    User { payload: usize },
    Users { rows: usize, payload: usize },
}

impl Size {
    const PREFIX: &[u8] = br#""response length overflow: rows="#;
    const PAYLOAD: &[u8] = b", payload=";

    fn parts(&self) -> (usize, usize) {
        match *self {
            Self::User { payload } => (1, payload),
            Self::Users { rows, payload } => (rows, payload),
        }
    }
}

impl writing::Size for Size {
    fn json_size(&self) -> usize {
        let (rows, payload) = self.parts();
        Self::PREFIX.len() + rows.json_size() + Self::PAYLOAD.len() + payload.json_size() + 1
    }
}

impl TryEncode for Size {
    fn try_write<'a, W: Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<writing::Failure<'a, W>, Writer<'a, W>> {
        let (rows, payload) = self.parts();
        let writer = rows.try_write(writer.put(Self::PREFIX)?)?;
        let writer = payload.try_write(writer.put(Self::PAYLOAD)?)?;
        writer.put(b"\"")
    }
}

const ENVELOPE_BYTES: usize = br#"{"ok":true,"users":[],"error":""}"#.len();
const USER_BYTES: usize = br#"{"id":,"name":""}"#.len() + 20;
const ESCAPE_EXPANSION: usize = 6;

pub(super) trait Data {
    type Iter<'a>: Iterator<Item = Record<'a>>
    where
        Self: 'a;

    fn records(&self) -> Self::Iter<'_>;

    fn try_bound(&self) -> Result<usize, Size>;
}

impl<P: source::Payload> Data for crate::Row<P> {
    type Iter<'a>
        = iter::Once<Record<'a>>
    where
        Self: 'a;

    fn records(&self) -> Self::Iter<'_> {
        iter::once(Record::from(self.view()))
    }

    fn try_bound(&self) -> Result<usize, Size> {
        let payload = self.payload_len().get();
        payload
            .checked_mul(ESCAPE_EXPANSION)
            .and_then(|len| len.checked_add(ENVELOPE_BYTES + USER_BYTES))
            .ok_or(Size::User { payload })
    }
}

impl<B: source::Batch> Data for crate::Rows<B> {
    type Iter<'a>
        = Rows<results::Iter<'a, B, crate::UserPage>>
    where
        Self: 'a;

    fn records(&self) -> Self::Iter<'_> {
        Rows(self.iter())
    }

    fn try_bound(&self) -> Result<usize, Size> {
        let rows = self.len().get();
        let payload = self.payload_len().get();
        rows.checked_mul(USER_BYTES + 1)
            .and_then(|len| len.checked_add(payload.checked_mul(ESCAPE_EXPANSION)?))
            .and_then(|len| len.checked_add(ENVELOPE_BYTES))
            .ok_or(Size::Users { rows, payload })
    }
}

impl Data for () {
    type Iter<'a> = iter::Empty<Record<'a>>;

    fn records(&self) -> Self::Iter<'_> {
        iter::empty()
    }

    fn try_bound(&self) -> Result<usize, Size> {
        Ok(ENVELOPE_BYTES)
    }
}

impl<T: Data> writing::TryMaxSize for Body<T> {
    type Error = Size;

    fn try_max_size(&self) -> Result<usize, Self::Error> {
        self.users.0.try_bound()
    }
}

pub(super) fn response<T: Data>(users: T) -> Result<crate::Response<T>, Size> {
    writing::Body::new(Body {
        ok: true,
        users: Rows(users),
        error: sark_json::Token::new().into(),
    })
    .try_response()
    .map_err(Rejected::discard)
}

#[sark_json::json(try_encode)]
pub(super) struct Failure<'region> {
    ok: bool,
    #[field(seq, nested)]
    users: Rows<()>,
    #[field(nested)]
    error: Cause<'region>,
}

#[sark::response(json)]
#[header("content-type", "application/json")]
pub(super) struct Error<'region> {
    status: status::Code,
    body: Failure<'region>,
}

enum Cause<'region> {
    InvalidId,
    InvalidName,
    Decode(cartel_pg::Error),
    Length(Size),
    Reply(reply::Error<'region, { crate::PG_CONNECTOR_ID }>),
    Rollback {
        operation: reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
        rollback: reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
    },
}

enum Message<'a, 'region> {
    Text(&'a str),
    Length(&'a Size),
    Rollback(
        &'a reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
        &'a reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
    ),
}

impl<'region> Error<'region> {
    fn new(status: status::Code, error: Cause<'region>) -> Self {
        Self {
            status,
            body: Failure {
                ok: false,
                users: Rows(()),
                error,
            },
        }
    }

    pub(super) fn invalid_name() -> Self {
        Self::new(status::Code::BAD_REQUEST, Cause::InvalidName)
    }
}

impl From<Size> for Error<'_> {
    fn from(error: Size) -> Self {
        Self::new(status::Code::INTERNAL_SERVER_ERROR, Cause::Length(error))
    }
}

impl From<TryFromIntError> for Error<'_> {
    fn from(_: TryFromIntError) -> Self {
        Self::new(status::Code::BAD_REQUEST, Cause::InvalidId)
    }
}

impl From<cartel_pg::Error> for Error<'_> {
    fn from(error: cartel_pg::Error) -> Self {
        Self::new(status::Code::BAD_GATEWAY, Cause::Decode(error))
    }
}

impl<'region> From<reply::Error<'region, { crate::PG_CONNECTOR_ID }>> for Error<'region> {
    fn from(error: reply::Error<'region, { crate::PG_CONNECTOR_ID }>) -> Self {
        let status = if matches!(error, reply::Error::Driver(cartel_pg::Error::NotFound)) {
            status::Code::NOT_FOUND
        } else {
            status::Code::BAD_GATEWAY
        };
        Self::new(status, Cause::Reply(error))
    }
}

impl<'region>
    From<
        FinishError<
            reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
            reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
        >,
    > for Error<'region>
{
    fn from(
        error: FinishError<
            reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
            reply::Error<'region, { crate::PG_CONNECTOR_ID }>,
        >,
    ) -> Self {
        match error {
            FinishError::Operation(error) | FinishError::Commit(error) => error.into(),
            FinishError::Rollback {
                operation: action,
                rollback,
            } => Self::new(
                status::Code::BAD_GATEWAY,
                Cause::Rollback {
                    operation: action,
                    rollback,
                },
            ),
        }
    }
}

impl<'region> Cause<'region> {
    fn message(&self) -> Message<'_, 'region> {
        let text = match self {
            Self::InvalidId => "id must be positive",
            Self::InvalidName => "name must be valid UTF-8 and 1..=128 bytes",
            Self::Decode(cartel_pg::Error::Protocol(Reason::TextUtf8 { .. })) => {
                "database row text is invalid UTF-8"
            }
            Self::Decode(cartel_pg::Error::Protocol(Reason::ColumnNull { .. })) => {
                "database row contains an unexpected null"
            }
            Self::Decode(_) => "database row decode failed",
            Self::Reply(error) => Self::reply(error),
            Self::Length(size) => return Message::Length(size),
            Self::Rollback {
                operation: action,
                rollback,
            } => return Message::Rollback(action, rollback),
        };
        Message::Text(text)
    }

    fn reply<'a>(error: &'a reply::Error<'_, { crate::PG_CONNECTOR_ID }>) -> &'a str {
        match error {
            reply::Error::Driver(cartel_pg::Error::NotFound) => "not found",
            reply::Error::Driver(_) => "database driver failed",
            reply::Error::Wait(_) => "database wait failed",
            reply::Error::Admission { .. } => "database admission wait failed",
            reply::Error::Open(_) => "database connection failed",
            reply::Error::Pinned(_) => "database reply is still pinned",
            reply::Error::Server(_) => "database server rejected the request",
        }
    }
}

impl writing::Size for Cause<'_> {
    fn json_size(&self) -> usize {
        match self.message() {
            Message::Length(size) => size.json_size(),
            Message::Rollback(action, rollback) => {
                2 + "transaction ".len()
                    + Self::reply(action).len()
                    + "; rollback ".len()
                    + Self::reply(rollback).len()
            }
            Message::Text(text) => text.len() + 2,
        }
    }
}

impl TryEncode for Cause<'_> {
    fn try_write<'a, W: Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<writing::Failure<'a, W>, Writer<'a, W>> {
        match self.message() {
            Message::Length(size) => size.try_write(writer),
            Message::Rollback(action, rollback) => writer
                .put(b"\"transaction ")?
                .put(Self::reply(action).as_bytes())?
                .put(b"; rollback ")?
                .put(Self::reply(rollback).as_bytes())?
                .put(b"\""),
            Message::Text(text) => writer.put_str(text),
        }
    }
}
