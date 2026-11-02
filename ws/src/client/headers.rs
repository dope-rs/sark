use std::borrow::Cow;

use o3::{
    alloc,
    buffer::{
        storage::{BuildError, Exact, Shared},
        write::{Sink as _, consume},
    },
};
use sark_http::field::{self, storage};

use crate::client;

pub(in crate::client) const MAX_BYTES: usize = 16 * 1024;

const OWNED_REQUEST_FIELDS: [&str; 9] = [
    "host",
    "user-agent",
    "accept",
    "upgrade",
    "sec-websocket-version",
    "sec-websocket-key",
    "connection",
    "content-length",
    "transfer-encoding",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Name(field::Error),
    Value(field::Error),
    Unsupported(crate::upgrade::Negotiation),
    Reserved {
        index: usize,
    },
    Length {
        current: usize,
        additional: usize,
        maximum: usize,
    },
    Build(BuildError<consume::Error>),
}

pub struct Field<N = String, V = String> {
    name: field::Name<N>,
    value: field::Value<V>,
}

pub struct Changes<'a, N, V, R> {
    added: &'a [Field<N, V>],
    removed: &'a [R],
}

#[must_use = "a handshake request must be finished or rejected"]
pub struct Request<'a> {
    config: &'a client::Config,
    key: &'a [u8; 24],
}

#[must_use = "an encoded handshake request must be submitted"]
#[repr(transparent)]
pub struct Encoded {
    bytes: Shared,
}

struct Plan<'a, 'request, N, V, S> {
    request: Request<'request>,
    added: &'a [Field<N, V>],
    select: S,
}

struct All;

trait Selection {
    fn write<X: storage::Text, Y: storage::Text, E>(
        &self,
        field: &Field<X, Y>,
        append: &mut impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E>;
}

impl<N: storage::Text, V: storage::Text> Field<N, V> {
    pub fn new(name: N, value: V) -> Result<Self, Error> {
        let name = field::Name::new(name).map_err(Error::Name)?;
        if let Some(header) = crate::upgrade::Negotiation::from_name(name.as_str().as_bytes()) {
            return Err(Error::Unsupported(header));
        }
        if let Some(index) = OWNED_REQUEST_FIELDS
            .iter()
            .position(|owned| name.as_str().eq_ignore_ascii_case(owned))
        {
            return Err(Error::Reserved { index });
        }
        let value = field::Value::new(value).map_err(Error::Value)?;
        Ok(Self { name, value })
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl Field<Cow<'_, str>, Cow<'_, str>> {
    pub(in crate::client) fn try_owned(self) -> Result<Field, alloc::Exhausted> {
        Ok(Field {
            name: self.name.try_owned()?,
            value: self.value.try_owned()?,
        })
    }
}

impl<'a, N, V, R> Changes<'a, N, V, R> {
    #[must_use]
    pub const fn new(added: &'a [Field<N, V>], omit: &'a [R]) -> Self {
        Self {
            added,
            removed: omit,
        }
    }
}

impl<'request> Request<'request> {
    pub(in crate::client) const fn new(
        config: &'request client::Config,
        key: &'request [u8; 24],
    ) -> Self {
        Self { config, key }
    }

    pub(in crate::client) fn validate(
        config: &'request client::Config,
    ) -> Result<(), client::Error> {
        const KEY: [u8; 24] = [0; 24];

        let added = &[] as &[Field<&str, &str>];
        Self::new(config, &KEY).plan(added, All).measure()?;
        Ok(())
    }

    pub fn finish(self) -> Result<Encoded, client::Error> {
        let added = &[] as &[Field<&str, &str>];
        self.finish_with(added)
    }

    pub fn finish_with<N, V>(self, added: &[Field<N, V>]) -> Result<Encoded, client::Error>
    where
        N: storage::Text,
        V: storage::Text,
    {
        self.plan(added, All).encode()
    }

    pub fn finish_edits<N, V, R>(
        self,
        edits: Changes<'_, N, V, R>,
    ) -> Result<Encoded, client::Error>
    where
        N: storage::Text,
        V: storage::Text,
        R: storage::Text,
    {
        self.plan(edits.added, edits.removed).encode()
    }

    fn plan<'a, N, V, S>(self, added: &'a [Field<N, V>], select: S) -> Plan<'a, 'request, N, V, S> {
        Plan {
            request: self,
            added,
            select,
        }
    }
}

impl Encoded {
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(in crate::client) fn into_shared(self) -> Shared {
        self.bytes
    }
}

impl Selection for All {
    fn write<X: storage::Text, Y: storage::Text, E>(
        &self,
        field: &Field<X, Y>,
        append: &mut impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        append(field.name().as_bytes())?;
        append(b": ")?;
        append(field.value().as_bytes())?;
        append(b"\r\n")
    }
}

impl<R: storage::Text> Selection for &[R] {
    fn write<X: storage::Text, Y: storage::Text, E>(
        &self,
        field: &Field<X, Y>,
        append: &mut impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        if self
            .iter()
            .any(|name| field.name().eq_ignore_ascii_case(name.as_str()))
        {
            return Ok(());
        }
        All.write(field, append)
    }
}

impl<N, V, S> Plan<'_, '_, N, V, S>
where
    N: storage::Text,
    V: storage::Text,
    S: Selection,
{
    fn encode(self) -> Result<Encoded, client::Error> {
        let cap = self.measure()?;
        let bytes = Exact::try_build(cap, |writer| self.visit(|bytes| writer.write_slice(bytes)))
            .map_err(Error::Build)?;
        Ok(Encoded {
            bytes: bytes.into_shared(),
        })
    }

    fn measure(&self) -> Result<usize, Error> {
        let mut total = 0usize;
        self.visit(|bytes| {
            total = total
                .checked_add(bytes.len())
                .filter(|next| *next <= MAX_BYTES)
                .ok_or(Error::Length {
                    current: total,
                    additional: bytes.len(),
                    maximum: MAX_BYTES,
                })?;
            Ok(())
        })?;
        Ok(total)
    }

    fn visit<E>(&self, mut append: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        append(b"GET ")?;
        append(self.request.config.path().as_bytes())?;
        append(b" HTTP/1.1\r\nHost: ")?;
        append(self.request.config.host().as_bytes())?;
        append(b"\r\nUser-Agent: ")?;
        let default = field::literal!(field::Value::try_bytes(b"sark-ws/0.1"));
        append(
            self.request
                .config
                .handshake
                .user_agent
                .as_ref()
                .map_or(default.as_bytes(), field::Value::as_bytes),
        )?;
        append(b"\r\nAccept: */*")?;
        append(b"\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ")?;
        append(self.request.key)?;
        append(b"\r\nConnection: Upgrade\r\n")?;
        for header in &self.request.config.handshake.headers {
            self.select.write(header, &mut append)?;
        }
        for header in self.added {
            self.select.write(header, &mut append)?;
        }
        append(b"\r\n")
    }
}

const _: () = {
    assert!(core::mem::size_of::<field::Name<String>>() == core::mem::size_of::<String>());
    assert!(core::mem::size_of::<field::Value<String>>() == core::mem::size_of::<String>());
    assert!(core::mem::size_of::<Field>() == 2 * core::mem::size_of::<String>());
    assert!(core::mem::size_of::<Request<'static>>() == 2 * core::mem::size_of::<usize>());
    assert!(
        core::mem::size_of::<Changes<'static, &str, &str, &str>>()
            == 4 * core::mem::size_of::<usize>()
    );
    assert!(
        core::mem::size_of::<
            Plan<'static, 'static, &'static str, &'static str, &'static [&'static str]>,
        >() == 6 * core::mem::size_of::<usize>()
    );
    assert!(core::mem::size_of::<Encoded>() == core::mem::size_of::<Shared>());
    assert!(core::mem::size_of::<Error>() <= 4 * core::mem::size_of::<usize>());
};
