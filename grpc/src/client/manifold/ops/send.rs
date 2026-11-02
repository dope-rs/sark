use std::mem;

use dope::{core::driver::route::Identity, manifold::client::conn};
use o3::base::cell::region;
use sark_h2::stream;

use crate::{
    client::{
        call::{self, bidi, request, response, unary},
        manifold::{channel, error, ops::cancels},
    },
    metadata,
};

pub(in crate::client::manifold) struct Sender<'a, 'd, ID: Identity> {
    source: channel::Access<'a, 'a, 'd, ID>,
    conn: conn::Id<'d, ID>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::client::manifold) enum Outcome {
    Sent,
    Pending,
}

impl<'a, 'd, ID: Identity> Sender<'a, 'd, ID> {
    pub(in crate::client::manifold) fn new(
        token: &'a mut region::Token<region::Identity<'d, 1>>,
        port: &'a channel::Port<'_, 'd, ID>,
    ) -> Result<Self, error::Error> {
        let mut source = port.shared.source(token);
        let conn = source.open().ok_or_else(|| source.unavailable())?;
        Ok(Self { source, conn })
    }

    pub(in crate::client::manifold) fn bound(
        token: &'a mut region::Token<region::Identity<'d, 1>>,
        port: &'a channel::Port<'_, 'd, ID>,
        conn: conn::Id<'d, ID>,
    ) -> Self {
        Self {
            source: port.shared.source(token),
            conn,
        }
    }

    pub(in crate::client::manifold) const fn connection(&self) -> conn::Id<'d, ID> {
        self.conn
    }

    fn wake(&self) {
        self.source.signal(self.conn);
        self.source.shared().wake_space(&self.source);
    }

    fn admit<T>(&mut self, result: call::Admission<T>) -> Result<Option<T>, error::Error> {
        let result = match result {
            call::Admission::Ready(value) => Ok(Some(value)),
            call::Admission::Pending => return Ok(None),
            call::Admission::Rejected(status) => Err(status.into()),
        };
        self.wake();
        result
    }

    fn sent(&mut self, result: call::Admission<()>) -> Result<Outcome, error::Error> {
        Ok(match self.admit(result)? {
            Some(()) => Outcome::Sent,
            None => Outcome::Pending,
        })
    }

    pub(in crate::client::manifold) fn try_unary<C: crate::Codec>(
        &mut self,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Option<cancels::Registered<unary::Handle<'d, stream::Key>>>, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            Ok(state
                .active_mut(conn)?
                .session
                .try_unary(path, host, metadata, codec, message, done))
        })?;
        let result = self.admit(result)?;
        Ok(result.map(|value| {
            self.source.register(
                self.conn,
                cancels::Key::Unary(value.completion),
                value.stream,
                value,
            )
        }))
    }

    pub(in crate::client::manifold) fn try_server<C: crate::Codec>(
        &mut self,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Option<cancels::Registered<response::Stream<'d, stream::Key>>>, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            Ok(state
                .active_mut(conn)?
                .session
                .try_response(path, host, metadata, codec, message, done))
        })?;
        let result = self.admit(result)?;
        Ok(result.map(|value| {
            self.source.register(
                self.conn,
                cancels::Key::Stream(value.completion),
                value.stream,
                value,
            )
        }))
    }

    pub(in crate::client::manifold) fn open_request(
        &mut self,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<cancels::Registered<request::Stream<'d, stream::Key>>, error::Error> {
        let value = request::Session::open(
            &mut self.source.active_mut(self.conn)?.session,
            path,
            host,
            metadata,
        );
        self.source.signal(self.conn);
        let value = value?;
        Ok(self.source.register(
            self.conn,
            cancels::Key::Unary(value.response.completion),
            value.response.stream,
            value,
        ))
    }

    pub(in crate::client::manifold) fn open_bidi(
        &mut self,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<cancels::Registered<bidi::Stream<'d, stream::Key>>, error::Error> {
        let value = bidi::Session::open(
            &mut self.source.active_mut(self.conn)?.session,
            path,
            host,
            metadata,
        );
        self.source.signal(self.conn);
        let value = value?;
        Ok(self.source.register(
            self.conn,
            cancels::Key::Stream(value.response.completion),
            value.response.stream,
            value,
        ))
    }

    pub(in crate::client::manifold) fn try_request<C: crate::Codec>(
        &mut self,
        stream: &mut request::Stream<'d, stream::Key>,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Outcome, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            Ok(state
                .active_mut(conn)?
                .session
                .try_request(stream, codec, message, done))
        })?;
        self.sent(result)
    }

    pub(in crate::client::manifold) fn end_request(
        &mut self,
        stream: cancels::Registered<request::Stream<'d, stream::Key>>,
    ) -> Result<cancels::Registered<unary::Handle<'d, stream::Key>>, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            stream.try_map(|stream| {
                state
                    .active_mut(conn)?
                    .session
                    .finish_request(stream, done)
                    .map_err(error::Error::from)
            })
        });
        self.wake();
        result
    }

    pub(in crate::client::manifold) fn try_bidi<C: crate::Codec>(
        &mut self,
        stream: &mut bidi::Stream<'d, stream::Key>,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Outcome, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            Ok(state
                .active_mut(conn)?
                .session
                .try_bidi(stream, codec, message, done))
        })?;
        self.sent(result)
    }

    pub(in crate::client::manifold) fn finish_bidi(
        &mut self,
        stream: cancels::Registered<bidi::Stream<'d, stream::Key>>,
    ) -> Result<cancels::Registered<response::Stream<'d, stream::Key>>, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            stream.try_map(|stream| {
                state
                    .active_mut(conn)?
                    .session
                    .finish_bidi(stream, done)
                    .map_err(error::Error::from)
            })
        });
        self.wake();
        result
    }

    pub(in crate::client::manifold) fn try_half<C: crate::Codec>(
        &mut self,
        send: &mut bidi::Send<'d, stream::Key>,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Outcome, error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            Ok(state
                .active_mut(conn)?
                .session
                .try_send_half(send, codec, message, done))
        })?;
        self.sent(result)
    }

    pub(in crate::client::manifold) fn finish_half(
        &mut self,
        send: cancels::Registered<bidi::Send<'d, stream::Key>>,
    ) -> Result<(), error::Error> {
        let conn = self.conn;
        let result = self.source.with(|state, done| -> Result<_, error::Error> {
            send.try_map(|send| {
                state
                    .active_mut(conn)?
                    .session
                    .finish_send(send, done)
                    .map_err(error::Error::from)
            })
        });
        self.wake();
        result?;
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Outcome>() == mem::size_of::<Option<()>>());
