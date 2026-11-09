use core::marker;

use conn::{Message, config, event, ingress, outputs};
use dope::core::random;
use o3::buffer::{pool, pool::owners, storage};
use sark_core::{
    life::session,
    manifold::{
        client::{
            self,
            event::{Reset, Response, Shutdown},
            io::{self, Outcome, Read},
        },
        schedule, server,
        stream::Boundary,
    },
};
use sark_http::{
    field::{
        self,
        metadata::{
            block,
            section::{rules, state::Selected},
        },
    },
    payload::compress::dynamic::hash,
};

use crate::{ClientRole, ServerRole, conn, frame::numbers, stream};

mod bound;
pub(crate) mod build;
mod decoder;
pub(crate) mod header;
pub mod ops;
pub(crate) use bound::Bound;
pub(in crate::conn) mod ready;
pub mod streams;
pub mod view;

use ops::state;
pub use ready::Ready;

pub struct Connection<
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(super) ingress: ingress::Ingress<F, C, E>,
    pub(in crate::conn) egress: outputs::Egress<C>,
    pub(in crate::conn) state: state::State<R>,
    pub(super) push: marker::PhantomData<fn() -> A>,
}

impl<R: crate::Role> Connection<R> {
    pub fn try_new() -> Result<Self, config::Error> {
        Self::with_config(config::Config::default())
    }

    pub fn with_config(config: config::Config) -> Result<Self, config::Error> {
        Self::from_plan(config::Validated::new(config)?).map_err(config::Error::Build)
    }

    pub fn with_push<A: rules::Push>()
    -> Result<Connection<R, rules::RetainRaw, storage::Shared, A>, config::Error> {
        Self::try_push(config::Config::default())
    }

    pub fn try_push<A: rules::Push>(
        config: config::Config,
    ) -> Result<Connection<R, rules::RetainRaw, storage::Shared, A>, config::Error> {
        Connection::from_plan(config::Validated::new(config)?).map_err(config::Error::Build)
    }
}

impl<R, F, C, A, E> Connection<R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn from_plan(config: config::Validated<R>) -> Result<Self, conn::Build> {
        let seed = random::Seed::acquire().map_err(conn::Build::Entropy)?;
        Self::from_seed(config, hash::Seed::new(&seed))
    }
}

impl<R, F, C, A, E> Connection<R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn ingress(&mut self) -> ops::Ingress<'_, R, F, C, A, E> {
        ops::Ingress { conn: self }
    }

    pub fn control(&mut self) -> ops::Control<'_, R, F, C, A, E> {
        ops::Control { conn: self }
    }

    pub fn transfer(&mut self) -> ops::Transfer<'_, R, F, C, A, E> {
        ops::Transfer::new(self)
    }

    pub fn flow(&mut self) -> ops::Flow<'_, R, F, C, A, E> {
        ops::Flow { conn: self }
    }

    pub fn streams(&mut self) -> streams::Set<'_, R, F, C, A, E> {
        streams::Set { conn: self }
    }

    pub fn output(&self) -> view::Output<'_, R, F, C, A, E> {
        view::Output { conn: self }
    }

    pub fn settings(&self) -> view::Settings<'_, R, F, C, A, E> {
        view::Settings { conn: self }
    }

    pub fn status(&self) -> view::Status<'_, R, F, C, A, E> {
        view::Status { conn: self }
    }
}

impl<F: rules::Policy, C: session::Storage> Read for Connection<ClientRole, F, C> {
    fn has_event(&mut self) -> bool {
        self.ingress().has_event()
    }

    fn is_blocked(&mut self) -> bool {
        self.ingress().is_blocked()
    }

    fn ingest(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.ingress().ingest(bytes)
    }

    fn ingest_owned(&mut self, bytes: C) -> Result<(), Self::Error> {
        self.ingress().ingest_owned(bytes)
    }

    fn ingest_from<I>(&mut self, source: &mut I) -> Result<(), Self::Error>
    where
        I: Iterator<Item = C>,
    {
        self.ingress().ingest_from(source)
    }

    fn resume(&mut self) -> Result<(), Self::Error> {
        self.ingress().resume()
    }
}

impl<F: rules::Policy, C: session::Storage> io::Write for Connection<ClientRole, F, C> {
    type Prepared<'a>
        = crate::client::Prepared<'a, C>
    where
        Self: 'a;

    fn outbound(&self) -> &[u8] {
        self.output().first()
    }

    fn outbound_len(&self) -> usize {
        self.output().len()
    }

    fn drain(&mut self, len: usize) {
        self.transfer().drain(len);
    }

    fn take(&mut self, prefix: &mut [u8]) -> Result<io::Output<C>, io::Error> {
        let write = self.transfer().take_plain(prefix)?;
        Ok(crate::client::Prepared::<C>::output(write))
    }

    fn prepare(&mut self, prefix: pool::Cursor) -> Result<Self::Prepared<'_>, io::Reject> {
        self.transfer().prepare(prefix)
    }
}
impl<F: rules::Policy, C: session::Storage> client::Connection for Connection<ClientRole, F, C> {
    type Error = conn::Error;
    type Owner = C;
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Message = Message;
    type Fields = F::Block;
    type Selection = F::Selection;
    type Data = event::DataPayload<C>;
    type GoAway = stream::Id;

    fn start<S: field::Source>(
        &mut self,
        fields: S,
        boundary: Boundary,
    ) -> Result<Self::Key, Self::Error> {
        self.streams().headers().request_key(fields, boundary)
    }

    fn send(
        &mut self,
        stream: Self::Key,
        prefix: &[u8],
        body: &[u8],
        offset: usize,
        boundary: Boundary,
    ) -> Result<Outcome<Self::Block>, Self::Error> {
        let body = body.get(offset..).ok_or(conn::Error::DataRange {
            offset,
            prefix: prefix.len(),
            body: body.len(),
        })?;
        Ok(self
            .streams()
            .try_to(stream, prefix, body, boundary, streams::Limit::QUANTUM)?
            .write())
    }

    fn send_owned(
        &mut self,
        stream: Self::Key,
        prefix: &[u8],
        body: &storage::Shared,
        offset: usize,
        boundary: Boundary,
    ) -> Result<Outcome<Self::Block>, Self::Error> {
        Ok(self
            .streams()
            .retained()
            .try_parts(stream, prefix, body, offset, boundary)?
            .write())
    }

    fn reset(&mut self, stream: Self::Key, reason: Reset) -> Result<(), Self::Key> {
        self.streams()
            .try_cancel(stream, numbers::ErrorCode::from_reason(reason))
    }

    fn cancel(&mut self, stream: Self::Key, reason: Reset) {
        self.streams()
            .cancel(stream, numbers::ErrorCode::from_reason(reason));
        self.ingress.events.cancel(stream);
    }

    fn poll(&mut self) -> Option<Response<Self>> {
        loop {
            let event = self.ingress().poll_event()?;
            match event {
                event::Event::Headers {
                    stream_id: id,
                    head,
                    boundary,
                    section,
                    ..
                } => {
                    let (fields, select, _) = head.into_parts();
                    return Some(Response::Head {
                        message: Message::new(id, boundary),
                        fields,
                        selection: select.into_inner(),
                        section,
                    });
                }
                event::Event::Data {
                    stream_id: id,
                    data,
                    boundary,
                } => {
                    return Some(Response::Data {
                        message: Message::new(id, boundary),
                        data,
                    });
                }
                event::Event::StreamReset {
                    stream_id: id,
                    error,
                    ..
                } => {
                    return Some(Response::Reset {
                        stream: id,
                        reason: error.reason(),
                    });
                }
                event::Event::GoAway {
                    last_stream_id: last,
                    error,
                    ..
                } => {
                    return Some(Response::GoAway {
                        boundary: last,
                        shutdown: match error {
                            numbers::ErrorCode::NoError => Shutdown::Clean,
                            _ => Shutdown::Failed,
                        },
                    });
                }
                _ => {}
            }
        }
    }

    fn is_accepted(&self, bound: &Self::GoAway, stream: Self::Key) -> bool {
        stream.id() <= *bound
    }
}

impl<F, C, E> server::Connection for Connection<ServerRole, F, C, rules::RejectPush, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    type Owner = C;
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Message = Message;
    type Fields = F::Block;
    type Selection = Selected<F>;
    type Control = Option<block::Plan>;
    type Permit = pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>;
    type Data = event::DataPayload<C>;
    type Retired = E::Retired;
    type Output = storage::Shared;
    type Error = conn::Error;
    type Writer<'a>
        = streams::Writer<'a, F, C, E>
    where
        Self: 'a;
    type Ready<'a>
        = Ready<'a, F, C, E>
    where
        Self: 'a;

    fn has_event(&mut self) -> bool {
        self.ingress().has_event()
    }

    fn ready(&mut self) -> Option<Self::Ready<'_>> {
        ready::Incoming::new(self).map(Ready::new)
    }

    fn poll_wake(&mut self) -> Option<schedule::Wake<Self::Key>> {
        self.flow().poll_wake()
    }

    fn writer(&mut self, stream: Self::Key) -> Self::Writer<'_> {
        streams::Writer::new(self, stream)
    }

    fn reset(&mut self, stream: Self::Key, reason: Reset) {
        self.streams()
            .cancel(stream, numbers::ErrorCode::from_reason(reason))
    }

    fn fail(&mut self) {
        self.control().goaway(numbers::ErrorCode::InternalError)
    }
}
