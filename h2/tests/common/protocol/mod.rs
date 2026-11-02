use core::iter;

use o3::buffer::{resident, storage::Shared};
use sark_core::{life::session::Storage, manifold::stream::Boundary};
use sark_h2::{
    ClientRole, Endpoint, Role, ServerRole,
    conn::{
        Build, CLIENT_PREFACE, Error,
        config::{self, Config, Validated},
        event::Event,
        machine::Connection,
    },
    server::response::{Encoder, Pending},
    stream::{Id, Key},
};
use sark_http::{
    field::{
        self,
        metadata::section::rules::{Policy, Push, RejectPush, RetainRaw},
    },
    payload::response::{dispatch::Accept, headers::set::Set, status::Code},
    request::body::Ref,
};

use crate::common::{
    INITIAL_SETTINGS,
    headers::{Authority, entry},
};

pub struct Session<'a, R: Role, P: Policy, C: Storage, A: Push> {
    conn: &'a mut Connection<R, P, C, A>,
}

impl<'a, R: Role, P: Policy, C: Storage, A: Push> Session<'a, R, P, C, A> {
    pub fn new(conn: &'a mut Connection<R, P, C, A>) -> Self {
        Self { conn }
    }

    pub fn try_prime(self) -> Result<(), Error> {
        let conn = self.conn;
        conn.transfer().drain(usize::MAX);
        if matches!(R::ENDPOINT, Endpoint::Server) {
            conn.ingress().ingest(CLIENT_PREFACE)?;
        }
        conn.ingress().ingest(INITIAL_SETTINGS)?;
        while conn.ingress().poll_event().is_some() {}
        conn.transfer().drain(usize::MAX);
        Ok(())
    }
}

pub mod conn;

pub mod content_length;

pub mod rules;

pub mod session;

pub mod trailers;

#[derive(Debug)]
pub enum OpenError {
    Field(field::Error),
    Protocol(Error),
}

impl Session<'_, ClientRole, RetainRaw, Shared, RejectPush> {
    pub fn try_open(self) -> Result<Id, OpenError> {
        use sark_core::manifold::stream::Boundary;

        use crate::common::{headers::entry, hpack::GET};
        let [method, scheme, host, path] =
            GET.map(|field| entry(field.name, field.value).map_err(OpenError::Field));
        let id = self
            .conn
            .streams()
            .headers()
            .request([method?, scheme?, host?, path?], Boundary::End)
            .map_err(OpenError::Protocol)?;
        self.conn.transfer().drain(usize::MAX);
        Ok(id)
    }
}

pub mod preface;

impl<'d> Session<'_, ServerRole, RetainRaw, resident::Shared<'d>, RejectPush> {
    pub fn try_request(
        budget: resident::Budget<'d>,
    ) -> Result<(Connection<ServerRole, RetainRaw, resident::Shared<'d>>, Key), ReplyError> {
        Self::try_fields(
            budget,
            [
                field::literal!(entry(b":method", b"GET")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/")),
                field::literal!(entry(b":authority", b"localhost")),
            ],
            Boundary::End,
        )
    }

    pub fn try_fields(
        budget: resident::Budget<'d>,
        fields: impl field::Source,
        boundary: Boundary,
    ) -> Result<(Connection<ServerRole, RetainRaw, resident::Shared<'d>>, Key), ReplyError> {
        let mut client = Connection::<ClientRole>::try_new().map_err(ReplyError::Config)?;
        client
            .streams()
            .headers()
            .request(fields, boundary)
            .map_err(ReplyError::Protocol)?;
        Self::try_input(budget, iter::once(client.output().first()))
    }

    pub fn try_input<'wire>(
        budget: resident::Budget<'d>,
        input: impl IntoIterator<Item = &'wire [u8]>,
    ) -> Result<(Connection<ServerRole, RetainRaw, resident::Shared<'d>>, Key), ReplyError> {
        let plan = Validated::new(Config::default()).map_err(ReplyError::Config)?;
        let mut server = Connection::from_plan(plan).map_err(ReplyError::Build)?;
        server
            .ingress()
            .try_bind(budget)
            .map_err(ReplyError::Protocol)?;
        for bytes in input {
            server
                .ingress()
                .ingest(bytes)
                .map_err(ReplyError::Protocol)?;
        }
        let stream = iter::from_fn(|| server.ingress().poll_event())
            .find_map(|event| match event {
                Event::Headers { stream_id: id, .. } => Some(id),
                _ => None,
            })
            .ok_or(ReplyError::MissingHeaders)?;
        server.transfer().drain(usize::MAX);
        Ok((server, stream))
    }
}

impl<'d, P: Policy> Session<'_, ServerRole, P, resident::Shared<'d>, RejectPush> {
    pub fn try_finish(self) -> Result<Option<Pending<'d>>, ReplyError> {
        let conn = self.conn;
        let id = match conn.ingress().poll_event() {
            Some(Event::Headers { stream_id: id, .. }) => id,
            _ => return Err(ReplyError::MissingHeaders),
        };
        let encoder = Encoder::new(conn, id, Ref::from_slice(&[]));
        let prepared = Accept::fixed(encoder, Code::NO_CONTENT, Set::<0>::new(), Vec::new())
            .map_err(|(error, stream)| ReplyError::Prepare(error, stream))?;
        let pending = prepared.finish().map_err(ReplyError::Protocol)?;
        conn.transfer().drain(usize::MAX);
        Ok(pending)
    }
}

impl Session<'_, ClientRole, RetainRaw, Shared, Authority> {
    pub fn try_start(self) -> Result<(), Error> {
        self.conn
            .streams()
            .headers()
            .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)?;
        self.conn.transfer().drain(usize::MAX);
        Ok(())
    }
}

#[derive(Debug)]
pub enum ReplyError {
    Config(config::Error),
    Build(Build),
    Protocol(Error),
    Prepare(Error, Key),
    MissingHeaders,
}
