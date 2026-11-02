use std::{iter, num::NonZeroUsize};

use sark_core::manifold::{
    client::event::Reset,
    stream::{Boundary, Section},
};
use sark_grpc::{
    Code, Reason, Status,
    client::config::{self, Input, Limits, Output},
    details::{Diagnostic, Fault},
    message::{self, Compression, Frame},
    metadata::Metadata,
    protocol::Block,
};
use sark_h2::{
    ServerRole,
    conn::{self, DataOutcome, event::Event, machine::Connection},
    frame::numbers::ErrorCode,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

use crate::common::grpc::{call::ingress::Failure, replies};

pub const FIRST_RESPONSE: &[u8] = b"first";
pub const SECOND_RESPONSE: &[u8] = b"second";
pub const HTTP_BODY: &[u8] = b"<h1>unavailable</h1>";
pub const SMALL_RESPONSE: &[u8] = b"x";

pub const BAD_MESSAGE_FRAMES: [(&[u8], [u8; 5], Code, Diagnostic<'static>); 5] = [
    (
        SMALL_RESPONSE,
        [2, 0, 0, 0, 0],
        Code::Internal,
        Diagnostic::Message(&message::Error::BadCompressionFlag(2)),
    ),
    (
        SMALL_RESPONSE,
        [0, 0, 0, 0, 2],
        Code::ResourceExhausted,
        Diagnostic::Message(&message::Error::MessageTooLarge { len: 2, max: 1 }),
    ),
    (
        SMALL_RESPONSE,
        [1, 0, 0, 0, 0],
        Code::Internal,
        Diagnostic::Local {
            code: Code::Internal,
            reason: Reason::Compression,
        },
    ),
    (
        SMALL_RESPONSE,
        [0; 5],
        Code::Unimplemented,
        Diagnostic::Local {
            code: Code::Unimplemented,
            reason: Reason::UnaryResponseMany,
        },
    ),
    (
        b"",
        [0; 5],
        Code::Unimplemented,
        Diagnostic::Local {
            code: Code::Unimplemented,
            reason: Reason::UnaryResponseMany,
        },
    ),
];

pub const BAD_STATUS_FIELDS: [(Code, Fault, &[Entry<'static>]); 4] = [
    (
        Code::Internal,
        Fault::GrpcStatus,
        &[Entry::new(b"grpc-status", b"x")],
    ),
    (
        Code::InvalidArgument,
        Fault::DuplicateStatus,
        &[
            Entry::new(b"grpc-status", b"0"),
            Entry::new(b"grpc-status", b"1"),
        ],
    ),
    (
        Code::InvalidArgument,
        Fault::Duplicate,
        &[
            Entry::new(b"grpc-message", b"first"),
            Entry::new(b"grpc-message", b"second"),
        ],
    ),
    (
        Code::Internal,
        Fault::Details,
        &[Entry::new(b"grpc-status-details-bin", b"AQ==,AQ==")],
    ),
];

pub const RESET_FAILURES: [(ErrorCode, Reset, Code); 2] = [
    (
        ErrorCode::EnhanceYourCalm,
        Reset::ResourceExhausted,
        Code::ResourceExhausted,
    ),
    (
        ErrorCode::InadequateSecurity,
        Reset::InadequateSecurity,
        Code::PermissionDenied,
    ),
];

pub const UNARY_BUDGETS: [(usize, usize, Reason, (NonZeroUsize, usize)); 3] = [
    (1, 1024, Reason::LiveMessages, (NonZeroUsize::MIN, 0)),
    (
        2,
        8,
        Reason::LiveMessage,
        (NonZeroUsize::MIN.saturating_add(5), 3),
    ),
    (
        2,
        5,
        Reason::LiveMessage,
        (NonZeroUsize::MIN.saturating_add(5), 5),
    ),
];

type Field<'a> = Option<&'a [u8]>;

pub const HTTP_FAILURES: [(Field<'static>, Field<'static>, Code); 3] = [
    (None, None, Code::Unavailable),
    (Some(b"text/html"), Some(b"14"), Code::Unavailable),
    (
        Some(b"application/grpc"),
        Some(b"7"),
        Code::PermissionDenied,
    ),
];

pub const HTTP_TRAILERS: [(Option<Code>, Code); 2] =
    [(None, Code::Unavailable), (Some(Code::Ok), Code::Ok)];

pub const HTTP_MEDIA: [(Field<'static>, Boundary); 4] = [
    (None, Boundary::End),
    (Some(b"text/html"), Boundary::End),
    (None, Boundary::More),
    (Some(b"text/html"), Boundary::More),
];

#[derive(Debug)]
pub enum Error {
    Status(Status),
    Protocol(conn::Error),
    Frame(message::Error),
    Send {
        actual: DataOutcome,
        expected: usize,
    },
}

pub struct Settings {
    calls: Option<usize>,
    done: Option<usize>,
    events: Option<usize>,
    output: Option<usize>,
}

impl Settings {
    pub const fn new(
        calls: Option<usize>,
        done: Option<usize>,
        events: Option<usize>,
        output: Option<usize>,
    ) -> Self {
        Self {
            calls,
            done,
            events,
            output,
        }
    }

    pub fn try_build(self) -> Result<Limits, config::Error> {
        let Self {
            calls,
            done,
            events,
            output,
        } = self;
        let defaults = Limits::default();
        let input = defaults.input();
        let done = done.unwrap_or(input.completed());
        let events = events.unwrap_or(input.events());
        let input = Input::try_new(
            done,
            events,
            input.messages(),
            input.segments(),
            input.bytes(),
        )?;
        let output = output.unwrap_or(defaults.output().messages());
        let output = Output::try_new(output)?;
        let calls = calls.unwrap_or(defaults.calls());
        Limits::try_new(calls, defaults.bytes(), input, output)
    }

    pub fn try_ingress(
        calls: usize,
        done: usize,
        events: usize,
        messages: usize,
        bytes: usize,
    ) -> Result<Limits, config::Error> {
        let defaults = Input::default();
        let input = Input::try_new(done, events, messages, defaults.segments(), bytes)?;
        Limits::default().try_calls(calls)?.try_input(input)
    }
}

pub struct Response<'a> {
    server: &'a mut Connection<ServerRole>,
    stream: Id,
}

impl<'a> Response<'a> {
    pub const fn new(server: &'a mut Connection<ServerRole>, stream: Id) -> Self {
        Self { server, stream }
    }

    pub fn try_header(&mut self) -> Result<(), Error> {
        let headers = Block::response(&Metadata::new()).map_err(Error::Status)?;
        self.server
            .streams()
            .headers()
            .respond(self.stream, &headers, Boundary::More)
            .map_err(Error::Protocol)
    }

    pub fn try_http(
        &mut self,
        status: &[u8],
        media: Field<'_>,
        code: Field<'_>,
        boundary: Boundary,
    ) -> Result<(), Error> {
        let mut headers = Block::new();
        headers.push(b":status", status).map_err(Error::Status)?;
        if let Some(media) = media {
            headers
                .push(b"content-type", media)
                .map_err(Error::Status)?;
        }
        if let Some(code) = code {
            headers.push(b"grpc-status", code).map_err(Error::Status)?;
        }
        self.server
            .streams()
            .headers()
            .respond(self.stream, &headers, boundary)
            .map_err(Error::Protocol)
    }

    pub fn try_message(&mut self, payload: &[u8]) -> Result<(), Error> {
        let prefix = Frame::try_header(Compression::Plain, payload.len()).map_err(Error::Frame)?;
        self.try_parts(&prefix, payload, Boundary::More)
    }

    pub fn try_data(&mut self, payload: &[u8], boundary: Boundary) -> Result<(), Error> {
        self.try_parts(&[], payload, boundary)
    }

    fn try_parts(
        &mut self,
        prefix: &[u8],
        payload: &[u8],
        boundary: Boundary,
    ) -> Result<(), Error> {
        let sent = self
            .server
            .streams()
            .try_parts(self.stream, prefix, payload, boundary)
            .map_err(Error::Protocol)?;
        let expected = prefix.len() + payload.len();
        if !matches!(sent, DataOutcome::Sent(count) if count.get() == expected) {
            return Err(Error::Send {
                actual: sent,
                expected,
            });
        }
        Ok(())
    }

    pub fn try_trailers(&mut self, code: Option<Code>) -> Result<(), Error> {
        let trailers = match code {
            Some(code) => Block::trailers(&Status::from_code(code), &Metadata::new())
                .map_err(Error::Status)?,
            None => Block::new(),
        };
        self.server
            .streams()
            .headers()
            .trail(self.stream, &trailers)
            .map_err(Error::Protocol)
    }

    pub fn try_status(
        &mut self,
        section: Section,
        boundary: Boundary,
        invalid: &[Entry<'_>],
    ) -> Result<(), Error> {
        let mut fields = match section {
            Section::Initial => Block::response(&Metadata::new()).map_err(Error::Status)?,
            Section::Trailing => {
                self.try_header()?;
                Block::new()
            }
        };
        for &field in invalid {
            fields.push_entry(field).map_err(Error::Status)?;
        }
        match section {
            Section::Initial => {
                self.server
                    .streams()
                    .headers()
                    .respond(self.stream, &fields, boundary)
            }
            Section::Trailing => self.server.streams().headers().trail(self.stream, &fields),
        }
        .map_err(Error::Protocol)
    }

    pub fn try_respond(mut self, payload: &[u8]) -> Result<(), Error> {
        self.try_header()?;
        self.try_message(payload)?;
        self.try_trailers(Some(Code::Ok))
    }

    pub fn try_overlap(
        server: &mut Connection<ServerRole>,
        [first, second]: [Id; 2],
    ) -> Result<(), Error> {
        Response::new(server, first).try_header()?;
        Response::new(server, first).try_message(FIRST_RESPONSE)?;
        Response::new(server, second).try_header()?;
        Response::new(server, second).try_message(SECOND_RESPONSE)?;
        Response::new(server, first).try_trailers(Some(Code::Ok))?;
        Response::new(server, second).try_trailers(Some(Code::Ok))
    }

    pub fn try_wire(request: &[u8], payload: &[u8]) -> Result<Vec<u8>, Failure> {
        let mut server = Connection::<ServerRole>::try_new().map_err(Failure::Build)?;
        server.ingress().ingest(request).map_err(Failure::Wire)?;
        while let Some(event) = server.ingress().poll_event() {
            let Event::Headers {
                stream_id: stream, ..
            } = event
            else {
                continue;
            };
            let head = Block::response(&Metadata::new()).map_err(Failure::Status)?;
            server
                .streams()
                .headers()
                .respond(stream.id(), &head, Boundary::More)
                .map_err(Failure::Wire)?;
            let actual = server
                .streams()
                .try_send(stream.id(), payload, Boundary::More)
                .map_err(Failure::Wire)?;
            if !matches!(actual, DataOutcome::Sent(sent) if sent.get() == payload.len()) {
                return Err(Failure::Data {
                    expected: payload.len(),
                    actual,
                });
            }
            let tail = Block::trailers(&Status::ok(), &Metadata::new()).map_err(Failure::Status)?;
            server
                .streams()
                .headers()
                .trail(stream.id(), &tail)
                .map_err(Failure::Wire)?;
        }
        Ok(server.output().first().to_vec())
    }

    pub fn try_framed(request: &[u8], payload: &[u8]) -> Result<Vec<u8>, replies::Error> {
        use replies::Error;

        let mut server = Connection::<ServerRole>::try_new().map_err(Error::Build)?;
        server.ingress().ingest(request).map_err(Error::Ingress)?;
        let stream = iter::from_fn(|| server.ingress().poll_event())
            .find_map(|event| match event {
                Event::Headers { stream_id: id, .. } => Some(id.id()),
                _ => None,
            })
            .ok_or(Error::MissingStream)?;
        Response::new(&mut server, stream)
            .try_respond(payload)
            .map_err(Error::Response)?;
        Ok(server.output().first().to_vec())
    }
}
