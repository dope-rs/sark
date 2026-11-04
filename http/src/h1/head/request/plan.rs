use std::{convert::Infallible, num::NonZero};

use line::error;

use crate::{
    h1::{
        field,
        field::Connection,
        framing,
        head::{
            Boundary, Limit, ParseError, Semantic, line,
            request::{Fields, route},
            state,
            tail::Tail,
        },
        switch, target,
    },
    payload::response::Method,
    request,
    request::span,
};

#[derive(Clone, Copy)]
pub struct Plan {
    admission: framing::Admission,
    pub(super) route: route::Line,
    connection: Connection,
    upgrades: switch::Offers,
    version: line::Version,
    tail: Tail,
}

#[derive(Clone, Copy)]
pub(in crate::h1::head::request) struct Start {
    target: span::Span,
    plan: target::Target,
    method: u32,
    version: line::Version,
    transition: Method,
    fields: u32,
}

#[derive(Clone, Copy)]
pub(in crate::h1::head::request) struct Kind;

impl Start {
    pub(super) fn try_new(
        line: line::Request<'_>,
        limit: Limit,
    ) -> Result<Self, ParseError<Infallible>> {
        if line.end() > limit.bytes() {
            return Err(ParseError::too_large(line.end(), limit.bytes()));
        }
        Self::from(line).map_err(ParseError::Boundary)
    }

    pub(super) fn from(line: line::Request<'_>) -> Result<Self, Boundary> {
        let fields = line.end();
        let fields = u32::try_from(fields).map_err(|_| Boundary::TooLarge {
            actual: fields,
            limit: u32::MAX as usize,
        })?;
        Ok(Self {
            target: line.target_span(),
            plan: line.plan(),
            method: line.method().len() as u32,
            version: line.version(),
            transition: Method::from_bytes(line.method()),
            fields,
        })
    }
}

impl Plan {
    pub const fn admission(&self) -> framing::Admission {
        self.admission
    }

    pub const fn request(&self) -> request::Layout {
        self.route.layout()
    }

    pub const fn connection(&self) -> Connection {
        self.connection
    }

    pub const fn transition(&self) -> switch::Intent {
        self.route.transition()
    }

    pub const fn upgrades(&self) -> switch::Offers {
        self.upgrades
    }

    pub const fn version(&self) -> line::Version {
        self.version
    }

    pub const fn fields(&self) -> span::Span {
        self.tail.fields()
    }

    pub const fn wire_len(&self) -> NonZero<u32> {
        self.tail.wire_len()
    }
}

impl state::Kind for Kind {
    type Start = Start;
    type Fields = Fields;
    type Parts = Plan;

    fn start<E>(self, bytes: &[u8]) -> Result<line::Progress<Start>, ParseError<E>> {
        let progress = match line::Request::progress(bytes) {
            Ok(progress) => progress,
            Err(error::Request::Syntax) => return Err(ParseError::invalid()),
            Err(error::Request::Target(cause)) => {
                return Err(ParseError::Semantic(Semantic::Authority(cause)));
            }
            Err(error::Request::Span(cause)) => {
                return Err(ParseError::Boundary(Boundary::Span(cause)));
            }
        };
        match progress {
            line::Progress::Complete(line) => match Start::from(line) {
                Ok(start) => Ok(line::Progress::Complete(start)),
                Err(cause) => Err(ParseError::Boundary(cause)),
            },
            line::Progress::Partial(cursor) => Ok(line::Progress::Partial(cursor)),
        }
    }

    fn fields(start: Start) -> u32 {
        start.fields
    }

    fn observe(
        start: &Start,
        fields: Fields,
        field: &field::Entry<'_>,
    ) -> Result<Fields, Semantic> {
        fields.apply(field, start.version)
    }

    fn finish<E>(fields: Fields, start: Start, tail: Tail) -> Result<Plan, ParseError<E>> {
        let conn = fields.connection();
        let upgrades = fields.upgrades();
        let intent = fields
            .transition(start.transition, start.version)
            .map_err(ParseError::Semantic)?;
        let (permit, request, auth) = fields
            .admit_parts(start.plan, start.target, start.version)
            .map_err(ParseError::Semantic)?;
        Ok(Plan {
            admission: permit,
            route: route::Line::new(request, start.method, auth, intent, permit.expectation()),
            connection: conn,
            upgrades,
            version: start.version,
            tail,
        })
    }
}
