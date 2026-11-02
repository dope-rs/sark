use std::{convert::Infallible, ops::Range};

use dope_core::driver::life::recovery;
use dope_test::scope;
use o3::{
    buffer::storage::{self, range},
    mem::{self, rc},
};
use sark_core::{
    body::{
        self, Body, Discarded,
        admit::{self, Length, Retention, Stream},
    },
    request::{self, BindError, Head, Retained, View, frame::proof},
    route,
    route::field::{header, query},
};
use sark_http::{
    method::Method,
    payload::owned,
    request::{Error, Form, Id, Layout, Seq, Target, span},
};

mod segments;
pub use segments::Segments;

pub const WIRE: &[u8] = b"POST /items HTTP/1.1\r\n\r\nbody";
pub const HEAD_LEN: usize = WIRE.len() - 4;

pub const RAW: &[u8] = b"..a=1&&flag&n=x=y&=empty&tail=&bare&..";
pub const QUERIES: &[(usize, usize)] = &[(30, 5), (35, 6), (36, 6)];

pub const HEAD: &[u8] = b"GET / HTTP/1.1\r\nx-token: hi\r\n\r\n";

pub const ORIGIN_AUTHORITY: &[u8] = b"origin.example:443";
pub const GUARD_PATHS: [(Method, Form, &[u8]); 2] = [
    (Method::OPTIONS, Form::Asterisk, b"*"),
    (Method::CONNECT, Form::Extended, b"/tunnel"),
];

pub const FIELDS: &[[(&[u8], Range<usize>); 2]] = &[
    [(b"a", 2..3), (b"1", 4..5)],
    [(b"flag", 7..11), (b"", 11..11)],
    [(b"n", 12..13), (b"x=y", 14..17)],
    [(b"", 18..18), (b"empty", 19..24)],
    [(b"tail", 25..29), (b"", 30..30)],
    [(b"bare", 31..35), (b"", 35..35)],
];

pub fn ids<R>(
    run: impl for<'d> FnOnce(Seq<'d>),
    policy: impl recovery::Observe,
    finish: impl for<'owner> FnOnce(Result<(), scope::Error<'owner, ()>>) -> R,
) -> R {
    use std::num::NonZero;

    use dope_core::driver::{self, schedule::ready::task::owner, settings};
    use scope::Driver;

    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let owners = owner::Capacity::new(NonZero::<u32>::MIN);
            let binding = driver::Binding::new(driver.driver_ref().timer(), owners);
            run(Seq::new(binding.sequence()))
        },
        policy,
        finish,
    )
}

pub fn try_layout<'d>() -> Result<Layout, Build<'d>> {
    let span = span::Span::checked(5, 6).map_err(Build::Span)?;
    let target = Target::from_span(Form::Path, span).map_err(Build::Target)?;
    Ok(Layout::new(target, None, Some(span)))
}

pub fn try_retain<'d>(id: Id<'d>, request: storage::Shared) -> Result<Retained<'d>, Build<'d>> {
    let body = request
        .try_clone()
        .map_err(Build::Retain)?
        .project(HEAD_LEN..WIRE.len())
        .map_err(Build::Range)?;
    let head = request.project(0..HEAD_LEN).map_err(Build::Range)?;
    let head = Head::bind(head, try_layout()?).map_err(Build::Head)?;
    let track = Retention::Retained
        .plan(4)
        .try_admit(4, 4)
        .map_err(Build::Admission)?;
    let body = Body::try_new(track, owned::Segments::from(body)).map_err(Build::Body)?;
    Ok(Retained::from_head(id, head, body))
}

pub fn shorten<'short>(request: View<'static, 'static>, _: &'short ()) -> View<'short, 'static> {
    request
}

pub struct Schema;

impl route::Schema for Schema {
    type Header = header::Discard;
    type Query = query::Discard;
    type RawHeaders = ();
    type Error = Infallible;
    type RawParams = ();
    type Body = Discarded;
    type Frame = proof::Maybe;
}

#[derive(Debug)]
pub enum Build<'d> {
    Span(span::Error),
    Target(Error),
    Head(BindError),
    Retain(rc::Error),
    Range(mem::Rejected<storage::Shared, range::Error>),
    Admission(body::Error),
    Body(mem::Rejected<(admit::Track<admit::Finished>, owned::Segments<'d>), body::Error>),
}

impl Schema {
    pub fn try_parse(bytes: &[u8]) -> Result<request::raw::Parsed<Head<&[u8]>, Self>, Build<'_>> {
        let span = span::Span::checked(0, bytes.len()).map_err(Build::Span)?;
        let target = Target::from_span(Form::Path, span).map_err(Build::Target)?;
        let head = Head::bind(bytes, Layout::new(target, None, Some(span))).map_err(Build::Head)?;
        // SAFETY: this schema has no raw fields to associate with another head.
        Ok(unsafe { request::raw::Parsed::new(head, (), ()) })
    }
}

pub trait Plan {
    fn plan(self, len: usize) -> admit::Plan;
}

impl Plan for Retention {
    fn plan(self, len: usize) -> admit::Plan {
        admit::Plan::new(self, len, Length::Known(len), Stream::Buffered)
    }
}
