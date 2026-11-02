use crate::common::h1::fields;
pub mod common;

use sark_core::{
    request::{self, field, raw},
    route::{
        self, Project, Schema,
        field::query::{self, Fields as _},
    },
};
use sark_fiber::request::Head;
use sark_http::request::{Form, Layout, body, span::Span};

use crate::common::route::glue::{
    Ordered, OrderedHeadersRaw, QueryReq, QueryReqHeadersRaw, QueryReqIdentity,
};

#[test]
fn parses_unordered_query() {
    let input = fields::UNORDERED;
    let mut headers = QueryReqHeadersRaw::default();

    <QueryReq as Schema>::Query::parse(query::Input::new(&mut headers, input, 2..input.len()))
        .expect("valid unordered query");
    let layout = Layout::try_new(
        Form::Path,
        Span::checked(0, input.len()).expect("query target span"),
        None,
        Some(Span::checked(0, 1).expect("query route span")),
    )
    .expect("query layout");
    let owner = Head::bind(input.as_slice(), layout).expect("query owner");
    // SAFETY: QueryReq parsed these fields from this exact input;
    // its generated parameter schema is the empty tuple.
    let raw = unsafe { raw::Parsed::<_, QueryReq>::new(owner, (), headers) };
    let headers = <QueryReq as Project>::from_headers(&raw).expect("query headers");
    let view = request::View::from_head(raw.into_owner(), body::Ref::from_slice(&[]));
    let request = <QueryReq as Project>::from_parts((), headers, (), &view);
    assert_eq!((request.count, request.limit, request.flag), (41, 7, true));
}

#[test]
fn parses_ordered_query() {
    let input = fields::ORDERED;
    let mut headers = OrderedHeadersRaw::default();

    <Ordered as Schema>::Query::parse(query::Input::new(&mut headers, input, 2..input.len()))
        .expect("valid ordered query");
    let layout = Layout::try_new(
        Form::Path,
        Span::checked(0, input.len()).expect("query target span"),
        None,
        Some(Span::checked(0, 1).expect("query route span")),
    )
    .expect("query layout");
    let owner = Head::bind(input.as_slice(), layout).expect("query owner");
    // SAFETY: Ordered parsed these fields from this exact input;
    // its generated parameter schema is the empty tuple.
    let raw = unsafe { raw::Parsed::<_, Ordered>::new(owner, (), headers) };
    let headers = <Ordered as Project>::from_headers(&raw).expect("query headers");
    let view = request::View::from_head(raw.into_owner(), body::Ref::from_slice(&[]));
    let request = <Ordered as Project>::from_parts((), headers, (), &view);
    assert_eq!((request.count, request.limit), (41, 7));
}

#[test]
fn matches_query_names() {
    let input = fields::QUERY_PREFIX;
    let mut headers = OrderedHeadersRaw::default();

    let result =
        <Ordered as Schema>::Query::parse(query::Input::new(&mut headers, input, 0..input.len()));

    assert!(result.is_err(), "a field-name prefix must not be accepted");
}

#[test]
fn rejects_bad_query_range() {
    let input = fields::QUERY;
    let mut headers = QueryReqHeadersRaw::default();

    let result = <QueryReq as Schema>::Query::parse(query::Input::new(
        &mut headers,
        input,
        0..input.len() + 1,
    ));

    assert_eq!(
        result,
        Err(route::Error::QueryBounds {
            start: 0,
            end: input.len() + 1,
            capacity: input.len(),
        }),
    );
}

#[test]
fn query_preserves_error() {
    let input = fields::BAD_QUERY;
    let mut headers = QueryReqHeadersRaw::default();

    assert_eq!(
        <QueryReq as Schema>::Query::parse(query::Input::new(&mut headers, input, 6..input.len(),)),
        Err(route::Error::Query {
            field: QueryReqIdentity::Query2,
            cause: field::Error::Boolean { start: 20, end: 23 },
        }),
    );
}
