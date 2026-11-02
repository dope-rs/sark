use std::{cell::Cell, convert::Infallible};

use dope_test::harness::observe::VecCellExt as _;
use sark_core::{
    body,
    request::{self, field, frame::proof},
    route::{
        self,
        field::{header, query},
    },
};
use sark_http::{
    field::metadata::{map::record, section::storage},
    payload::response::dispatch,
    request::body::Ref,
};

thread_local! {
    pub static SETS: Cell<Vec<Vec<u8>>> = const { Cell::new(Vec::new()) };
}

pub struct Request<const START: usize = 0>;

pub const PREFIX_HOST: &[u8] = b"HoSt: local\r\n\r\n";
pub const PREFIX_LENGTH: &[u8] = b"Content-Length: invalid\r\n\r\n";
pub const OWNED_PREFIX: &[u8] = b"GET / HTTP/1.1\r\nHost: local\r\nX-Next: value\r\nX-Count: on";
pub const OWNED_SUFFIX: &[u8] = b"ce\r\n\r\nTAIL";
pub const OWNED_TAIL: &[u8] = b"TAIL";
pub const CSV_FIELD: &[u8] =
    b"GET / HTTP/1.1\r\nHost: local\r\nAccept-Encoding: \t,gzip, \t\r\n\r\n";
pub const TRANSFER_CODINGS: &[(&[u8], bool)] = &[
    (b"chunked", true),
    (b"\tChUnKeD \t", true),
    (b"gzip, chunked", false),
    (b"identity, chunked", false),
    (b"custom; level=\"one,two\", chunked", false),
    (b"gzip\r\nTransfer-Encoding: chunked", false),
];

impl Request {
    pub const PREFIX: &[u8] = b"GET / HTTP/1.1\r\nHost: local\r\n";
    pub const LONG_FIELDS: [&[u8]; 2] = [b"X-Other: ", b"Connection: "];
    pub const TRANSFER_PREFIX: &[u8] = b"POST / HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: ";

    pub fn transfer(coding: &[u8]) -> Vec<u8> {
        let mut input = Self::TRANSFER_PREFIX.to_vec();
        input.extend_from_slice(coding);
        input.extend_from_slice(b"\r\n\r\n");
        input
    }

    pub fn long(width: usize, field: &[u8]) -> Vec<u8> {
        let mut input = Self::PREFIX.to_vec();
        input.extend_from_slice(field);
        input.resize(input.len() + width - field.len(), b'a');
        input.extend_from_slice(b"\r\n\r\n");
        input
    }
}

#[derive(Clone, Copy)]
pub struct Header;

impl header::Slot for Header {
    fn into_tag(self) -> storage::Slot {
        storage::Slot::exact::<0>()
    }
    fn from_tag(tag: storage::Slot) -> Result<Self, header::Tag> {
        if tag.get() == 0 {
            Ok(Self)
        } else {
            Err(header::Tag::new(tag.get() as u16))
        }
    }
}

impl<const START: usize> header::Policy<Request<START>> for Header {
    fn prefix<P: field::Prefix<Self>>(prefix: P) -> P::Output {
        if prefix
            .rest()
            .get(..16)
            .is_some_and(|name| name.eq_ignore_ascii_case(b"accept-encoding:"))
        {
            return prefix.user(Self, 15);
        }
        match prefix.rest().get(..8) {
            Some(name)
                if name.eq_ignore_ascii_case(b"x-count:")
                    || name.eq_ignore_ascii_case(b"upgrade:") =>
            {
                prefix.user(Self, 7)
            }
            Some(_) => prefix.miss(START),
            None => prefix.short(),
        }
    }
}

impl<const START: usize> header::Fields<Request<START>> for Header {
    fn slice(name: &[u8]) -> Option<Self> {
        (name.eq_ignore_ascii_case(b"x-count")
            || name.eq_ignore_ascii_case(b"upgrade")
            || name.eq_ignore_ascii_case(b"accept-encoding"))
        .then_some(Self)
    }
    fn span(name: record::Span<'_>) -> Option<Self> {
        (name.is_equal(b"x-count")
            || name.is_equal(b"upgrade")
            || name.is_equal(b"accept-encoding"))
        .then_some(Self)
    }
    fn set<V: field::HeaderValue>(_: &mut (), _: Self, value: &V) -> Result<(), Infallible> {
        SETS.with(|values| values.push(value.as_ref().to_vec()));
        Ok(())
    }
}

impl<const START: usize> route::Schema for Request<START> {
    type Header = Header;
    type Query = query::Discard;
    type RawHeaders = ();
    type Error = Infallible;
    type RawParams = ();
    type Body = body::Discarded;
    type Frame = proof::Maybe;
}

impl<const START: usize> route::Project for Request<START> {
    type Params<'req> = ();
    type Headers<'req> = ();
    type Error<'req> = Infallible;
    type View<'req, 'd>
        = usize
    where
        'd: 'req;
    type ParsedBody<'req, 'd>
        = usize
    where
        'd: 'req;
    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
    fn from_params<'req, 'body>(
        _: &request::raw::Parsed<request::Head<&'req [u8], Self::Frame>, Self>,
    ) -> Result<Self::Params<'req>, <Self as route::Project>::Error<'body>> {
        Ok(())
    }
    fn from_headers<'req, 'body>(
        _: &request::raw::Parsed<request::Head<&'req [u8], Self::Frame>, Self>,
    ) -> Result<Self::Headers<'req>, <Self as route::Project>::Error<'body>> {
        Ok(())
    }
    fn parse_body<'req, 'd: 'req>(body: body::Len) -> Result<usize, Infallible> {
        Ok(Ref::from(body).len())
    }
    fn from_parts<'req, 'd: 'req>(_: (), _: (), body: usize, _: &request::View<'req, 'd>) -> usize {
        body
    }
}

impl<const START: usize> route::Spec for Request<START> {
    type Kind = dispatch::Sync;
    type Request = Self;
    type Stream<'d> = dispatch::Never;
}
