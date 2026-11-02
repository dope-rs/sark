use std::{cell::Cell, convert::Infallible};

use request::frame::{self, proof};
use route::field::{header, query};
use sark_core::{body::Contiguous, manifold, request, route};
use sark_h1::route::{Cache, Policy, PolicyOf};
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    payload::{
        fixed,
        response::{dispatch, headers, statics, statics::Direct, status::Code},
    },
    request::body,
};

pub struct Request;

impl PolicyOf for Request {
    const POLICY: Policy =
        Policy::new(Cache::Bypass, Coding::Plain, Date::Include, Server::Include);
}

impl Request {
    pub const HEAD: &[u8] =
        b"POST /retain?key=value HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\n";

    pub fn field_block(oversize: bool) -> Vec<u8> {
        let len = 64 * 1024 + usize::from(oversize);
        let mut wire = b"Host: local\r\n".to_vec();
        while len - wire.len() > 2 {
            let value = (len - wire.len() - 10).min(1024);
            wire.extend_from_slice(b"x-pad:");
            wire.resize(wire.len() + value, b'x');
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"\r\n");
        wire
    }
}

impl route::Schema for Request {
    type Header = header::Discard;
    type Query = query::Discard;
    type RawHeaders = ();
    type Error = Infallible;
    type RawParams = ();
    type Body = Contiguous;
    type Frame = proof::Maybe;
}

impl route::Project for Request {
    type Params<'req> = ();
    type Headers<'req> = &'req [u8];
    type Error<'req> = frame::Error;
    type View<'req, 'd>
        = (&'req [u8], &'req [u8])
    where
        'd: 'req;
    type ParsedBody<'req, 'd>
        = &'req [u8]
    where
        'd: 'req;

    fn reject(_: frame::Error) -> route::Reject {
        route::Reject::BadRequest
    }

    fn from_params<'body>(
        _: &request::raw::Parsed<request::Head<&[u8]>, Self>,
    ) -> Result<(), <Self as route::Project>::Error<'body>> {
        Ok(())
    }

    fn parse_body<'req, 'd: 'req>(body: body::View<'req, 'd>) -> Result<&'req [u8], frame::Error> {
        Ok(body.as_slice())
    }

    fn from_parts<'req, 'd: 'req>(
        _: (),
        headers: &'req [u8],
        body: &'req [u8],
        _: &request::View<'req, 'd>,
    ) -> Self::View<'req, 'd> {
        (headers, body)
    }

    fn from_headers<'req, 'body>(
        request: &request::raw::Parsed<request::Head<&'req [u8]>, Self>,
    ) -> Result<&'req [u8], <Self as route::Project>::Error<'body>> {
        Ok(*request.owner().owner())
    }
}

impl route::Spec for Request {
    type Kind = dispatch::Sync;
    type Request = Self;
    type Stream<'d> = dispatch::Never;
}

impl manifold::Route<Cell<(*const u8, bool, bool)>> for Request {
    type Response<'req, 'd>
        = statics::Direct<'d, [headers::Entry<'req>; 0]>
    where
        'd: 'req;
    fn invoke<'req, 'd, 'state>(
        (_, _, headers, body, state): manifold::Args<
            '_,
            'req,
            'd,
            'state,
            Self::Request,
            Cell<(*const u8, bool, bool)>,
        >,
    ) -> Self::Response<'req, 'd>
    where
        'req: 'state,
        'd: 'req,
    {
        state.set((headers.as_ptr(), headers == Self::HEAD, body == b"z"));
        Direct::new(Code::OK, [], fixed::Body::new(b"ok"))
    }
}
