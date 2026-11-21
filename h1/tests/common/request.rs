use std::{cell::Cell, convert::Infallible};

use request::frame::{self, proof};
use route::field::{header, query};
use sark_core::{body::Contiguous, manifold, request, route};
use sark_h1::route::{Mode, Policy, PolicyOf};
use sark_http::{
    h1::head::response::{Date, Server},
    payload::{
        fixed,
        response::{Static, dispatch, headers, status::Code},
    },
};

pub struct Request;

impl PolicyOf for Request {
    const POLICY: Policy = Policy::new(Mode::Plain, Date::Include, Server::Include);
}

impl Request {
    pub const HEAD: &[u8] =
        b"POST /retain?key=value HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\n";

    pub fn field_block(len: usize) -> Vec<u8> {
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
    type Error<'req> = frame::Error;
    type View<'req, 'd>
        = (&'req [u8], &'req [u8])
    where
        'd: 'req;
    fn reject(_: frame::Error) -> route::Reject {
        route::Reject::BadRequest
    }

    fn try_view<'req, 'body: 'req, 'd: 'body>(
        request: &request::raw::Parsed<request::Head<&'req [u8]>, Self>,
        body: sark_core::body::Scoped<'body, 'd, Contiguous>,
    ) -> Result<Self::View<'req, 'd>, frame::Error> {
        Ok((*request.owner().owner(), body.into_inner().as_slice()))
    }
}

impl route::Spec for Request {
    type Kind = dispatch::Sync;
    type Request = Self;
    type Stream<'d> = dispatch::Never;
}

impl manifold::Route<Cell<(*const u8, bool, bool)>> for Request {
    type Response<'req, 'd>
        = Static<'d, [headers::Entry<'req>; 0]>
    where
        'd: 'req;
    fn invoke<'req, 'd, 'state>(
        (headers, body): <Self::Request as route::Project>::View<'req, 'd>,
        state: &'state Cell<(*const u8, bool, bool)>,
    ) -> Self::Response<'req, 'd>
    where
        'req: 'state,
        'd: 'req,
    {
        state.set((headers.as_ptr(), headers == Self::HEAD, body == b"z"));
        Static::new(Code::OK, [], fixed::Body::new(b"ok"))
    }
}
