//! Per-request policies, buffers, and transaction state.

use std::{mem, ops, time};

use o3::{
    alloc,
    buffer::{pool, write::Sink as _},
};
use sark_client::{
    Error, Failure, RequestError, capacity,
    protocol::{redirect, retry},
};
use sark_http::{
    field,
    h1::codec::numbers::Wire as _,
    method,
    payload::response::{Method, status},
    uri::target,
};

use crate::client::config;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::client) enum Kind {
    Other,
    Head,
}

impl Kind {
    fn from_request(request: &Admitted<'_>) -> Self {
        if *request.method() == method::Method::HEAD {
            Self::Head
        } else {
            Self::Other
        }
    }

    pub(in crate::client) const fn transition(self) -> Method {
        match self {
            Self::Other => Method::Other,
            Self::Head => Method::Head,
        }
    }
}

/// An HTTP message request whose method cannot switch the connection to a tunnel.
#[repr(transparent)]
pub(in crate::client) struct Admitted<'request>(redirect::Machine<'request>);

impl<'request> Admitted<'request> {
    pub(in crate::client) fn try_new(
        follows: u32,
        origin: &'request target::Target,
        path: &'request str,
        method: method::Method,
        body: &'request [u8],
    ) -> Result<Self, Error> {
        if method == method::Method::CONNECT {
            return Err(Error::Request(RequestError::UnsupportedTransition));
        }
        redirect::Machine::new(follows, origin, path, method, body).map(Self)
    }

    pub(in crate::client) fn response<'head>(
        self,
        status: status::Code,
        location: impl FnOnce() -> Option<&'head [u8]>,
    ) -> Result<Option<Self>, Failure> {
        Ok(match self.0.response(status, location)? {
            redirect::Action::Complete => None,
            redirect::Action::Follow(request) => Some(Self(request)),
        })
    }
}

impl<'request> ops::Deref for Admitted<'request> {
    type Target = redirect::Machine<'request>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(in crate::client) struct Encoded {
    wire: pool::Cursor,
    kind: Kind,
}

impl Encoded {
    pub(in crate::client) fn into_parts(self) -> (pool::Cursor, Kind) {
        (self.wire, self.kind)
    }
}

/// Proof that a borrowed request-header slice has passed the shared HTTP
/// field grammar and this client's reserved-name policy.
#[derive(Clone, Copy)]
pub(in crate::client) struct Headers<'headers> {
    fields: &'headers [(&'headers str, &'headers str)],
}

#[derive(Clone, Copy)]
pub(in crate::client) struct Policy {
    pub(in crate::client) max_redirects: u32,
    pub(in crate::client) retry: retry::Policy,
    pub(in crate::client) timeout: time::Duration,
    pub(in crate::client) dispatch_timeout: time::Duration,
    pub(in crate::client) head_timeout: time::Duration,
    pub(in crate::client) decode_timeout: time::Duration,
}

pub(in crate::client) struct Origin {
    origin: target::Plan,
}

pub(in crate::client) struct Plan {
    pub(in crate::client) buffers: pool::Layout,
    pub(in crate::client) origin: Origin,
    pub(in crate::client) policy: Policy,
}

pub(in crate::client) struct Request {
    buffers: pool::Pool,
    origin: target::Target,
    policy: Policy,
}

impl Origin {
    pub(in crate::client) fn new(host: String) -> Result<Self, config::Error> {
        if host.contains('@') {
            return Err(config::Error::HostUserInfo);
        }
        let http = host
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"));
        let https = host
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"));
        if http || https {
            let origin = target::Plan::parse(host).map_err(config::Error::HostUri)?;
            if origin.path() != "/" {
                return Err(config::Error::HostPath);
            }
            return Ok(Self { origin });
        }
        let origin = target::Plan::http(host).map_err(config::Error::HostAuthority)?;
        Ok(Self { origin })
    }
}

impl Request {
    pub(in crate::client) fn try_new(plan: Plan) -> Result<Self, alloc::Exhausted> {
        Ok(Self {
            buffers: pool::Pool::try_from(plan.buffers)?,
            origin: plan.origin.origin.try_build()?,
            policy: plan.policy,
        })
    }

    pub(in crate::client) fn host(&self) -> &str {
        self.origin.authority()
    }

    pub(in crate::client) fn origin(&self) -> &target::Target {
        &self.origin
    }

    pub(in crate::client) fn redirects(&self) -> u32 {
        self.policy.max_redirects
    }

    pub(in crate::client) fn retry(&self) -> retry::Policy {
        self.policy.retry
    }

    pub(in crate::client) fn timeout(&self) -> time::Duration {
        self.policy.timeout
    }

    pub(in crate::client) fn dispatch(&self) -> time::Duration {
        self.policy.dispatch_timeout
    }

    pub(in crate::client) fn head_timeout(&self) -> time::Duration {
        self.policy.head_timeout
    }

    pub(in crate::client) fn decode(&self) -> time::Duration {
        self.policy.decode_timeout
    }

    pub(in crate::client) fn admit<'headers>(
        &self,
        headers: &'headers [(&'headers str, &'headers str)],
    ) -> Result<Headers<'headers>, Error> {
        for (name, value) in headers {
            if let Err(error) = field::Error::name(name.as_bytes()) {
                return Err(Error::Request(RequestError::HeaderName(error)));
            }
            if let Err(error) = field::Error::value(value.as_bytes()) {
                return Err(Error::Request(RequestError::HeaderValue(error)));
            }
            if is_reserved(name) {
                return Err(Error::Request(RequestError::ReservedHeader));
            }
        }
        Ok(Headers { fields: headers })
    }

    pub(in crate::client) fn encode(
        &self,
        request: &Admitted<'_>,
        headers: Headers<'_>,
    ) -> Result<Encoded, Error> {
        self.encode_for(self.host(), request, headers, true)
    }

    pub(in crate::client) fn encode_for(
        &self,
        host: &str,
        request: &Admitted<'_>,
        headers: Headers<'_>,
        is_base: bool,
    ) -> Result<Encoded, Error> {
        let body = request.body();
        if !is_base && (!headers.fields.is_empty() || !body.is_empty()) {
            return Err(Error::Request(RequestError::CrossOrigin));
        }
        let kind = Kind::from_request(request);
        let mut buffer = self
            .buffers
            .cursor()
            .ok_or(Error::Capacity(capacity::Error::RequestBuffer))?;
        let initial: [&[u8]; 6] = [
            request.method().as_str().as_bytes(),
            b" ",
            redirect::Target::origin_form(&request.0).as_bytes(),
            b" HTTP/1.1\r\nHost: ",
            host.as_bytes(),
            b"\r\nConnection: keep-alive\r\nAccept: \x2a/\x2a\r\n",
        ];
        buffer
            .write_slices(initial)
            .map_err(capacity::Error::RequestBytes)
            .map_err(Error::Capacity)?;
        for &(name, value) in headers.fields {
            if !request.preserves_body() && is_content(name) {
                continue;
            }
            buffer
                .write_slices([name.as_bytes(), b": ", value.as_bytes(), b"\r\n"])
                .map_err(capacity::Error::RequestBytes)
                .map_err(Error::Capacity)?;
        }
        if !body.is_empty() {
            let mut digits = [std::mem::MaybeUninit::uninit(); 20];
            buffer
                .write_slices([
                    b"Content-Length: ",
                    body.len().decimal_uninit(&mut digits),
                    b"\r\n",
                ])
                .map_err(capacity::Error::RequestBytes)
                .map_err(Error::Capacity)?;
        }
        buffer
            .write_slices([b"\r\n", body])
            .map_err(capacity::Error::RequestBytes)
            .map_err(Error::Capacity)?;
        Ok(Encoded { wire: buffer, kind })
    }
}

fn is_reserved(name: &str) -> bool {
    name.eq_ignore_ascii_case("host")
        || name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("transfer-encoding")
}

fn is_content(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-encoding")
        || name.eq_ignore_ascii_case("content-language")
        || name.eq_ignore_ascii_case("content-location")
        || name.eq_ignore_ascii_case("content-type")
        || name.eq_ignore_ascii_case("content-range")
        || name.eq_ignore_ascii_case("content-digest")
        || name.eq_ignore_ascii_case("repr-digest")
        || name.eq_ignore_ascii_case("digest")
        || name.eq_ignore_ascii_case("last-modified")
}

const _: () = assert!(
    mem::size_of::<Headers<'static>>() == mem::size_of::<&'static [(&'static str, &'static str)]>()
);
const _: () = assert!(mem::size_of::<Kind>() == 1);
const _: () = assert!(
    mem::size_of::<Encoded>() <= mem::size_of::<pool::Cursor>() + mem::align_of::<pool::Cursor>()
);
