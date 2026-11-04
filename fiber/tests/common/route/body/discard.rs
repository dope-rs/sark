use std::{convert::Infallible, time::Duration};

use dope::{
    core::{
        BranchError,
        driver::{life::recovery, settings::errors},
        io::socket::Listener,
    },
    manifold::BuildError,
    runtime::shutdown,
};
use dope_test::reclaim;
use sark_fiber::{Config, request::BodyLen};
use sark_h1::server::{
    listener::bind,
    run::{self, prepare},
};
use sark_http::payload::response::status;

use crate::common::server;

#[derive(Debug)]
pub enum Error {
    Claim(errors::Claim),
    Prepare(BranchError<prepare::Error, errors::Claim>),
}

pub fn try_serve(
    context: shutdown::ReadySource,
    listener: Listener,
    recover: impl recovery::Recover,
    finish: impl for<'a> FnOnce(
        Result<(), run::RunCause<'a, Infallible, BranchError<Infallible, BuildError<bind::Error>>>>,
    ),
) -> Result<reclaim::Reports<1>, Error> {
    let server = server::Plan::new(listener.local_addr(), Duration::from_secs(10)).build();
    let mut reports = server::Plan::try_reports(&server).map_err(Error::Claim)?;
    StreamDiscardDispatch::serve(
        server.with_bound(listener),
        context,
        (),
        Config::new(server::MAX_CONNECTIONS),
        recovery::Policy::new(|event| reports.push(event), recover),
        |outcome| finish(outcome.as_ref().map(|_| ()).map_err(|error| error.cause())),
    )
    .map_err(Error::Prepare)?;
    Ok(reports)
}

pub const PING: &[u8] = b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n";

#[sark_fiber::response(raw)]
pub struct PingReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
fn ping() -> PingReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"pong");
    PingReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::request]
pub struct UpReq {
    #[body_len]
    pub payload: BodyLen,
}

#[sark_fiber::response(raw)]
pub struct UpReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
#[max_body(16 * 1024 * 1024)]
fn up(req: UpReq) -> UpReply {
    let mut body = Vec::new();
    body.extend_from_slice(req.payload.len().to_string().as_bytes());
    UpReply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub StreamDiscardDispatch: () => {
        GET "/ping" => ping,
        POST "/up" => up,
    }
}

impl UpReq<'_> {
    pub fn head(total: usize) -> String {
        format!("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {total}\r\n\r\n")
    }

    /// One counted upload whose header can be fragmented independently of its payload.
    pub fn wire(total: usize) -> Vec<u8> {
        let mut wire = Self::head(total).into_bytes();
        wire.resize(wire.len() + total, 0xa5);
        wire
    }
}
