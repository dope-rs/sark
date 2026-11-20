use std::cell::Cell;

use dope::{
    core::{BranchError, driver::settings::errors},
    runtime::shutdown::ReadySource,
};
use dope_tls::tls::endpoint;
use sark_fiber::server::Serve as _;
use sark_h1::server::run::prepare;
use sark_http::payload::response::status;

use crate::common::{client::tls::Keys, server};

const SEED: [u8; 32] = [7; 32];
pub const BIG_LEN: usize = 512 * 1024;

#[sark_fiber::response(raw)]
pub struct Reply {
    status: status::Code,
    body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
pub(crate) fn hello() -> Reply {
    Reply {
        status: status::Code::OK,
        body: b"hello".to_vec(),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn big() -> Reply {
    let mut body = Vec::new();
    body.resize(BIG_LEN, b'x');
    Reply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub Routes<'state>: server::State<'state> => {
        GET "/hello" => hello,
        POST "/hello" => hello,
        GET "/big" => big,
    }
}

#[derive(Clone, Copy)]
pub enum Mode {
    Plain,
    Tls,
}

pub fn tls() -> Keys {
    Keys::new(&SEED)
}

pub fn serve<const TLS: bool>(
    plan: server::Http1Plan,
    ready: ReadySource<'_>,
) -> Result<(), BranchError<prepare::Error, errors::Claim>> {
    let mut reports = server::Plan::try_reports(&plan).map_err(BranchError::Second)?;
    let failures = Cell::new(Vec::new());
    let state = server::State(&failures);
    let config = sark_fiber::Config::new(server::MAX_CONNECTIONS);
    let report = server::reporting(&mut reports);
    if TLS {
        let wire = endpoint::Config::from(tls().server().expect("TLS server config"));
        Routes::serve_with(plan, wire, ready, state, config, report, |outcome| {
            server::completed(&outcome)
        })?;
    } else {
        Routes::serve(plan, ready, state, config, report, |outcome| {
            server::completed(&outcome)
        })?;
    }
    let failures = failures.into_inner();
    assert!(failures.is_empty(), "TLS failures: {failures:?}");
    server::reclaimed(&reports);
    Ok(())
}

pub mod resource;
