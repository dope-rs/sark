//! Production-topology HTTP/2 task execution tests.

use std::{net::SocketAddr, ops::ControlFlow, time::Duration};

use dope::{core::driver::life::recovery::Recovery, manifold::app::Host};
use sark::{Guard, Head};
use sark_http::field::{Entry, Pseudo};

pub fn recover(failure: Recovery<'_, '_>) -> std::convert::Infallible {
    panic!("native H2 failure: {:?}", failure.cause())
}

pub mod apps;

pub mod facts;

pub mod server;

pub mod events;
pub mod peer;

mod session;
