//! Production-topology HTTP/2 task execution tests.

use std::{net::SocketAddr, ops::ControlFlow, time::Duration};

use dope::manifold::app::Host;
use sark::{Guard, Head};
use sark_http::field::{Entry, Pseudo};

pub use super::server::recover;

pub mod apps;

pub mod facts;

pub mod server;

pub mod events;
pub mod peer;
pub mod tls;

mod session;
