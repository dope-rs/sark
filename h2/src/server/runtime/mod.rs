//! Typed HTTP/2 listener topology.

use crate::server::app::Spec;
use dope::{
    manifold::network::listener::conn::{Ctx, Id},
    net::link::egress::data,
};
use o3::buffer::resident;
use sark_http::field::metadata::section::rules;

use crate::conn::{self, event, machine};

#[doc(hidden)]
pub mod app;
pub(in crate::server) mod input;
mod local;
#[doc(hidden)]
pub mod modes;
pub mod output;
mod plan;
mod recipe;
pub mod startup;

pub use local::Local;
pub use plan::{
    Plan,
    tasks::{Prepared, Tasks},
};
pub use recipe::Recipe;

pub type Connection<'d, A, E = event::Ignore> = machine::Connection<
    crate::ServerRole,
    <A as Spec>::Fields,
    resident::Shared<'d>,
    rules::RejectPush,
    E,
>;

type Context<'e, 'a, 'p, 's, 'w, 'd, ID, A, W, E> = Ctx<
    'e,
    'a,
    'p,
    's,
    'w,
    'd,
    ID,
    W,
    app::State<'d, A, Id<'d, ID>>,
    data::Bound<E, <A as Spec>::Egress<'d>>,
>;

#[derive(Debug, Eq, PartialEq)]
pub enum Build<E> {
    Connection(conn::Build),
    App(E),
}
