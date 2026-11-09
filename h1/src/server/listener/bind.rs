//! Linear H1 application binding after listener validation.

use dope::{
    core::{driver, driver::route::Identity},
    manifold::network::listener::handler,
    net::{link::egress::data, wire},
};
use o3::alloc;
use sark_core::duplex;

use crate::server::{
    date,
    deadline::{self, policies},
};

#[derive(Clone, Copy, Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Deadline(deadline::BindError),
}

/// A cold generated application that becomes runnable for one exact driver.
pub trait Bind<'w, 'd: 'w, ID: Identity>: Sized {
    type Wire: wire::Spec;
    type Input;
    type Egress: data::raw::Payload<'d> + 'd;
    type Error;
    type Application<'c, D>: handler::Application<'d, ID, Wire = Self::Wire, Input = Self::Input, Egress = Self::Egress>
        + handler::Runtime<'w, 'd, ID>
        + duplex::Application<'d>
    where
        'd: 'c,
        D: policies::Policy<'d, ID>;

    fn try_bind<'c, D>(
        self,
        binding: driver::Binding<'d>,
        deadline: D,
        clock: &'c date::Clock,
    ) -> Result<Self::Application<'c, D>, Self::Error>
    where
        'd: 'c,
        D: policies::Policy<'d, ID>;
}
