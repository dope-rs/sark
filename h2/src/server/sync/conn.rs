use o3::buffer::resident;
use sark_core::{decode, manifold::schedule};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{
        event,
        machine::{self, ops, view},
    },
    server::sync::{self, Book, Dispatch, Endpoint},
};

/// Direct synchronous connection tied to its router and validated limits.
///
/// The endpoint borrow prevents a connection from outliving that authority.
///
///
/// The retained payload domain cannot be widened after construction.
///
pub struct Conn<'endpoint, 'd: 'endpoint, R: decode::Decode> {
    endpoint: &'endpoint Endpoint<R>,
    machine: machine::Connection<
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    >,
    book: Book<'d, R>,
}

impl<'endpoint, 'd: 'endpoint, R> Conn<'endpoint, 'd, R>
where
    R: decode::Decode,
{
    pub fn ingress(
        &mut self,
    ) -> ops::Ingress<
        '_,
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    > {
        self.machine.ingress()
    }

    pub fn transfer(
        &mut self,
    ) -> ops::Transfer<
        '_,
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    > {
        self.machine.transfer()
    }

    pub fn output(
        &self,
    ) -> view::Output<
        '_,
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    > {
        self.machine.output()
    }

    pub fn status(
        &self,
    ) -> view::Status<
        '_,
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    > {
        self.machine.status()
    }
}

impl<'endpoint, 'd: 'endpoint, R> Conn<'endpoint, 'd, R>
where
    R: Dispatch<'d>,
{
    pub fn try_new(endpoint: &'endpoint Endpoint<R>) -> Result<Self, sync::Create> {
        let config = endpoint.machine();
        let capacity = config.stream_count();
        let machine = machine::Connection::from_seed(config, endpoint.seed())
            .map_err(sync::Create::Connection)?;
        Ok(Self {
            endpoint,
            machine,
            book: Book::try_new(capacity).map_err(sync::Create::Book)?,
        })
    }

    pub fn drain_events(&mut self) {
        self.endpoint
            .router()
            .events(&mut self.book, &mut self.machine, self.endpoint.limits())
    }

    pub fn pump(&mut self) -> schedule::Report {
        self.endpoint.router().pump::<false, _>(
            &mut self.book,
            &mut self.machine,
            self.endpoint.limits(),
            schedule::Egress::Released,
            || Some(()),
        )
    }

    pub fn close(&mut self) {
        self.endpoint
            .router()
            .clear(&mut self.book, &mut self.machine);
    }
}
