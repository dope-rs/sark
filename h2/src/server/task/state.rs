use dope::manifold::network::listener::handler;
use o3::buffer::resident;
use sark_core::{
    body,
    decode::{request, state},
    duplex,
    manifold::schedule,
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{event, machine},
    server::{
        runtime::output::{self, flush::End},
        task::{book, registry},
    },
};

/// Per-connection protocol and execution state.
#[doc(hidden)]
pub struct State<
    'd,
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
    X,
    E = (),
    S = (),
> {
    pub(super) carrier: machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    pub(super) book: book::Book<'d, P, G, B, D, X>,
    pub(super) task_index: registry::Index,
    pub(super) deadlines: E,
    pub(super) service: S,
}

impl<'d, P, G, F, B, D, X, E, S> State<'d, P, G, F, B, D, X, E, S>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn activity(&self, service: schedule::Quiescence) -> schedule::Quiescence {
        if self.task_index.is_empty()
            && self.book.incoming.is_empty()
            && self.book.responses.is_empty()
            && self.carrier.status().active_len() == 0
            && matches!(service, schedule::Quiescence::Quiet)
        {
            schedule::Quiescence::Quiet
        } else {
            schedule::Quiescence::Busy
        }
    }

    pub(super) fn finish_drain(&mut self, service: schedule::Quiescence) -> handler::Drain {
        if self.carrier.status().is_draining() {
            let activity = self.activity(service);
            self.carrier.control().finish(activity);
        }
        match self.carrier.status().is_closing() {
            true => handler::Drain::Complete,
            false => handler::Drain::Pending,
        }
    }

    pub(super) fn turn(&mut self) -> book::Turn<'_, 'd, P, G, F, B, D, X> {
        book::Turn::new(&mut self.carrier, &mut self.book)
    }
}

impl<'d, P, G, F, B, D, X, E, S> output::State<'d> for State<'d, P, G, F, B, D, X, E, S>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    type Fields = F;
    type Evidence = event::Retirement;

    fn protocol(&mut self) -> &mut machine::Connection<crate::ServerRole, F, resident::Shared<'d>> {
        &mut self.carrier
    }

    fn settle(&mut self) -> End {
        if self.carrier.status().is_closing() {
            End::Close
        } else {
            End::Keep
        }
    }
}
