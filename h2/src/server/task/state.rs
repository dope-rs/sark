use dope::manifold::network::listener::handler;
use o3::buffer::resident;
use sark_core::{
    body,
    decode::{request, state},
    duplex,
    manifold::schedule,
};
use sark_http::{field::metadata::section::rules, payload::compress::dynamic::hash};
use task::{book, registry};

use crate::{
    conn::{config, event, machine},
    server::{
        runtime::output::{self, flush::End},
        task,
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
    X,
    D: duplex::Session<'d>,
    E = (),
> {
    pub(super) carrier: machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    pub(super) book: book::Book<'d, P, G, F, B, X, D>,
    pub(super) task_index: registry::Index,
    pub(super) deadlines: E,
}

impl<'d, P, G, F, B, X, D, E> State<'d, P, G, F, B, X, D, E>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn try_new(
        config: config::Validated<crate::ServerRole>,
        timer: E,
        seed: hash::Seed,
    ) -> Result<Self, task::Build> {
        let domain = config.stream_count();
        let carrier =
            machine::Connection::from_seed(config, seed).map_err(task::Build::Connection)?;
        let book = book::Book::try_new(domain).map_err(task::Build::Book)?;
        Ok(Self {
            carrier,
            book,
            task_index: registry::Index::try_new(domain).map_err(task::Build::Tasks)?,
            deadlines: timer,
        })
    }
}

impl<'d, P, G, F, B, X, D, E> State<'d, P, G, F, B, X, D, E>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn activity(&self) -> schedule::Quiescence {
        if self.task_index.is_empty()
            && self.book.incoming.is_empty()
            && self.book.build_ready.is_empty()
            && self.book.responses.is_empty()
            && self.carrier.status().active_len() == 0
        {
            schedule::Quiescence::Quiet
        } else {
            schedule::Quiescence::Busy
        }
    }

    pub(super) fn finish_drain(&mut self) -> handler::Drain {
        let activity = self.activity();
        if matches!(
            self.carrier.control().finish(activity),
            machine::ops::Drain::Complete
        ) {
            return handler::Drain::Complete;
        }
        match self.carrier.status().is_closing() {
            true => handler::Drain::Complete,
            false => handler::Drain::Pending,
        }
    }

    pub(super) fn turn(&mut self) -> book::Turn<'_, 'd, P, G, F, B, X, D> {
        book::Turn::new(&mut self.carrier, &mut self.book)
    }
}

impl<'d, P, G, F, B, X, D, E> output::State<'d> for State<'d, P, G, F, B, X, D, E>
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
