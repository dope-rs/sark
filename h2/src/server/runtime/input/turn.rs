use std::ops::ControlFlow;

use buffer::Source as _;
use dope::net::wire::{self, receive};
use o3::buffer::{
    self,
    hold::{self, scope},
    resident,
    write::consume,
};
use receive::{
    mode::{Disabled, access},
    scoped,
};
use sark_http::field::metadata::section::rules;

use crate::conn::{self, event, machine};

/// Decodes each cursor head while its original retention authority stays live.
pub(in crate::server) struct Turn<'input, 'w, 'd: 'w, W: wire::Spec> {
    source: Option<&'input mut receive::Chunk<'w, 'd, W, Disabled>>,
    budget: resident::Budget<'d>,
}

impl<'input, 'w, 'd: 'w, W: wire::Spec> Turn<'input, 'w, 'd, W> {
    pub(in crate::server) fn new(source: scoped::Input<'input, 'w, 'd, W>) -> Self {
        let (chunk, budget) = source.into_parts();
        Self {
            source: (!chunk.is_empty()).then_some(chunk),
            budget: *budget,
        }
    }

    pub(super) const fn budget(&self) -> resident::Budget<'d> {
        self.budget
    }

    pub(in crate::server) fn discard(&mut self) {
        let Some(chunk) = self.source.take() else {
            return;
        };
        chunk.drive((), &mut access::Discard);
    }

    pub(super) fn borrow<F: rules::Policy, E: event::Evidence>(
        &mut self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
    ) -> Result<(), conn::Error> {
        let Some(chunk) = self.source.take() else {
            return conn.ingress().resume();
        };
        let result = chunk.drive(
            Ok(()),
            &mut Borrow {
                conn,
                budget: &self.budget,
            },
        );
        if !chunk.is_empty() {
            self.source = Some(chunk);
        }
        result
    }
}

struct Borrow<'a, 'd, F: rules::Policy, E: event::Evidence> {
    conn: &'a mut machine::Connection<
        crate::ServerRole,
        F,
        resident::Shared<'d>,
        rules::RejectPush,
        E,
    >,
    budget: &'a resident::Budget<'d>,
}

impl<'d, F: rules::Policy, E: event::Evidence> access::Use<'d, Result<(), conn::Error>>
    for Borrow<'_, 'd, F, E>
{
    fn apply<R>(
        &mut self,
        head: &mut R,
    ) -> ControlFlow<Result<(), conn::Error>, Result<(), conn::Error>>
    where
        R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume,
    {
        let mut consumed = 0;
        let result = self
            .conn
            .ingress()
            .borrow_scope(&scope::Input::new(head, self.budget), &mut consumed);
        head.consume_max(consumed);
        if result.is_err() || !head.is_empty() {
            ControlFlow::Break(result)
        } else {
            ControlFlow::Continue(result)
        }
    }
}
