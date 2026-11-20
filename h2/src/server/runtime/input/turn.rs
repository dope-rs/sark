use std::ops::ControlFlow;

use dope::{
    manifold::app::receive,
    net::wire::{self, receive::scoped},
};
use o3::buffer::{resident, write::consume::scope};
use sark_http::field::metadata::section::rules;

use crate::conn::{self, event, machine};

pub(in crate::server) struct Turn<'s, 'w, 'd: 'w, W: wire::Spec + 'w> {
    head: Result<Option<scoped::Head<'s, 'w, 'd, W>>, scope::Error>,
    pub(super) budget: resident::Budget<'d>,
}

impl<'s, 'w, 'd: 'w, W: wire::Spec + 'w> Turn<'s, 'w, 'd, W> {
    pub(in crate::server) fn new(input: scoped::Input<'s, 'w, 'd, W>) -> Self {
        Self {
            budget: *input.budget(),
            head: Ok(input.head()),
        }
    }

    pub(in crate::server) fn discard(&mut self) {
        if let Ok(head) = &mut self.head {
            *head = None;
        }
    }

    pub(in crate::server) fn is_failed(&self) -> bool {
        self.head.is_err()
    }

    pub(in crate::server) fn settle(self) -> Result<receive::Outcome, scoped::Pending<'s>> {
        match self.head {
            Ok(Some(head)) => Err(head.defer()),
            Ok(None) => Ok(receive::Outcome::Continue),
            Err(error) => Ok(receive::Outcome::Failed(receive::Failure::Input(error))),
        }
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
    ) -> ControlFlow<(), Result<(), conn::Error>> {
        let Ok(current) = &mut self.head else {
            return ControlFlow::Break(());
        };
        let Some(mut head) = current.take() else {
            return ControlFlow::Continue(conn.ingress().resume());
        };
        loop {
            let mut consumed = 0;
            let result = conn.ingress().borrow_scope(&head.input(), &mut consumed);
            let rest = match &result {
                Ok(()) => head.finish(),
                Err(error) if !error.is_pressure() => return ControlFlow::Continue(result),
                Err(_) if consumed == 0 => {
                    self.head = Ok(Some(head));
                    return ControlFlow::Continue(result);
                }
                Err(_) => head.consume_max(consumed),
            };
            let next = match rest {
                Ok(input) => input.head(),
                Err(error) => {
                    self.head = Err(error);
                    return ControlFlow::Break(());
                }
            };
            match (result, next) {
                (Ok(()), Some(next)) => head = next,
                (result, next) => {
                    self.head = Ok(next);
                    return ControlFlow::Continue(result);
                }
            }
        }
    }
}
