use std::{num, ops::ControlFlow};

use dope::{
    core::{
        driver::{retained, route::Identity},
        io::transfer,
    },
    manifold::{app::receive, network::listener::conn},
    net::{
        link::egress::{
            self,
            data::{self, compose},
        },
        wire,
    },
};
use o3::buffer::{self, hold, resident, resident::bound, write::consume};
use sark_core::duplex::{self, machine};
use wire::receive::mode::access;

use crate::{
    server::{conn::driver::Ingress, duplex::Runtime, payload},
    session, switch,
};

/// One listener turn borrowing its exact receive budget, connection, and driver.
pub struct Read<'a, 'r, 's, 'd, C, F> {
    conn: &'a mut C,
    driver: &'a mut retained::Context<'r, 's, 'd>,
    budget: &'a resident::Budget<'d>,
    http: F,
}

impl<'a, 'r, 's, 'd, C, F> Read<'a, 'r, 's, 'd, C, F>
where
    F: FnMut(&mut C, &[u8], &mut retained::Context<'r, 's, 'd>) -> Ingress,
{
    pub fn new(
        conn: &'a mut C,
        driver: &'a mut retained::Context<'r, 's, 'd>,
        budget: &'a resident::Budget<'d>,
        http: F,
    ) -> Self {
        Self {
            conn,
            driver,
            budget,
            http,
        }
    }
}

impl<'e, 'c, 'p, 's, 'r, 'o, 'w, 'd: 'w, ID: Identity, W, S, P, Ready, Heads, D, K, F>
    access::Use<'d, receive::Outcome>
    for Read<
        '_,
        'r,
        'o,
        'd,
        conn::Ctx<
            'e,
            'c,
            'p,
            's,
            'w,
            'd,
            ID,
            W,
            session::Conn<'d, S, P, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>,
            data::Bound<
                compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>,
                bound::Unbounded,
            >,
        >,
        F,
    >
where
    W: wire::Spec,
    D: duplex::Session<'d>,
    K: Copy + Default + 'd,
    F: FnMut(
        &mut conn::Ctx<
            'e,
            'c,
            'p,
            's,
            'w,
            'd,
            ID,
            W,
            session::Conn<'d, S, P, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>,
            data::Bound<
                compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>,
                bound::Unbounded,
            >,
        >,
        &[u8],
        &mut retained::Context<'r, 'o, 'd>,
    ) -> Ingress,
{
    fn apply<R>(&mut self, head: &mut R) -> ControlFlow<receive::Outcome, receive::Outcome>
    where
        R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume,
    {
        use hold::scope::Input;

        let flow = Input::consume(head, self.budget, |input| {
            let Some(count) = num::NonZero::new(input.as_slice().len()) else {
                return (None, ControlFlow::Break(receive::Outcome::Continue));
            };
            let (whole, end) = match input.try_prefix::<{ transfer::MAX_BYTES as u32 }>(count) {
                Ok(prefix) => prefix,
                Err(cause) => {
                    return (
                        None,
                        ControlFlow::Break(receive::Outcome::Failed(receive::Failure::Input(
                            cause,
                        ))),
                    );
                }
            };
            let outcome = match self.conn.state().mode() {
                switch::Mode::Raw(_) => Runtime::new(self.conn).chunk(whole, self.driver),
                switch::Mode::Switching(_) => {
                    return (None, ControlFlow::Break(receive::Outcome::Continue));
                }
                switch::Mode::Http => match (self.http)(self.conn, input.as_slice(), self.driver) {
                    Ingress::Continue => receive::Outcome::Continue,
                    Ingress::CloseAfter => receive::Outcome::CloseAfter,
                    Ingress::Handoff { staged: (), range } => {
                        let tail = match input.try_plan(range) {
                            Ok(tail) => tail,
                            Err(cause) => {
                                return (
                                    None,
                                    ControlFlow::Break(receive::Outcome::Failed(
                                        receive::Failure::Hold(hold::Error::Range(cause)),
                                    )),
                                );
                            }
                        };
                        Runtime::new(self.conn).handoff(tail)
                    }
                },
            };
            match outcome {
                receive::Outcome::Continue => {
                    (Some(end), ControlFlow::Continue(receive::Outcome::Continue))
                }
                failed @ (receive::Outcome::CloseAfter | receive::Outcome::Failed(_)) => {
                    (None, ControlFlow::Break(failed))
                }
            }
        });
        let ControlFlow::Continue(outcome) = flow else {
            return flow;
        };
        if self.conn.write().pending() == egress::Pending::Present {
            return ControlFlow::Break(outcome);
        }
        match self.conn.state().raw() {
            Ok(raw) => match raw.access() {
                machine::Access::Vacant(()) => ControlFlow::Continue(outcome),
                machine::Access::Blocked => ControlFlow::Break(outcome),
            },
            Err(switch::Mode::Http) => ControlFlow::Continue(outcome),
            Err(switch::Mode::Switching(_)) => ControlFlow::Break(outcome),
        }
    }
}
