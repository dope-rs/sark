use std::pin;

use dope::{
    core::{driver, driver::route::Identity},
    manifold::network::listener::handler,
    net::wire,
};
use driver::retained;
use sark_core::manifold::task::app::Runtime;

use crate::{
    conn::{Error, machine::ops::Drain},
    server::{
        runtime::output::flush::{self, Flush},
        state,
        task::{
            Context,
            response::{deadline::Deadline, expiry},
            timers, *,
        },
    },
};

pub(in crate::server::task::app) struct Timeout<'a, 'd, ID: Identity + 'd, R, M, D>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(super) router: pin::Pin<&'a mut R>,
    pub(super) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(super) mode: &'a mut M,
    pub(super) deadlines: &'a mut D,
}

impl<'d, ID: Identity + 'd, R, M, D> Timeout<'_, 'd, ID, R, M, D>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    D: timers::Policy<'d>,
{
    pub(in crate::server::task::app) fn new<'a>(
        router: pin::Pin<&'a mut R>,
        tasks: &'a mut registry::Table<'d, ID, R::Task>,
        mode: &'a mut M,
        book: &'a mut D,
    ) -> Timeout<'a, 'd, ID, R, M, D> {
        Timeout {
            router,
            tasks,
            mode,
            deadlines: book,
        }
    }

    pub(in crate::server::task::app) fn application<W>(
        mut self,
        mut conn: Context<'_, '_, '_, '_, '_, 'd, ID, R, W, M, D>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d>
    where
        W: wire::Spec,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
    {
        let id = conn.id();
        if !D::ENABLED {
            return handler::Timeout::Discard;
        }
        let now = driver.driver_ref().clock().now();
        let turn = {
            let state = conn.state_mut();
            let mut mode =
                state::Projection::new(self.mode, &mut state.deadlines, now, self.deadlines);
            let mut h2 = book::Turn::new(&mut state.carrier, &mut state.book);
            let turn = expiry::Expiry::new(mode.poll_due()).apply(
                self.router.as_mut(),
                self.tasks,
                id,
                &mut state.task_index,
                &mut h2,
                &mut mode,
            );
            let (_, error) = mode.finish();
            match error {
                Some(error) => Err(Error::Deadline(error)),
                None => Ok(turn),
            }
        };
        let turn = match turn {
            Ok(turn) => turn,
            Err(error) => {
                finish_transport::<ID, R, _, _, _>(&mut conn, driver, Some(error), self.mode);
                return handler::Timeout::Discard;
            }
        };
        let due = {
            let state = conn.state_mut();
            timers::Policy::controller(self.deadlines, &mut state.deadlines)
                .and_then(|ctl| ctl.deadline())
        };
        if matches!(turn, Deadline::Wake) {
            conn.wake_target().wake();
        }
        if matches!(turn, Deadline::Critical) {
            finish_transport::<ID, R, _, _, _>(
                &mut conn,
                driver,
                Some(Error::CriticalDeadline),
                self.mode,
            );
            return handler::Timeout::Discard;
        }
        let drain = conn.state_mut().finish_drain();
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Flush::new(&mut conn).observe(driver, None, self.mode);
        }
        let update = due.map_or(handler::Deadline::Clear, handler::Deadline::Arm);
        handler::Timeout::Continue(update)
    }

    pub(in crate::server::task::app) fn begin<W>(
        self,
        mut conn: Context<'_, '_, '_, '_, '_, 'd, ID, R, W, M, D>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain
    where
        W: wire::Spec,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
    {
        let done = {
            let state = conn.state_mut();
            let activity = state.activity();
            let drain = state.carrier.control().drain(activity);
            match (drain, state.carrier.status().is_closing()) {
                (Drain::Complete, _) | (Drain::Pending, true) => handler::Drain::Complete,
                (Drain::Pending, false) => handler::Drain::Pending,
            }
        };
        if !conn.state().carrier.output().is_empty() {
            Flush::new(&mut conn).observe(driver, None, self.mode);
        }
        done
    }
}
