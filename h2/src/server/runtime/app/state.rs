use std::marker;

use sark_core::manifold::{
    schedule,
    server::app::{self, Capacity, Spec},
};
use sark_http::payload::compress::dynamic::hash;

use crate::{
    conn::{config, event, ingress::PrefacePhase, machine},
    server::runtime::{
        self, Connection,
        output::{self, flush::End},
    },
};

pub struct State<'d, A, O>
where
    A: app::Application<'d, Connection<'d, A>, O>,
{
    pub(in crate::server::runtime) carrier: Connection<'d, A>,
    pub(in crate::server::runtime) app: A::State,
    owner: marker::PhantomData<fn(O) -> O>,
}

impl<'d, A, O> State<'d, A, O>
where
    A: app::Application<'d, Connection<'d, A>, O>,
{
    pub(super) fn try_new(
        app: &A,
        machine: &config::Validated<crate::ServerRole>,
        seed: hash::Seed,
        preface: PrefacePhase,
    ) -> Result<Self, runtime::Build<A::Build>> {
        let carrier = Connection::<'d, A>::from_runtime_seed(machine, seed, preface)
            .map_err(runtime::Build::Connection)?;
        let capacity = Capacity::new(machine.stream_count(), machine.data_events().raw());
        let app = app.state(capacity).map_err(runtime::Build::App)?;
        Ok(Self {
            carrier,
            app,
            owner: marker::PhantomData,
        })
    }

    fn activity(&self) -> schedule::Quiescence {
        match (A::activity(&self.app), self.carrier.status().active_len()) {
            (schedule::Quiescence::Quiet, 0) => schedule::Quiescence::Quiet,
            (schedule::Quiescence::Busy | schedule::Quiescence::Quiet, _) => {
                schedule::Quiescence::Busy
            }
        }
    }

    pub(super) fn begin_drain(&mut self) {
        let activity = self.activity();
        self.carrier.control().drain(activity);
    }
}

impl<'d, A, O> output::State<'d> for State<'d, A, O>
where
    A: app::Application<'d, Connection<'d, A>, O>,
{
    type Fields = <A as Spec>::Fields;
    type Evidence = event::Ignore;

    fn protocol(&mut self) -> &mut Connection<'d, A> {
        &mut self.carrier
    }

    fn settle(&mut self) -> End {
        use machine::ops::Drain;

        if self.carrier.status().is_closing() {
            return End::Close;
        }
        if !self.carrier.status().is_goaway() {
            return End::Keep;
        }
        let activity = self.activity();
        match self.carrier.control().finish(activity) {
            Drain::Complete => End::Close,
            Drain::Pending => End::Keep,
        }
    }
}
