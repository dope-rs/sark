use std::{marker, time};

use dope::{
    core::{
        self,
        driver::{
            route::Identity,
            settings,
            settings::{claim, errors},
            storage,
        },
        io::socket::Listener,
    },
    manifold::{
        self,
        app::{creation, env, receive},
        network::listener::{self, handler},
        timing::{self, interval},
    },
    net::{
        self,
        link::{
            egress::data,
            pool::{change, recv::input},
        },
        tcp,
        wire::{self, domain},
    },
    runtime::{
        executor,
        topology::{self, lending},
    },
};
use sark_core::life::session;

use crate::server::{
    date,
    deadline::policies,
    listener::{Config, bind},
    run::{Prepared, Root, Wait, prepare, root::Bound},
};

/// Complete HTTP/1 server topology for one listener and Date clock.
pub struct Plan<
    ListenerId: Identity,
    DateId: Identity,
    P,
    I = receive::Coupled,
    D = policies::Input,
> where
    P: settings::Profile + timing::Policy,
    I: receive::Resources,
{
    listener: Config<ListenerId, P>,
    bound: Option<Listener>,
    date: interval::Component<DateId>,
    input: marker::PhantomData<fn() -> I>,
    deadline: D,
}

impl<ListenerId: Identity, DateId: Identity, P, I, D> Plan<ListenerId, DateId, P, I, D>
where
    P: settings::Profile + timing::Policy,
    I: receive::Resources,
{
    pub(crate) fn new(listener: Config<ListenerId, P>, deadline: D) -> Self {
        Self {
            listener,
            bound: None,
            date: interval::Component::every_second(),
            input: marker::PhantomData,
            deadline,
        }
    }

    #[must_use]
    pub fn with_bound(mut self, listener: Listener) -> Self {
        self.bound = Some(listener);
        self
    }

    /// Sets the validated maximum connection age and drain window.
    pub fn session(mut self, config: session::Config) -> Self {
        self.listener = self.listener.with_age(config);
        self
    }

    /// Replaces the static exchange-deadline policy without changing resources.
    pub fn policy<N>(self, deadline: N) -> Plan<ListenerId, DateId, P, I, N> {
        Plan {
            listener: self.listener,
            bound: self.bound,
            date: self.date,
            input: self.input,
            deadline,
        }
    }

    /// Allocates the selected wire owner for an existing storage tree.
    pub fn owner<W>(&self) -> Result<domain::Owner<W, ListenerId>, net::Resource>
    where
        W: wire::Spec,
        W::Bootstrap: domain::Allocate<W>,
    {
        let capacity = self.listener.listener.connections();
        domain::Owner::try_new(capacity)
    }

    /// Resolves listener resources while carrying the caller's storage.
    pub fn prepare<S>(
        self,
        value: S,
    ) -> Result<
        topology::Prepared<storage::With<S, date::Clock>, Self>,
        core::BranchError<prepare::Error, errors::Claim>,
    >
    where
        S: storage::Claimless,
    {
        let clock = date::Clock::new()
            .map_err(|error| core::BranchError::First(prepare::Error::Date(error)))?;
        let storage = storage::With::new(value, clock);
        let config = self.config();
        topology::Plan::new(storage, self)
            .with_install(config)
            .map_err(core::BranchError::Second)
    }

    /// Materializes this server's resource profile before topology composition.
    pub const fn config(&self) -> settings::Config {
        settings::Config::for_profile::<P>()
    }

    /// Prepares borrowed storage before the executor issues a dependent client.
    /// The callback owns this listener's sole terminal-failure waiter.
    pub fn prepare_in<'stage, 'scope, 'life, 'e, 'd, W, R, B, S, Q, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::With<S, date::Clock>, Q>,
        wire: R,
        next: impl for<'n, 'p, 's> FnOnce(
            Prepared<
                'n,
                'p,
                's,
                'life,
                'd,
                ListenerId,
                DateId,
                env::Bundle<tcp::Tcp, W, P>,
                R,
                B,
                I,
                D,
            >,
            Wait<'p, 'd>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::With<S, date::Clock>, Q>,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        'd: 'life,
        'life: 'scope,
        ListenerId: 'd,
        W: input::Execution + 'd,
        R: change::Prepare<'life, 'd, ListenerId, W>,
        B: data::raw::Payload<'d> + 'd,
        D: policies::Policy<'d, ListenerId>,
        I: input::Mode<W>,
        S: storage::Plan,
        Q: executor::Startup,
    {
        let clock = &stage.storage().1;
        let deadline = self.deadline;
        let (config, head, session) = self.listener.into_parts();
        let (lifetime, timing) = Self::timing(head, session, deadline);
        let install =
            listener::Install::<ListenerId, env::Bundle<tcp::Tcp, W, P>, _>::new(config, wire)
                .with_bound(self.bound)
                .with_life(lifetime)
                .with_timing(timing);
        let hash = stage.hash_state(listener::Domain::for_listener(ListenerId::ID.raw()));
        install.prepare(hash, stage, lending::Stage::driver, |listener, wait, stage| {
            let date = interval::Interval::<DateId>::every_second(&mut stage.driver())?;
            Ok(next(
                Prepared {
                    listener,
                    clock,
                    date,
                    deadline,
                    input: marker::PhantomData,
                },
                wait,
                stage,
            ))
        })?
    }

    /// Installs the listener and Date tick against their exact driver storage.
    /// The callback owns this listener's sole terminal-failure waiter.
    pub fn install<'stage, 'scope, 'life, 'e, 'd, W, B, F, S, Q, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::With<S, date::Clock>, Q>,
        wire: B,
        app: F,
        next: impl for<'n, 'p, 's> FnOnce(
            Bound<'n, 'p, 's, 'life, 'd, ListenerId, DateId, F, D, env::Bundle<tcp::Tcp, W, P>>,
            Wait<'p, 'd>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::With<S, date::Clock>, Q>,
        ) -> R,
    ) -> super::Result<R, F::Error>
    where
        'd: 'life,
        'life: 'scope,
        ListenerId: 'd,
        W: input::Execution + 'd,
        B: change::Prepare<'life, 'd, ListenerId, W>,
        F: bind::Bind<'life, 'd, ListenerId, Wire = W, Input = I>,
        D: policies::Policy<'d, ListenerId>,
        I: input::Mode<W>,
        S: storage::Plan,
        Q: executor::Startup,
    {
        let clock = &stage.storage().1;
        let listener = self.listener;
        let bound = self.bound;
        let deadline = self.deadline;
        let (config, head, session) = listener.into_parts();
        let (lifetime, timing) = Self::timing(head, session, deadline);
        listener::Install::<ListenerId, env::Bundle<tcp::Tcp, W, P>, _>::new(config, wire)
            .with_bound(bound)
            .with_life(lifetime)
            .with_timing(timing)
            .build_in(
                stage,
                |binding| bind::Bind::try_bind(app, binding, deadline, clock),
                |listener, wait, stage| {
                    let interval = interval::Interval::<DateId>::every_second(&mut stage.driver())?;
                    let date = date::Root::new(clock, interval);
                    Ok(next(Root { listener, date }, wait, stage))
                },
            )?
            .map_err(manifold::BuildError::Create)
    }

    fn timing<'d>(
        head: time::Duration,
        session: Option<session::Config>,
        deadline: D,
    ) -> (handler::Lifetime, handler::Timing)
    where
        D: policies::Policy<'d, ListenerId>,
    {
        let lifetime = match session {
            Some(config) => {
                let max_age = config.age();
                let drain = config.drain();
                handler::Lifetime::graceful(max_age, drain)
            }
            None => handler::Lifetime::persistent(),
        };
        let timing = match policies::Policy::timeout(deadline, head) {
            Some(timeout) => handler::Timing::timeout(timeout),
            None => handler::Timing::managed(),
        };
        (lifetime, timing)
    }
}

impl<ListenerId: Identity, DateId: Identity, P, D> Plan<ListenerId, DateId, P, receive::Coupled, D>
where
    P: settings::Profile + timing::Policy,
{
    pub fn gated_scoped(self) -> Plan<ListenerId, DateId, P, receive::GatedScoped, D> {
        Plan {
            listener: self.listener,
            bound: self.bound,
            date: self.date,
            input: marker::PhantomData,
            deadline: self.deadline,
        }
    }
}

impl<ListenerId: Identity, DateId: Identity, P, I, D> settings::Resource
    for Plan<ListenerId, DateId, P, I, D>
where
    P: settings::Profile + timing::Policy,
    I: receive::Resources,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        use dope::manifold::network::listener::topology::Layout;

        let needs =
            needs.include(claim::Claim::new(settings::Routes::EMPTY).receive(P::RECEIVE.into()))?;
        let needs =
            Layout::<ListenerId, I>::new(self.listener.listener.connections()).include_in(needs)?;
        self.date.include_in(needs)
    }
}
