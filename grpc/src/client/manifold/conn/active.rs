use std::{iter, mem};

use dope::{
    core::{
        driver::{
            route::{Identity, KeyTag},
            schedule::ready,
        },
        random,
    },
    manifold::client::{conn, session::response},
};
use io::{Session as _, ingress};
use o3::{alloc, buffer::resident};
use sark_core::manifold::client::domain::Plan as _;
use sark_h2::{client, conn::Build};

use crate::{
    client::{
        io,
        manifold::conn::{Pending, Progress},
        receive::policies::Notify,
        session,
        state::plan,
    },
    protocol::section,
};

pub(in crate::client::manifold) struct Active<'d, ID: Identity> {
    conn: conn::Id<'d, ID>,
    pub(in crate::client::manifold) ready: ready::Target<'d>,
    pub(in crate::client::manifold) session:
        session::Session<'d, client::Plan<section::Plan>, resident::Shared<'d>>,
}

impl<'d, ID: Identity> Active<'d, ID> {
    pub(super) fn new(
        lifetime: conn::Lifetime<'d, ID>,
        ready: ready::Target<'d>,
        protocol: plan::Plan<client::Plan<section::Plan>>,
        seed: &random::Seed,
    ) -> Result<Self, session::Create<Build, alloc::Error>> {
        let carrier = protocol
            .carrier
            .open_seeded(seed)
            .map_err(session::Create::Connection)?;
        let conn = lifetime.connection();
        Ok(Self {
            conn,
            ready,
            session: session::Session::try_plan(protocol, carrier)?,
        })
    }

    pub(super) const fn connection(&self) -> conn::Id<'d, ID> {
        self.conn
    }

    pub(super) fn wake(&self) {
        self.ready.wake();
    }

    pub(super) fn has_output(&self) -> bool {
        self.session.outbound_len() != 0
    }

    pub(super) fn owner_cap(&self) -> usize {
        self.session.owner_cap()
    }

    pub(super) fn resume<'scope>(
        &mut self,
        pending: response::Item<Pending<'d>>,
        done: &mut Notify<'scope, '_, '_>,
    ) -> Result<Progress<'d>, crate::Status> {
        let output = self.has_output();
        let mut owner = None;
        let result = match pending.into_inner() {
            Pending::Owner(input) => {
                owner = Some(input);
                let mut source = iter::from_fn(|| owner.take());
                self.session.ingest_many_on(&mut source, done)
            }
            Pending::Resume => self.session.resume_on(done),
        };
        if !output && self.has_output() {
            self.wake();
        }
        result?;
        let pending = match owner {
            Some(owner) => Some(Pending::Owner(owner)),
            None if ingress::Session::is_blocked(&self.session) => Some(Pending::Resume),
            None => None,
        };
        Ok(match pending {
            Some(pending) => Progress::Blocked(pending),
            None if self.session.state.ingress.closed
                && self.session.state.calls.responses().next().is_none() =>
            {
                Progress::Drained
            }
            None => Progress::Ready,
        })
    }
}

const _: () = assert!(
    mem::size_of::<(conn::Id<'static, KeyTag<0>>, ready::Target<'static>)>()
        <= mem::size_of::<conn::Lifetime<'static, KeyTag<0>>>()
);
