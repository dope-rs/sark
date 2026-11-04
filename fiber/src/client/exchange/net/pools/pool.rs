use std::{convert::Infallible, marker, marker::PhantomData};

use dope::{
    core::driver::{self, route::Identity, schedule::app},
    manifold::client::{
        close,
        conn::Id,
        deadline, port,
        session::{Input, turns},
    },
    net::link::egress,
};
use dope_fiber::reply::{self, Registration, order::Depth, recv::attach};
use o3::{alloc, base::cell::region, buffer::pool};
use sark_client::{capacity, conn};

use crate::client::{
    config::report,
    conn::{Connector, route::Route},
    exchange::{
        net::{
            self, ReadyQueue as _,
            pools::entries::{Bound, Directory, Positioned, State},
        },
        request, reuse,
    },
};

pub(in crate::client) struct Pool<
    'f,
    'e,
    'b,
    'c,
    'd,
    ID: Identity,
    Tag,
    E = Infallible,
    F = report::Never,
> where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) directory: Directory<'b, 'd, ID, Tag, F::State<'f, 'd>>,
    /// The port storage loan is independent of the report payload lifetime.
    pub(in crate::client) responses: &'e reply::Arena<
        'b,
        'c,
        'd,
        net::Outcome<'d, F::Detail<'f>>,
        net::Outcome<'d, F::Detail<'f>>,
        request::Kind,
    >,
    reports: F::Source<'f>,
    failure: marker::PhantomData<fn(E) -> F>,
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, Tag, E, F> Pool<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>
where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client::exchange::net) fn submit(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
        policy: reuse::Policy,
    ) -> Option<Tag> {
        let entry = bound.entry;
        entry.admission.set(entry.admission.get().submitted(policy));
        self.schedule(token, &bound)
    }

    /// The admission reservation preserves report vacancy through activation.
    pub(in crate::client::exchange::net) fn try_connect(
        &self,
        id: Id<'d, ID>,
    ) -> Result<
        impl FnOnce(&mut region::Token<region::Identity<'d>>, Tag, reuse::Admission<'d>) + '_,
        net::Open,
    > {
        let reserve = self.directory.try_connect(id)?;
        Ok(
            move |token: &mut region::Token<region::Identity<'d>>, tag, access| {
                let bound = reserve.commit(tag);
                let entry = bound.entry;
                let reply = self.responses.lane(bound.position());
                self.directory.live.set(self.directory.live.get() + 1);
                reply.control().activate(token);
                entry.admission.set(access);
                self.directory.schedule(token, &bound);
            },
        )
    }

    /// The arena hands back an index it allocated for this directory.
    pub(in crate::client) fn maintain(
        &self,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<Tag> {
        let released = self.responses.maintain(work, token)?;
        let entry = released.entry(&self.directory.entries);
        let (position, tag) = match entry.state.get() {
            State::Ready(position, tag) if entry.admission.get().can_queue() => (position, tag),
            _ => return None,
        };
        let bound = Bound {
            entry,
            tag,
            position,
            route: PhantomData,
        };
        self.schedule(token, &bound)
    }

    pub(in crate::client::exchange::net) fn complete(
        &self,
        driver: &mut driver::Context<'_, 'd>,
        bound: &Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
        budget: reuse::Budget,
        cause: Option<capacity::Error>,
    ) -> Option<Tag> {
        let entry = bound.entry;
        let reply = self.responses.lane(bound.position());
        let depth = match cause {
            Some(cause) => reply.fail_one(driver.region_token(), cause),
            None => reply.complete(driver.region_token()),
        };
        let access = entry
            .admission
            .get()
            .completed(budget, depth, || driver.driver_ref().clock().now());
        let detailed = depth == Depth::ZERO
            && F::disarm(&entry.report, driver.region_token()) == report::Disarm::Detailed;
        entry.admission.set(access);
        if detailed || !access.can_queue() {
            return None;
        }
        self.schedule(driver.region_token(), bound)
    }

    pub(in crate::client::exchange::net) fn release(
        &self,
        driver: &mut driver::Context<'_, 'd>,
        bound: Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
        policy: reuse::Policy,
    ) -> Option<Tag> {
        let entry = bound.entry;
        let reply = self.responses.lane(bound.position());
        let access = entry
            .admission
            .get()
            .released(policy, || driver.driver_ref().clock().now());
        let detailed = reply.depth(driver.region_token()) == Depth::ZERO
            && F::disarm(&entry.report, driver.region_token()) == report::Disarm::Detailed;
        entry.admission.set(access);
        if detailed || !access.can_queue() {
            return None;
        }
        self.schedule(driver.region_token(), &bound)
    }

    fn schedule(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
    ) -> Option<Tag> {
        let reply = self.responses.lane(bound.position());
        if matches!(reply.control().registration(token), Registration::Full) {
            return None;
        }
        self.directory.schedule(token, bound)
    }

    /// Partial HTTP heads already retain the request bound by the codec.
    /// Unprocessed input must settle before another reply can be registered.
    pub(in crate::client) fn input(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        id: Id<'d, ID>,
        input: Input,
    ) -> Option<Tag> {
        let bound = self.directory.entry(id)?;
        let entry = bound.entry;
        match (entry.state.get(), input) {
            (State::Ready(position, tag), Input::Yield | Input::Blocked) => {
                entry.state.set(State::Receiving(position, tag));
                self.directory.ready.remove(token, &bound);
                None
            }
            (State::Receiving(position, tag), Input::Empty | Input::Partial) => {
                entry.state.set(State::Ready(position, tag));
                self.schedule(token, &bound)
            }
            _ => None,
        }
    }

    pub(in crate::client::exchange::net) fn seal(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
    ) {
        let entry = bound.entry;
        entry.admission.set(reuse::Admission::Sealed);
        self.directory.ready.remove(token, bound);
    }

    pub(in crate::client) fn begin_close(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        id: Id<'d, ID>,
    ) {
        let Some(bound) = self.directory.entry(id) else {
            return;
        };
        self.close_claim(token, bound);
    }

    pub(in crate::client::exchange::net) fn close_claim(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
    ) -> Id<'d, ID> {
        let id = bound.connection();
        let entry = bound.entry;
        if matches!(entry.state.get(), State::Ready(..) | State::Receiving(..)) {
            entry.state.set(State::Closing(bound.position(), bound.tag));
            self.responses
                .lane(bound.position())
                .control()
                .deactivate(token);
            entry.admission.set(reuse::Admission::Sealed);
            self.directory.ready.remove(token, &bound);
        }
        id
    }

    pub(in crate::client) fn failed(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        id: Id<'d, ID>,
        cause: report::Failure<E>,
    ) {
        let Some(bound) = self.directory.entry(id) else {
            return;
        };
        F::failed(&bound.entry.report, token, cause);
    }

    pub(in crate::client) fn retire(
        &self,
        id: Id<'d, ID>,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        reason: close::Reason,
    ) -> egress::Cleared {
        let Some(bound) = self.directory.entry(id) else {
            return egress::Cleared::Done;
        };
        let entry = bound.entry;
        let position = bound.position();
        let tag = bound.tag;
        self.close_claim(token, bound);
        if matches!(entry.state.get(), State::Drained(..)) {
            return egress::Cleared::Done;
        }
        let reply = self.responses.lane(position);
        let progress = F::retire(&entry.report, token, |failure, retiring| {
            if let Some(error) = failure {
                retiring.fail(work, reply, error)
            } else {
                match net::Close::<F::Detail<'f>>::from(reason) {
                    net::Close::Capacity(cause) => retiring.fail(work, reply, &cause),
                    net::Close::Transport(error) => retiring.fail(work, reply, &error),
                }
            }
        });
        match progress {
            reply::Progress::Complete(_) => {
                entry.state.set(State::Drained(position, tag));
                egress::Cleared::Done
            }
            reply::Progress::Pending(_) => egress::Cleared::Retry,
        }
    }

    /// The exact transport retirement permits clearing late receive failures
    /// before another generation can be admitted to this slot.
    pub(in crate::client) fn finish_close<'s, P, O>(
        source: &Connector<'s, ID, P, O>,
        drained: turns::Drained<'d, ID, Connector<'s, ID, P, O>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) where
        P: Route<'f, 'e, 'b, 'c, 'd, ID, Tag = Tag, Error = E, Failure = F>,
    {
        let pool = &source.root.protocol().transport.pool;
        let id = drained.connection();
        let Some(entry) = pool.directory.entries.get(id.index().into_usize()) else {
            return;
        };
        match entry.state.get() {
            State::Admitted(position) if position.connection() == id => {}
            State::Drained(position, _) if position.connection() == id => {
                pool.directory.live.set(pool.directory.live.get() - 1);
            }
            _ => return,
        }
        F::retire(&entry.report, token, |_, _| {
            reply::Progress::Complete(Depth::ZERO)
        });
        entry.state.set(State::Vacant);
    }

    /// Visits each ready slot at most once, preserving FIFO on report exhaustion.
    pub(in crate::client::exchange::net) fn acquire(
        &self,
        driver: &mut driver::Context<'_, 'd>,
        tag: Tag,
        io: &port::Port<'_, 'd, pool::Cursor, ID>,
    ) -> net::Acquire<Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>, F::Detail<'f>> {
        if self.responses.is_exhausted(driver.region_token()) {
            return net::Acquire::Reply(attach::Error::IdentityExhausted);
        }
        let mut now = None;
        let mut first = None;
        let mut blocked = false;
        loop {
            if first.is_some_and(|index| {
                self.directory.ready.front(driver.region_token(), tag) == Some(index)
            }) {
                break;
            }
            let index = match self.directory.pop_ready(driver.region_token(), tag) {
                Ok(Some(index)) => index,
                Ok(None) => break,
                Err(cause) => return net::Acquire::Failed(conn::Error::Ready(cause)),
            };
            let Some(bound) = self.directory.at(index) else {
                continue;
            };
            let entry = bound.entry;
            match entry
                .admission
                .get()
                .claim(|| *now.get_or_insert_with(|| driver.driver_ref().clock().now()))
            {
                reuse::Claim::Ready => {}
                reuse::Claim::Expired => {
                    self.expire(driver.region_token(), bound, io);
                    continue;
                }
                reuse::Claim::Unavailable => continue,
            }
            let reply = self.responses.lane(bound.position());
            let state = reply.control().registration(driver.region_token());
            if state == Registration::IdentityExhausted {
                return net::Acquire::Reply(attach::Error::IdentityExhausted);
            }
            if state == Registration::Available {
                match F::arm(&self.reports, &entry.report, driver.region_token()) {
                    report::Arm::Ready => {
                        return net::Acquire::Ready(bound);
                    }
                    report::Arm::Failed(error) => return net::Acquire::Failed(error),
                    report::Arm::Exhausted => {
                        blocked = true;
                        if self
                            .directory
                            .schedule(driver.region_token(), &bound)
                            .is_none()
                        {
                            self.expire(driver.region_token(), bound, io);
                        } else {
                            first.get_or_insert(index);
                        }
                    }
                }
            }
        }
        if let Some(cause) = self.directory.failure.get() {
            net::Acquire::Failed(conn::Error::Ready(cause))
        } else if blocked {
            net::Acquire::FailureReport
        } else {
            net::Acquire::Pending
        }
    }

    pub(in crate::client::exchange::net) fn arm(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        id: Id<'d, ID>,
    ) -> net::Arm<Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>, F::Detail<'f>> {
        if self.responses.is_exhausted(token) {
            return net::Arm::Reply(attach::Error::IdentityExhausted);
        }
        if let Some(cause) = self.directory.failure.get() {
            return net::Arm::Failed(conn::Error::Ready(cause));
        }
        let Some(bound) = self.directory.entry(id) else {
            return net::Arm::Missing;
        };
        if !matches!(bound.entry.state.get(), State::Ready(..)) {
            return net::Arm::Missing;
        }
        match F::arm(&self.reports, &bound.entry.report, token) {
            report::Arm::Ready => net::Arm::Ready(bound),
            report::Arm::Exhausted => net::Arm::FailureReport(bound),
            report::Arm::Failed(error) => net::Arm::Failed(error),
        }
    }

    fn expire(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: Positioned<'_, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
        io: &port::Port<'_, 'd, pool::Cursor, ID>,
    ) {
        let id = self.close_claim(token, bound);
        io.close(id, close::Reason::Timeout(deadline::Kind::Lifetime));
    }
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, Tag, E, F> Pool<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>
where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client::exchange::net) fn try_new(
        ready: Tag::Ready,
        reports: F::Source<'f>,
        token: &region::Token<region::Identity<'d>>,
        replies: &'e reply::Arena<
            'b,
            'c,
            'd,
            net::Outcome<'d, F::Detail<'f>>,
            net::Outcome<'d, F::Detail<'f>>,
            request::Kind,
        >,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            directory: Directory::try_new::<E, F>(replies.domain(), ready, token)?,
            responses: replies,
            reports,
            failure: marker::PhantomData,
        })
    }
}
