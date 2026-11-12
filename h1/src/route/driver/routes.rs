use o3::buffer::write::Slice;
use std::ops::ControlFlow;

use sark_core::{
    body::credit,
    route::{guard, table},
};
use sark_http::{
    h1::{head::request::Coding, target},
    payload::compress,
    request::Form,
};

use crate::{
    route::{Cache, PolicyOf, Router, set, sync},
    session,
    wire::{cache, head, output, parsing, parsing::framing},
};

pub(super) struct Select<'turn, 'io, R: Router> {
    pub(super) router: &'turn R,
    pub(super) egress: Egress<'turn, <R::Table as set::Table<R::State>>::Compression>,
    pub(super) permit: session::Permit,
    pub(super) conn: &'turn mut super::Connection<'io, R::State, R::Table>,
}

impl<'turn, 'io, R>
    set::Visit<
        'io,
        R::State,
        <R::Table as set::Table<R::State>>::Set,
        <R::Table as set::Table<R::State>>::Pending<'io>,
        <R::Table as set::Table<R::State>>::Head,
    > for Select<'turn, 'io, R>
where
    R: Router,
{
    type Output = super::Outcome<'io, R::State, R::Table>;

    fn visit<P>(self, found: parsing::raw::Bound<&parsing::Head<'_>, P::Route>) -> Self::Output
    where
        P: set::Slot<
                R::State,
                <R::Table as set::Table<R::State>>::Set,
                Pending<'io> = <R::Table as set::Table<R::State>>::Pending<'io>,
                Head = <R::Table as set::Table<R::State>>::Head,
            >,
        P::Route: table::Sync<R::State>,
    {
        match framing::Head::<_, Coding>::start(found, P::Route::POLICY.coding) {
            Ok(framing::HeadProgress::Complete(head)) => self.complete::<P>(head),
            Ok(framing::HeadProgress::Pending(pending)) => session::RouteOutcome::AwaitHead {
                permit: self.permit,
                pending: P::head(pending),
            },
            Err(error) => error.into_consume().into(),
        }
    }
}

impl<'turn, 'io, R>
    set::OwnedVisit<
        'io,
        R::State,
        <R::Table as set::Table<R::State>>::Set,
        <R::Table as set::Table<R::State>>::Pending<'io>,
        <R::Table as set::Table<R::State>>::Head,
    > for Select<'turn, 'io, R>
where
    R: Router,
{
    type Output = super::Outcome<'io, R::State, R::Table>;

    fn visit<P>(self, found: parsing::raw::Bound<parsing::Owned, P::Route>) -> Self::Output
    where
        P: set::Slot<
                R::State,
                <R::Table as set::Table<R::State>>::Set,
                Pending<'io> = <R::Table as set::Table<R::State>>::Pending<'io>,
                Head = <R::Table as set::Table<R::State>>::Head,
            >,
        P::Route: table::Sync<R::State>,
    {
        let permit = self.permit;
        match framing::PendingHead::<P::Route, Coding>::try_start(
            found,
            P::Route::POLICY.coding,
            permit,
            P::head,
            |head, permit| Self { permit, ..self }.complete::<P>(head),
        ) {
            Ok(outcome) => outcome,
            Err(error) => error.into_consume().into(),
        }
    }
}

impl<'turn, 'io, R: Router> Select<'turn, 'io, R> {
    fn complete<P>(
        self,
        found: framing::Head<'_, '_, P::Route, Coding>,
    ) -> super::Outcome<'io, R::State, R::Table>
    where
        P: set::Slot<
                R::State,
                <R::Table as set::Table<R::State>>::Set,
                Pending<'io> = <R::Table as set::Table<R::State>>::Pending<'io>,
                Head = <R::Table as set::Table<R::State>>::Head,
            >,
        P::Route: table::Sync<R::State>,
    {
        let source = found.source();
        let response = found.response();
        let permit = self.permit;
        let Egress {
            response_cache: entries,
            gzip,
            date,
            write,
        } = self.egress;
        let entry = cache_slot::<R, P>(entries);
        let request = source.request();
        let method = P::METHOD;
        let form = match request.plan().form() {
            target::Form::Origin | target::Form::Absolute => Form::Path,
            target::Form::Authority => Form::Authority,
            target::Form::Asterisk => Form::Asterisk,
        };
        let head = guard::Head::from_parts(&method, form, request.path(), request.authority());
        let (write, date) = match <P as guard::Slot<R::State>>::try_admit(
            head,
            self.router.state(),
            output::Egress::bind(write, date, source.input(), response),
        ) {
            ControlFlow::Continue(reject) => reject.into_parts(),
            ControlFlow::Break(reply) => {
                return reply
                    .into_consume(
                        permit,
                        session::Consumption::Buffered(session::Buffered::new(0)),
                        head::Connection::Close,
                    )
                    .into();
            }
        };
        let route = sync::Endpoint::new(
            sync::Egress::new(output::Egress::new(write, date, response), entry, gzip),
            self.conn,
        )
        .dispatch::<P::Route, R::State, _>(
            permit,
            found,
            self.router.state(),
            credit::Disabled,
        );
        match route {
            session::RouteOutcome::AwaitHead { pending, .. } => match pending {},
            session::RouteOutcome::AwaitReject { pending, .. } => match pending {},
            session::RouteOutcome::Outcome(ready) => session::RouteOutcome::Outcome(ready),
            session::RouteOutcome::AwaitBody {
                state,
                expectation: expect,
                pending,
            } => session::RouteOutcome::AwaitBody {
                state,
                expectation: expect,
                pending: P::pending(pending),
            },
        }
    }
}

pub(super) struct Resume<'turn, 'io, 'input, R: Router> {
    pub(super) input: crate::Input<'input>,
    pub(super) select: Select<'turn, 'io, R>,
}

impl<'io, R: Router>
    set::ResumeHead<
        'io,
        R::State,
        <R::Table as set::Table<R::State>>::Set,
        <R::Table as set::Table<R::State>>::Pending<'io>,
        <R::Table as set::Table<R::State>>::Head,
    > for Resume<'_, 'io, '_, R>
{
    type Output = super::Outcome<'io, R::State, R::Table>;

    fn resume<P>(self, pending: framing::PendingHead<P::Route, Coding>) -> Self::Output
    where
        P: set::Slot<
                R::State,
                <R::Table as set::Table<R::State>>::Set,
                Pending<'io> = <R::Table as set::Table<R::State>>::Pending<'io>,
                Head = <R::Table as set::Table<R::State>>::Head,
            >,
        P::Route: table::Sync<R::State>,
    {
        let select = self.select;
        let permit = select.permit;
        match pending.try_resume(self.input, permit, P::head, |head, permit| {
            Select { permit, ..select }.complete::<P>(head)
        }) {
            Ok(outcome) => outcome,
            Err(error) => error.into_consume().into(),
        }
    }
}

pub(super) struct Finish<'turn, R: Router> {
    pub(super) router: &'turn R,
    pub(super) egress: Egress<'turn, <R::Table as set::Table<R::State>>::Compression>,
    pub(super) body: session::Complete,
    pub(super) permit: session::Permit,
}

impl<'io, R> set::Take<'io, R::State, <R::Table as set::Table<R::State>>::Set> for Finish<'_, R>
where
    R: Router,
{
    type Output = session::Outcome<'io>;

    fn take<P>(self, wait: sync::Pending<'io, P::Route>) -> Self::Output
    where
        P: set::Slot<R::State, <R::Table as set::Table<R::State>>::Set>,
        P::Route: table::Sync<R::State>,
    {
        let entry = cache_slot::<R, P>(self.egress.response_cache);
        let response = wait.response();
        wait.finish(
            self.body,
            self.permit,
            self.router.state(),
            sync::Egress::new(
                output::Egress::new(self.egress.write, self.egress.date, response),
                entry,
                self.egress.gzip,
            ),
            credit::Disabled,
        )
    }
}

pub(super) struct Egress<'turn, C: compress::Compression> {
    pub(super) response_cache: &'turn mut [Option<cache::Entry>],
    pub(super) gzip: &'turn mut C,
    pub(super) date: &'turn [u8; 29],
    pub(super) write: Slice<'turn, &'turn mut usize>,
}

/// Bypassed routes need no slot; stored responses retain their table index proof.
fn cache_slot<R, P>(entries: &mut [Option<cache::Entry>]) -> cache::Cache<'_>
where
    R: Router,
    P: set::Slot<R::State, <R::Table as set::Table<R::State>>::Set>,
{
    const { assert!((P::INDEX as usize) < <R::Table as set::Table<R::State>>::LEN) };
    const {
        assert!(
            !matches!(P::Route::POLICY.cache, Cache::Store)
                || <R::Table as set::Table<R::State>>::HAS_CACHE
        );
    }
    match P::Route::POLICY.cache {
        Cache::Bypass => cache::Cache::empty(),
        Cache::Store => cache::Cache::new(&mut entries[P::INDEX as usize]),
    }
}
