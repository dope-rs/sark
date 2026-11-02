use core::convert::Infallible;

use dope::{
    core::{
        BranchError,
        driver::{route::Identity, schedule::ready::task::owner},
    },
    manifold::network::listener::conn,
};
use sark_core::{
    body,
    decode::{
        exec, request,
        state::{Family, Flow, Fold, Slot},
    },
    life,
    manifold::task::app::Runtime,
};
use sark_http::payload::owned;

use crate::{
    server::{
        Target, payload,
        payload::Buffer,
        state,
        task::{app::Drive, identify, ingress, response},
    },
    stream,
};

/// Completes the exact buffered request removed from this Drive's Book.
/// # Safety
/// `input` must have remained in `drive`'s Book throughout its charged lifetime.
pub(in crate::server::task) unsafe fn buffered<'d, ID, R, Q>(
    drive: Drive<'_, '_, 'd, ID, R, Q>,
    stream: stream::Key,
    input: <<R::Prepared as request::Prepared>::Family as Family<R::Prepared>>::State<
        ingress::Receiving<'d>,
    >,
) where
    ID: Identity + 'd,
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    Flow::fold(input, Finish { drive, stream });
}

/// Completes a selected synchronous request under its original Book account.
/// # Safety
/// `input` must have remained in `drive`'s Book throughout its charged lifetime.
pub(in crate::server::task) unsafe fn sync<'d, ID, R, Q>(
    drive: Drive<'_, '_, 'd, ID, R, Q>,
    stream: stream::Key,
    input: request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>,
) where
    ID: Identity + 'd,
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    (Finish { drive, stream }).sync(input);
}

/// Completes a selected task request under its original Book account.
/// # Safety
/// `input` must have remained in `drive`'s Book throughout its charged lifetime.
pub(in crate::server::task) unsafe fn start<'d, ID, R, Q>(
    drive: Drive<'_, '_, 'd, ID, R, Q>,
    stream: stream::Key,
    input: request::Admitted<exec::Task, R::Prepared, Buffer<'d>>,
) where
    ID: Identity + 'd,
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    (Finish { drive, stream }).task(input);
}

pub(in crate::server::task) fn built<'d, ID, R, Q>(
    drive: Drive<'_, '_, 'd, ID, R, Q>,
    stream: stream::Key,
    build: <R::Build as body::Build<'d>>::Output,
) where
    ID: Identity + 'd,
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    (Finish { drive, stream }).built(build);
}

struct Finish<'a, 'state, 'd, ID: Identity + 'd, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    drive: Drive<'a, 'state, 'd, ID, R, Q>,
    stream: stream::Key,
}

impl<'d, ID: Identity + 'd, R, Q> Fold<R::Prepared, ingress::Receiving<'d>>
    for Finish<'_, '_, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    type Output = ();

    fn sync_open<S: Slot<R::Prepared>>(self, slot: S, body: Buffer<'d>) -> Self::Output {
        let input = slot.admit::<exec::Sync, _>(body);
        self.sync(input)
    }

    fn task_open<S: Slot<R::Prepared>>(self, slot: S, body: Buffer<'d>) -> Self::Output {
        let input = slot.admit::<exec::Task, _>(body);
        self.task(input)
    }

    fn sync_done<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }

    fn task_done<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }

    fn duplex_open<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }

    fn duplex_done<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }

    fn reject_open<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }

    fn reject_done<S: Slot<R::Prepared>>(self, _: S, never: Infallible) -> Self::Output {
        match never {}
    }
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q> Finish<'a, 'state, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    fn built(mut self, build: <R::Build as body::Build<'d>>::Output) {
        let call = identify(self.drive.conn, self.stream);
        state::Transitions::started(self.drive.mode, call);
        let result = match self
            .drive
            .tasks
            .vacant(self.drive.index, self.drive.conn, self.stream)
        {
            Ok(vacant) => vacant
                .try_insert(|target| {
                    Runtime::try_built(
                        self.drive.router.as_mut(),
                        build,
                        target,
                        owner::Owner::from(self.drive.conn),
                        self.drive.parent,
                    )
                })
                .map_err(BranchError::Second),
            Err(error) => Err(BranchError::First(error)),
        };
        if let Err(cause) = result {
            let code = match &cause {
                BranchError::First(error) => error.code(),
                BranchError::Second(error) => ingress::errors::reject_code(R::rejection(error)),
            };
            self.drive.reset(self.stream, code);
            return;
        }
        self.drive.mode.observe(life::Event::TaskAdmitted(call));
    }

    fn sync(self, input: request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>) {
        self.complete(input, |mut this, input| {
            let call = identify(this.drive.conn, this.stream);
            this.drive.mode.observe(life::Event::BuildReady(call));
            this.drive
                .queue(this.stream, response::Response::Sync(input));
        });
    }

    fn task(self, input: request::Admitted<exec::Task, R::Prepared, Buffer<'d>>) {
        self.complete(input, |mut this, input| {
            let call = identify(this.drive.conn, this.stream);
            this.drive.mode.observe(life::Event::BuildReady(call));
            let result =
                match this
                    .drive
                    .tasks
                    .vacant(this.drive.index, this.drive.conn, this.stream)
                {
                    Ok(vacant) => vacant
                        .try_insert(|target| {
                            Runtime::try_dispatch(
                                this.drive.router.as_mut(),
                                input,
                                target,
                                owner::Owner::from(this.drive.conn),
                                this.drive.parent,
                            )
                        })
                        .map_err(BranchError::Second),
                    Err(error) => Err(BranchError::First(error)),
                };
            if let Err(cause) = result {
                let code = match &cause {
                    BranchError::First(error) => error.code(),
                    BranchError::Second(error) => ingress::errors::reject_code(R::rejection(error)),
                };
                this.drive.reset(this.stream, code);
                return;
            }
            this.drive.mode.observe(life::Event::TaskAdmitted(call));
        });
    }

    fn complete<K>(
        mut self,
        input: request::Admitted<K, R::Prepared, Buffer<'d>>,
        ready: impl FnOnce(Self, request::Ready<K, R::Prepared, owned::Segments<'d>>),
    ) {
        let call = identify(self.drive.conn, self.stream);
        let result = self.drive.state.body_credit.scope(|mut ledger| {
            let input = input.try_map_body(|body| {
                // SAFETY: Finish retains this selected or newly admitted body
                // in the exact Book that owns this account until settlement.
                let body = unsafe { ledger.bind(body) };
                body.try_finish(&mut ledger, self.drive.mode.body(call))
            });
            match input {
                Ok(input) => Ok(input.ready()),
                Err(failure) => Err(payload::release_rejected(
                    failure,
                    &mut ledger,
                    &mut self.drive.mode.body(call),
                )),
            }
        });
        match result {
            Ok(input) => ready(self, input),
            Err(code) => self.drive.reset(self.stream, code),
        }
    }
}
