use core::{convert::Infallible, ops::ControlFlow};

use dope::{
    core::driver::{route::Identity, schedule::sequence},
    manifold::network::listener::conn,
};
use o3::buffer::{resident, segments};
use sark_core::{
    body::{self, admit::Stream, buffered},
    decode::{
        dispatch, exec, request,
        state::{Family, Flow, Map, Slot},
    },
    life::Observer as _,
    manifold::{stream::Boundary, task::app::Runtime},
};

use crate::{
    conn::Error,
    frame::numbers,
    server::{
        payload::{self, Buffer},
        state,
        state::exchange::Accepted,
        task::{
            app::drive::Drive,
            ingress::{self, Buffered, Receiving, Schedule, errors},
            response::{self, input},
            *,
        },
    },
    stream,
};

/// Dispatches a concrete request under the Book that has retained its body.
/// # Safety
/// `input` must retain its original Book custody through fresh or deferred admission.
pub(super) unsafe fn accept<'a, 'state, 'd, ID, R, Q>(
    head: Drive<'a, 'state, 'd, ID, R, Q>,
    stream: stream::Key,
    input: <<R::Prepared as request::Prepared>::Family as Family<R::Prepared>>::State<Accepted<'d>>,
) -> Result<(), Error>
where
    ID: Identity + 'd,
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    Dispatch { head, stream }.run(input)
}

struct Dispatch<'a, 'state, 'd, ID: Identity + 'd, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    head: Drive<'a, 'state, 'd, ID, R, Q>,
    stream: stream::Key,
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q> Dispatch<'a, 'state, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    fn into_parts(self) -> (Drive<'a, 'state, 'd, ID, R, Q>, stream::Key) {
        (self.head, self.stream)
    }

    fn run(
        self,
        input: <<R::Prepared as request::Prepared>::Family as Family<R::Prepared>>::State<
            Accepted<'d>,
        >,
    ) -> Result<(), Error> {
        let (this, input) = match input.map_retain(self) {
            ControlFlow::Continue(parts) => parts,
            ControlFlow::Break(result) => return result,
        };
        let (head, stream) = this.into_parts();
        let evidence = head.mode.body(identify(head.conn, stream));
        // SAFETY: dispatch retains this input in its original Book while its
        // generated family chooses the persistent buffered successor.
        unsafe {
            ingress::raw::try_buffer(head.state.data(), stream, Buffered::new(input), evidence)
        }
    }

    fn duplex(self, input: input::Request<'d, R::Prepared>) {
        let (mut head, stream) = self.into_parts();
        head.queue(stream, response::Response::DuplexOpen { request: input });
    }

    fn unsupported(self, input: request::Admitted<exec::Unsupported, R::Prepared, Buffer<'d>>) {
        let (mut head, stream) = self.into_parts();
        let call = identify(head.conn, stream);
        let release = head.state.body_credit.scope(|mut ledger| {
            // SAFETY: this rejected body remains in the exact admission Book.
            let body = unsafe { ledger.bind(input.into_body()) };
            body.discard(&mut ledger, head.mode.body(call))
        });
        let error = match release {
            Ok(()) => numbers::ErrorCode::InternalError,
            Err(error) => error.code(),
        };
        head.reset(stream, error);
    }

    fn sync(self, input: request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>) {
        let (head, stream) = self.into_parts();
        // SAFETY: this request remains in dispatch's exact original Book.
        unsafe { response::raw::sync(head, stream, input) };
    }

    fn task(
        self,
        input: request::Admitted<exec::Task, R::Prepared, Buffer<'d>>,
        boundary: Boundary,
    ) -> Result<(), Error> {
        let (mut head, stream) = self.into_parts();
        let call = identify(head.conn, stream);
        if matches!(input.body().stream(), Stream::Streaming) {
            let (prepared, body) = input.into_parts();
            let build = match Runtime::try_start(head.router.as_ref().get_ref(), prepared) {
                Ok(build) => build,
                Err(error) => {
                    let release = head.state.body_credit.scope(|mut ledger| {
                        // SAFETY: failed start retains the body in its admission Book.
                        let body = unsafe { ledger.bind(body) };
                        body.discard(&mut ledger, head.mode.body(call))
                    });
                    if let Err(error) = release {
                        head.reset(stream, error.code());
                        return Ok(());
                    }
                    match error {
                        dispatch::Error::Invalid(reject) => {
                            head.queue(stream, response::Response::Status(reject.status()))
                        }
                        dispatch::Error::Busy | dispatch::Error::Sequence(sequence::Exhausted) => {
                            head.reset(stream, numbers::ErrorCode::RefusedStream)
                        }
                        dispatch::Error::Pending
                        | dispatch::Error::Moved
                        | dispatch::Error::Unsupported => {
                            head.reset(stream, numbers::ErrorCode::InternalError)
                        }
                        dispatch::Error::Body(never) | dispatch::Error::Encode(never) => {
                            match never {}
                        }
                    }
                    return Ok(());
                }
            };
            let schedule = if body.len() != 0 || matches!(boundary, Boundary::End) {
                Schedule::Ready
            } else {
                Schedule::Idle
            };
            let body = match body.try_stream(build, boundary) {
                Ok(body) => body,
                Err((body, error)) => {
                    let release = head.state.body_credit.scope(|mut ledger| {
                        // SAFETY: this transition's successor retains the same
                        // admission Book account and all of its original credit.
                        let body = unsafe { ledger.bind(body) };
                        payload::discard(body.into_release(), &mut ledger, head.mode.body(call))
                    });
                    let error = match release {
                        Ok(()) => errors::Push::Stream(error),
                        Err(release) => errors::Push::StreamRelease { error, release },
                    };
                    if let Some(observer) = head.mode.observer()
                        && let errors::Push::StreamRelease { error, release } = &error
                    {
                        match error {
                            buffered::Transition::Body(error) => observer.diagnostic(error),
                            buffered::Transition::Build(error) => {
                                <R::Build as body::Push<'d>>::observe(error, observer);
                            }
                        }
                        observer.diagnostic(release);
                    }
                    let error = error.code::<R>();
                    head.reset(stream, error);
                    return Ok(());
                }
            };
            let book = &mut **head.state;
            let occupied = {
                let evidence = head.mode.body(call);
                let vacant = match book.incoming.try_vacant(stream) {
                    Ok(vacant) => vacant,
                    Err(error) => {
                        let release = book.body_credit.scope(|mut ledger| {
                            // SAFETY: this builder remains in the dispatch's original Book.
                            let body = unsafe { ledger.bind(body) };
                            payload::discard(body.into_release(), &mut ledger, evidence)
                        });
                        return Err(errors::reject(error, release));
                    }
                };
                ingress::Streams::insert(vacant, &mut book.build_ready, body, schedule)
            };
            if matches!(boundary, Boundary::End) {
                // SAFETY: the selected entry, queue, and account belong to this exact Book.
                let turn = unsafe {
                    ingress::raw::drive_entry(
                        occupied,
                        &mut book.build_ready,
                        &mut book.body_credit,
                        body::DEFAULT_BUILD_STEP_BUDGET,
                        head.mode.body(call),
                    )
                };
                head.build(turn);
            }
            return Ok(());
        }
        // SAFETY: this request remains in dispatch's exact original Book.
        unsafe { response::raw::start(head, stream, input) };
        Ok(())
    }
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q> Map<R::Prepared, Accepted<'d>, Receiving<'d>>
    for Dispatch<'a, 'state, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    type Break = Result<(), Error>;
    type Retained = Self;

    fn sync_open<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Buffer<'d>)> {
        ControlFlow::Continue((self, slot, body))
    }

    fn sync_done<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Sync, _>(body);
        self.sync(input);
        ControlFlow::Break(Ok(()))
    }

    fn task_open<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Buffer<'d>)> {
        if body.stream() == Stream::Buffered {
            return ControlFlow::Continue((self, slot, body));
        }
        let input = slot.admit::<exec::Task, _>(body);
        ControlFlow::Break(self.task(input, Boundary::More))
    }

    fn task_done<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Task, _>(body);
        ControlFlow::Break(self.task(input, Boundary::End))
    }

    fn duplex_open<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Duplex, _>(body);
        self.duplex(input::Request::open(input));
        ControlFlow::Break(Ok(()))
    }

    fn duplex_done<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Duplex, _>(body);
        self.duplex(input::Request::finished(input));
        ControlFlow::Break(Ok(()))
    }

    fn reject_open<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Unsupported, _>(body);
        self.unsupported(input);
        ControlFlow::Break(Ok(()))
    }

    fn reject_done<S: Slot<R::Prepared>>(
        self,
        slot: S,
        body: Buffer<'d>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        let input = slot.admit::<exec::Unsupported, _>(body);
        self.unsupported(input);
        ControlFlow::Break(Ok(()))
    }
}
