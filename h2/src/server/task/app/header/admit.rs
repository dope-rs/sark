use std::ops::ControlFlow;

use dope::{core::driver::route::Identity, manifold::network::listener::conn::Id};
use sark_core::{
    decode::{self, request, state},
    life,
    manifold::{schedule::typed, task::app::Runtime},
};
use sark_http::request::body::Ref;

use crate::{
    conn::{self, event},
    frame::numbers,
    server::{
        response::{Encoder, Handoff},
        state::{
            Transitions,
            exchange::{self, Accepted, Input, Plain, admit},
        },
        task::{
            app::{drive::Drive, header},
            response, *,
        },
    },
    stream,
};

pub(in crate::server::task::app) struct Admit<'a, 'state, 'd, ID: Identity + 'd, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>> + decode::Guarded,
{
    head: Drive<'a, 'state, 'd, ID, R, Q>,
}

pub(in crate::server::task::app) trait Begin<'d, R: Runtime<'d>>:
    Sized
{
    fn begin<ID: Identity + 'd, Q>(
        self,
        head: Drive<'_, '_, 'd, ID, R, Q>,
        stream: stream::Key,
    ) -> Result<(), conn::Error>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<Id<'d, ID>>,
        Q::Retention: Handoff;
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q> Admit<'a, 'state, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>> + decode::Guarded,
    Q: Transitions<Id<'d, ID>>,
    Q::Retention: Handoff,
{
    pub(in crate::server::task::app) const fn new(head: Drive<'a, 'state, 'd, ID, R, Q>) -> Self {
        Self { head }
    }

    pub(in crate::server::task::app) fn next(self) -> Result<(), conn::Error> {
        if !self.head.state.connection.streams().headers().is_ready() {
            return Ok(());
        }
        let Some((stream, routed)) = self.head.state.data().take_route() else {
            return Ok(());
        };
        self.resume(stream, routed.into_state())?;
        Ok(())
    }

    fn resume<F>(
        self,
        stream: stream::Key,
        routed: request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<F>>,
    ) -> Result<(), conn::Error>
    where
        F: Input<'d>,
    {
        let mut head = self.head;
        let call = identify(head.conn, stream);
        let slot = match head.state.vacant(stream) {
            Some(slot) => slot,
            None => {
                let result = head.state.body_credit.scope(|mut ledger| {
                    routed.fold(|routed| {
                        // SAFETY: this fresh or deferred request remains in
                        // this exact admission Book while the slot is refused.
                        unsafe { exchange::raw::Closing::new(routed, &mut ledger) }
                            .discard::<F, _>(head.mode.body(call))
                    })
                });
                let error = match result {
                    Ok(()) => numbers::ErrorCode::EnhanceYourCalm,
                    Err(error) => error.code(),
                };
                head.reset(stream, error);
                return Ok(());
            }
        };
        let next = 'admission: {
            let writer = Encoder::new(slot.connection, stream, Ref::from_slice(&[]));
            let result = admit::run::<R, F, Q::Retention, event::Retirement>(
                head.router.as_ref().get_ref(),
                routed,
                writer,
            );
            let (reply, input) = match result {
                Ok(ControlFlow::Continue(ready)) => break 'admission Ok(Some(ready)),
                Ok(ControlFlow::Break((value, input))) => {
                    let reply = match value {
                        ControlFlow::Continue(wire) => (response::Response::Encoded(wire), None),
                        ControlFlow::Break(head) => (
                            response::Response::Head {
                                head,
                                outcome: life::Outcome::Rejected,
                                count: response::Count::Settled,
                            },
                            Some(conn::DataBlock::Egress),
                        ),
                    };
                    (Ok(reply), input)
                }
                Err((error, failed, input)) => (Err((error, failed)), input),
            };
            let release = slot.body_credit.scope(|mut ledger| {
                // SAFETY: the rejected input was produced by this exact
                // Book's header admission and has not left its custody.
                let input = unsafe { ledger.bind(input) };
                input.discard(&mut ledger, head.mode.body(call))
            });
            if let Err(error) = release {
                break 'admission Err((error.code(), stream));
            }
            match reply {
                Ok((next, block)) => {
                    slot.response.insert(typed::Next::Control(next, block));
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        };
        match next {
            Ok(Some(ready)) => {
                // SAFETY: admission retains the selected or fresh request in
                // this same Drive and Book through guard completion.
                unsafe { header::raw::accept(head, stream, ready) }?;
            }
            Ok(None) => Transitions::ready(head.mode, call),
            Err((error, failed)) => {
                head.reset(failed, error);
            }
        }
        Ok(())
    }
}

impl<'d, R> Begin<'d, R>
    for request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>
where
    R: Runtime<'d> + decode::Guarded,
{
    fn begin<ID: Identity + 'd, Q>(
        self,
        head: Drive<'_, '_, 'd, ID, R, Q>,
        stream: stream::Key,
    ) -> Result<(), conn::Error>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<Id<'d, ID>>,
        Q::Retention: Handoff,
    {
        if !head.state.connection.streams().headers().is_ready() {
            let evidence = head.mode.body(identify(head.conn, stream));
            head.state.data().try_route(stream, self, evidence)?;
            return Ok(());
        }
        Admit::new(head).resume(stream, self)
    }
}

impl<'d, R, U> Begin<'d, R> for request::Plain<U>
where
    R: Runtime<'d>,
    U: state::Admission<
        R::Prepared,
        state::Routed,
        Accepted<'d>,
        Output = <<R::Prepared as request::Prepared>::Family as state::Family<
            R::Prepared,
        >>::State<Accepted<'d>>,
    >,
{
    fn begin<ID: Identity + 'd, Q>(
        self,
        head: Drive<'_, '_, 'd, ID, R, Q>,
        stream: stream::Key,
    ) -> Result<(), conn::Error>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<Id<'d, ID>>,
        Q::Retention: Handoff,
    {
        let input = Plain::new().admit::<R::Prepared, state::Routed, U>(self.into_state());
        // SAFETY: Plain admission just created this uncharged body for this Book.
        unsafe { header::raw::accept(head, stream, input) }
    }
}

impl<'d, R, U, G> Begin<'d, R> for request::Start<U, G>
where
    R: Runtime<'d>,
    request::Plain<U>: Begin<'d, R>,
    request::Guarded<G>: Begin<'d, R>,
{
    fn begin<ID: Identity + 'd, Q>(
        self,
        head: Drive<'_, '_, 'd, ID, R, Q>,
        stream: stream::Key,
    ) -> Result<(), conn::Error>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<Id<'d, ID>>,
        Q::Retention: Handoff,
    {
        match self {
            Self::Plain(request) => request.begin(head, stream),
            Self::Guarded(request) => request.begin(head, stream),
        }
    }
}
