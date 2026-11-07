use std::convert::Infallible;

use sark_core::{
    body::{admit::Plan, credit},
    manifold::task::request::Input,
    request::BindError,
    route,
};
use sark_http::{
    h1::codec::canned,
    payload::{fixed, owned},
    request::Id,
};

use crate::{
    route::task::{Mode, Pending},
    session, switch,
    wire::{Failure, head, parsing::framing, response},
};

pub struct Endpoint<
    'a,
    'd,
    Runtime,
    Continuations,
    Transitions = switch::Disabled,
    Ready = Infallible,
    Heads = Infallible,
> {
    connection: &'a mut session::Conn<'d, Runtime, Continuations, Transitions, Ready, Heads>,
}

impl<'a, 'd, Runtime, Continuations, Transitions, Ready, Heads>
    Endpoint<'a, 'd, Runtime, Continuations, Transitions, Ready, Heads>
where
    Transitions: switch::Engine,
{
    pub fn new(
        conn: &'a mut session::Conn<'d, Runtime, Continuations, Transitions, Ready, Heads>,
    ) -> Self {
        Self { connection: conn }
    }

    pub fn dispatch<R, Source, Admit, Feed, E>(
        self,
        id: Id<'d>,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        source: Source,
        feed: Feed,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Request: route::Own>,
        <R::Request as route::Own>::Mode: Mode<'d, R>,
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(
            Input<'d, R>,
            usize,
            head::Connection,
            response::Context,
            &mut Runtime,
        ) -> session::Outcome<'d, Transitions::Raw>,
        Feed: FnOnce(
            Pending<'d, R>,
            owned::Segments<'d>,
            E,
        ) -> Result<Pending<'d, R>, fixed::Body<'d>>,
        E: credit::Evidence,
    {
        let frame = match framing::Frame::drive(found, &mut self.connection.ingress, &mut proof) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(wait)) => {
                let req = wait.head.with_id(id);
                let plan = Plan::new(
                    <R::Request as route::Schema>::BODY_RETENTION,
                    R::MAX_BODY,
                    wait.declared_body_len,
                    <<R::Request as route::Own>::Mode as route::Mode>::STREAM,
                );
                let open = match plan.try_open(R::MAX_BODY) {
                    Ok(open) => open,
                    Err(error) => {
                        return session::Outcome::Failed(Failure::Admission(error)).into();
                    }
                };
                let body = match <<R::Request as route::Own>::Mode as Mode<'d, R>>::begin(
                    req,
                    open,
                    wait.connection,
                    wait.response,
                ) {
                    Ok(body) => body,
                    Err(error) => {
                        let reply =
                            Failure::Route(<R::Request as route::Own>::reject(error)).body();
                        return session::Outcome::Close(reply).into();
                    }
                };
                let parts = self.connection.ingress.payload.take();
                let body = match feed(body, parts, proof) {
                    Ok(body) => body,
                    Err(reply) => return session::Outcome::Close(reply).into(),
                };
                permit.commit();
                return session::RouteOutcome::AwaitBody {
                    state: wait.state,
                    expectation: wait.expectation,
                    pending: body,
                };
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let framing::Assembly {
            input,
            total,
            connection: close,
            response,
        } = match frame.assemble(id) {
            Ok(Ok(ready)) => ready,
            Ok(Err(rejected)) => {
                let reason = Failure::Bind(BindError::Body(*rejected.0.error()));
                return session::Outcome::Failed(reason).into();
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let Some(admit) = source() else {
            return session::Outcome::Close(canned::SERVICE_UNAVAILABLE).into();
        };
        admit(
            input,
            total,
            close,
            response,
            &mut self.connection.async_state,
        )
        .into()
    }
}
