use o3::buffer::segments::assembly;
use sark_core::{
    body::{self, admit, buffered, credit, input},
    manifold::task::request::Input,
    request::{Parts, raw::Parsed},
    route,
};
use sark_http::payload::{fixed, owned, response::Body as _};

use crate::{
    session,
    wire::{Failure, head, response::Context},
};

use super::Streaming;

/// Retained request input whose builder starts only after the complete message body arrives.
pub struct Pending<'d, R: route::Spec<Request: route::Own>> {
    head: Parsed<Parts<'d, <R::Request as route::Schema>::Frame>, R::Request>,
    body: buffered::State<owned::Segments<'d>>,
    account: credit::raw::Account,
    connection: head::Connection,
    response: Context,
}

impl<'d, R: route::Spec<Request: route::Own>> Pending<'d, R> {
    pub(super) fn new(
        head: Parsed<Parts<'d, <R::Request as route::Schema>::Frame>, R::Request>,
        open: admit::Track<admit::Open>,
        connection: head::Connection,
        response: Context,
    ) -> Self {
        Self {
            head,
            body: buffered::State::new(open),
            account: credit::raw::Account::new(),
            connection,
            response,
        }
    }

    pub const fn response(&self) -> Context {
        self.response
    }

    pub const fn has_work(&self) -> bool {
        false
    }

    pub fn try_discard<E: credit::Evidence>(
        self,
        parts: owned::Segments<'d>,
        mut proof: E,
    ) -> Result<Self, fixed::Body<'d>> {
        let Self {
            head,
            body,
            mut account,
            connection,
            response,
        } = self;
        let bytes = parts.len();
        let chunks = parts.count();
        let body = account.scope(|mut ledger| {
            // SAFETY: this private body and account began empty together and
            // every successful input turn returns them to this same owner.
            let mut body = unsafe { ledger.bind(body) };
            let limits = credit::Limits::new(body.max_body(), body::DEFAULT_MAX_REQUEST_CHUNKS);
            match body.try_push(
                &mut ledger,
                &mut proof,
                input::Input::new(parts, bytes, chunks),
                limits,
                limits,
                retain,
            ) {
                buffered::Push::Accepted | buffered::Push::Discarded(_) => {}
                buffered::Push::Rejected { error, .. } => {
                    let reply = match error {
                        buffered::Error::Body(error) => Streaming::<R>::body_failure(error),
                        buffered::Error::Retain(error) => Failure::Assembly(error).body(),
                    };
                    return Err(Streaming::<R>::discard(
                        body.into_release(),
                        &mut ledger,
                        proof,
                        reply,
                    ));
                }
            }
            // SAFETY: only this body's successor returns to its original account.
            Ok(unsafe { ledger.store(body) })
        })?;
        Ok(Self {
            head,
            body,
            account,
            connection,
            response,
        })
    }

    pub fn try_retain<E: credit::Evidence>(
        self,
        parts: owned::Segments<'d>,
        proof: E,
    ) -> Result<Self, fixed::Body<'d>> {
        self.try_discard(parts, proof)
    }

    pub fn resume<E: credit::Evidence>(self, _proof: E) -> Result<Self, fixed::Body<'d>> {
        Ok(self)
    }

    pub fn cancel<E: credit::Evidence>(self, proof: E) {
        self.body.abandon(proof);
    }

    pub fn finish<Runtime, Admit, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        rt: &mut Runtime,
        admit: Admit,
        mut proof: E,
    ) -> session::BodyOutcome<'d, super::Ready<'d, R>, D>
    where
        Admit: FnOnce(
            Input<'d, R>,
            usize,
            head::Connection,
            Context,
            &mut Runtime,
        ) -> session::Outcome<'d, D>,
        E: credit::Evidence,
    {
        let Self {
            head,
            body,
            mut account,
            connection,
            response,
        } = self;
        let result = account.scope(|mut ledger| {
            // SAFETY: FIN consumes the body from its original exclusive account.
            let body = unsafe { ledger.bind(body) };
            let body = match body.try_exact(done.body_len) {
                body::Transfer::Accepted(body) => body,
                body::Transfer::Rejected { build: body, error } => {
                    let reply = Streaming::<R>::body_failure(error);
                    return session::Outcome::Close(Streaming::<R>::discard(
                        body.into_release(),
                        &mut ledger,
                        proof,
                        reply,
                    ));
                }
            };
            permit.commit();
            let body = match body.try_finish(&mut ledger, &mut proof) {
                Ok(body) => body,
                Err(failure) => {
                    let reply = Streaming::<R>::body_failure(*failure.error());
                    let release = match failure {
                        buffered::Rejected::Admission { state, .. } => state.into_release(),
                        buffered::Rejected::Storage { release, .. }
                        | buffered::Rejected::Credit { release, .. } => release,
                    };
                    return session::Outcome::Close(Streaming::<R>::discard(
                        release,
                        &mut ledger,
                        proof,
                        reply,
                    ));
                }
            };
            admit(
                Input::Buffered(head.with_body(body)),
                done.consumed,
                connection,
                response,
                rt,
            )
        });
        session::BodyOutcome::Complete(result)
    }
}

fn retain<'d>(
    current: Option<owned::Segments<'d>>,
    input: owned::Segments<'d>,
) -> buffered::Retain<owned::Segments<'d>, owned::Segments<'d>, assembly::Error> {
    match current.unwrap_or_default().try_extend(input) {
        Ok(storage) => buffered::Retain::Accepted {
            storage: Some(storage),
        },
        Err(failure) => {
            let ((storage, input), error) = failure.into_parts();
            buffered::Retain::Rejected {
                storage: Some(storage),
                input,
                error,
            }
        }
    }
}
