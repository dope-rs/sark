use std::num::NonZero;

use dope_fiber::{
    abi,
    task::time::{budget, clock},
};
use sark_client::{Error, Failure, Safety, protocol, reply, timeout};

use crate::client::exchange::{
    dispatch::{Backend, Dispatch},
    request::{Admitted, Headers},
    response::phases,
};

pub(in crate::client) struct Retry<'borrow, 'request, B> {
    backend: B,
    request: &'borrow Admitted<'request>,
    headers: Headers<'request>,
}

impl<'borrow, 'request, B> Retry<'borrow, 'request, B> {
    pub(in crate::client) const fn new(
        backend: B,
        request: &'borrow Admitted<'request>,
        headers: Headers<'request>,
    ) -> Self {
        Self {
            backend,
            request,
            headers,
        }
    }

    pub(in crate::client) fn stream<'scope, 'app, 'd: 'app>(
        self,
        budget: &'scope mut budget::Budget<'d>,
    ) -> impl abi::Fiber<'d, Output = super::Result<B::Stream, B::Cause, B::Mailbox>> + 'scope
    where
        'borrow: 'scope,
        'request: 'scope,
        'app: 'scope,
        B: Backend<'app, 'd> + 'scope,
    {
        let Self {
            backend,
            request,
            headers,
        } = self;
        dope_fiber::fiber!('d => async move {
            let mut retries = protocol::retry::State::new(
                backend.requests().retry(),
                request.method(),
            )
            .with_prior(request.safety());
            loop {
                let dispatched = {
                    let limit = backend.requests().dispatch();
                    let mut scope = budget.scope(clock::Clock::now().await, limit);
                    let check = scope.checkpoint();
                    scope
                        .timeout(Dispatch::new(backend, request, headers, check))
                        .await
                };
                let result = match dispatched {
                    Ok(dispatched) => dispatched,
                    Err(elapsed) => {
                        return Err(retries.finish(Failure::new(
                            Error::Timeout(timeout::Phase::Dispatch.expire(elapsed)),
                            Safety::BeforeSubmit,
                        )));
                    }
                };
                match result {
                    Ok(stream) => {
                        let mut stream = stream.into_inner();
                        phases::Events::attempted(&mut stream, retries.attempt());
                        return Ok(stream);
                    }
                    Err(rejected) => {
                        let failure =
                            Failure::new(rejected.into_cause(), Safety::BeforeSubmit);
                        let backoff = match retries.retry_after(&failure) {
                            Ok(backoff) => backoff,
                            Err(retries) => return Err(retries.finish(failure)),
                        };
                        drop(failure);
                        if let Err(elapsed) = budget.sleep(backoff.delay()).await {
                            return Err(backoff.finish(Failure::new(
                                Error::Timeout(timeout::Phase::Backoff.expire(elapsed)),
                                Safety::BeforeSubmit,
                            )));
                        }
                        retries = backoff.start();
                    }
                }
            }
        })
    }

    pub(in crate::client) fn response<'scope, 'app, 'd: 'app, O>(
        self,
        budget: &'scope mut budget::Budget<'d>,
        observe: &'scope mut O,
    ) -> impl abi::Fiber<
        'd,
        Output = super::Result<(reply::Scoped<'app, 'd>, NonZero<u8>), B::Cause, B::Mailbox>,
    > + 'scope
    where
        'borrow: 'scope,
        'request: 'scope,
        'app: 'scope,
        B: Backend<'app, 'd> + 'scope,
        O: FnMut(&Failure<B::Cause, B::Mailbox>) + 'scope,
    {
        let Self {
            backend,
            request,
            headers,
        } = self;
        dope_fiber::fiber!('d => async move {
            let mut retries = protocol::retry::State::new(
                backend.requests().retry(),
                request.method(),
            )
            .with_prior(request.safety());
            loop {
                let dispatched = {
                    let limit = backend.requests().dispatch();
                    let mut scope = budget.scope(clock::Clock::now().await, limit);
                    let check = scope.checkpoint();
                    scope
                        .timeout(Dispatch::new(backend, request, headers, check))
                        .await
                };
                let result = match dispatched {
                    Ok(dispatched) => dispatched,
                    Err(elapsed) => {
                        return Err(retries.finish(Failure::new(
                            Error::Timeout(timeout::Phase::Dispatch.expire(elapsed)),
                            Safety::BeforeSubmit,
                        )));
                    }
                };
                let failure = match result {
                    Err(rejected) => Failure::new(rejected.into_cause(), Safety::BeforeSubmit),
                    Ok(stream) => {
                        let collected =
                            phases::Head::new(stream.into_inner(), backend.segments());
                        match budget.timeout_for(backend.requests().head_timeout(), collected).await {
                            Ok(Ok((stream, assembly))) => {
                                match budget.timeout(phases::Body::new(stream, assembly)).await {
                                    Ok(Ok(response)) => return Ok((response, retries.attempt())),
                                    Ok(Err(failure)) => failure.widen(),
                                    Err(elapsed) => {
                                        return Err(retries.finish(Failure::new(
                                            Error::Timeout(timeout::Phase::Body.expire(elapsed)),
                                            Safety::PossiblyCommitted,
                                        )));
                                    }
                                }
                            }
                            Ok(Err(failure)) => failure.widen(),
                            Err(elapsed) => Failure::new(
                                Error::Timeout(timeout::Phase::Head.expire(elapsed)),
                                Safety::PossiblyCommitted,
                            ),
                        }
                    }
                };
                let failure = retries.finish(failure);
                let backoff = match retries.retry_after(&failure) {
                    Ok(backoff) => backoff,
                    Err(_) => return Err(failure),
                };
                observe(&failure);
                drop(failure);
                if let Err(elapsed) = budget.sleep(backoff.delay()).await {
                    return Err(backoff.finish(Failure::new(
                        Error::Timeout(timeout::Phase::Backoff.expire(elapsed)),
                        Safety::BeforeSubmit,
                    )));
                }
                retries = backoff.start();
            }
        })
    }
}
