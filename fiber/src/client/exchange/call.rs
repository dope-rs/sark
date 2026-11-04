use std::{num::NonZero, time::Duration};

use dope::core::driver::schedule::clock::Moment;
use dope_fiber::{
    abi,
    task::time::{budget, clock},
};
use sark_client::{Error, Failure, Safety, reply};
use sark_http::method::Method;

use crate::client::{
    exchange::{
        Retry,
        dispatch::Backend,
        request::{Admitted, Headers},
        response::decode,
    },
    process,
};

pub(in crate::client) struct Call<'call, B> {
    backend: B,
    method: Method,
    path: &'call str,
    headers: &'call [(&'call str, &'call str)],
    body: &'call [u8],
}

impl<'call, B> Call<'call, B> {
    pub(in crate::client) const fn new(
        backend: B,
        method: Method,
        path: &'call str,
        headers: &'call [(&'call str, &'call str)],
        body: &'call [u8],
    ) -> Self {
        Self {
            backend,
            method,
            path,
            headers,
            body,
        }
    }

    fn prepare<'app, 'd>(
        self,
        now: Moment<'d>,
        follows: u32,
    ) -> super::Result<(B, budget::Budget<'d>, Headers<'call>, Admitted<'call>), B::Cause, B::Mailbox>
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd>,
    {
        let Self {
            backend,
            method,
            path,
            headers,
            body,
        } = self;
        let budget = budget::Budget::try_new(backend.timer(), now, backend.requests().timeout())
            .map_err(Error::Timer)
            .map_err(|cause| Failure::new(cause, Safety::BeforeSubmit))?;
        let headers = backend
            .requests()
            .admit(headers)
            .map_err(|cause| Failure::new(cause.generalize().widen(), Safety::BeforeSubmit))?;
        let request =
            Admitted::try_new(follows, backend.requests().origin(), path, method, body)
                .map_err(|cause| Failure::new(cause.generalize().widen(), Safety::BeforeSubmit))?;
        Ok((backend, budget, headers, request))
    }

    pub(in crate::client) fn scoped<'app, 'd, const FOLLOW: bool>(
        self,
    ) -> impl abi::Fiber<'d, Output = super::Result<reply::Scoped<'app, 'd>, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
    {
        self.collected::<FOLLOW, false, _, _, _>(|_| {}, |response, _, _| Ok(response))
    }

    pub(in crate::client) fn process<'app, 'd, T, E, F, O>(
        self,
        observe: O,
        process: F,
    ) -> impl abi::Fiber<'d, Output = super::Result<Result<T, E>, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
        O: FnMut(&Failure<B::Cause, B::Mailbox>) + 'call,
        F: FnOnce(
                reply::Scoped<'app, 'd>,
                budget::Check<'d>,
                NonZero<u8>,
            ) -> Result<T, process::Error<E>>
            + 'call,
        T: 'call,
        E: 'call,
    {
        self.collected::<true, true, _, _, _>(
            observe,
            move |response, bound, attempt| match process(response, bound, attempt) {
                Ok(value) => {
                    bound
                        .check()
                        .map_err(|elapsed| decode::expired(elapsed).attempted(attempt).widen())?;
                    Ok(Ok(value))
                }
                Err(process::Error::Work(error)) => Ok(Err(error)),
                Err(process::Error::Elapsed(elapsed)) => {
                    Err(decode::expired(elapsed).attempted(attempt).widen())
                }
            },
        )
    }

    fn collected<'app, 'd, const FOLLOW: bool, const PROCESS: bool, T, F, O>(
        self,
        mut observe: O,
        complete: F,
    ) -> impl abi::Fiber<'d, Output = super::Result<T, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
        O: FnMut(&Failure<B::Cause, B::Mailbox>) + 'call,
        F: FnOnce(
                reply::Scoped<'app, 'd>,
                budget::Check<'d>,
                NonZero<u8>,
            ) -> super::Result<T, B::Cause, B::Mailbox>
            + 'call,
        T: 'call,
    {
        dope_fiber::fiber!('d => async move {
            let follows = if FOLLOW { self.backend.requests().redirects() } else { 0 };
            let (backend, mut budget, headers, mut request) =
                self.prepare(clock::Clock::now().await, follows)?;
            loop {
                let (response, attempt) = Retry::new(backend, &request, headers)
                    .response(&mut budget, &mut observe)
                    .await?;
                if FOLLOW
                    && let Some(next) = request
                        .response(response.status(), || response.headers().get("location"))
                        .map_err(|failure| failure.attempted(attempt).generalize().widen())?
                {
                    request = next;
                    continue;
                }
                let duration = backend.requests().decode();
                let mut scope = budget.scope(clock::Clock::now().await, duration);
                let bound = scope.checkpoint();
                let check = if PROCESS || duration != Duration::MAX { Some(bound) } else { None };
                let decode = decode::scoped::<B::Cause>(backend.decoder(), response, check);
                let response = match scope.timeout(decode).await {
                    Ok(response) => response,
                    Err(elapsed) => Err(decode::expired(elapsed)),
                }
                .map_err(|failure| failure.attempted(attempt).widen())?;
                if check.is_none() {
                    bound.check()
                        .map_err(|elapsed| decode::expired(elapsed).attempted(attempt).widen())?;
                }
                return complete(response, bound, attempt);
            }
        })
    }

    pub(in crate::client) fn retained<'app, 'd>(
        self,
    ) -> impl abi::Fiber<'d, Output = super::Result<reply::Retained, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
    {
        self.collected::<true, false, _, _, _>(
            |_| {},
            |response, _, attempt| {
                response.try_retain().map_err(|cause| {
                    Failure::new(Error::Detach(cause), Safety::PossiblyCommitted).attempted(attempt)
                })
            },
        )
    }

    pub(in crate::client) fn contiguous<'app, 'd>(
        self,
    ) -> impl abi::Fiber<'d, Output = super::Result<reply::Contiguous, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
    {
        self.collected::<true, false, _, _, _>(
            |_| {},
            |response, _, attempt| {
                response.try_flatten().map_err(|cause| {
                    Failure::new(Error::Detach(cause), Safety::PossiblyCommitted).attempted(attempt)
                })
            },
        )
    }

    pub(in crate::client) fn stream<'app, 'd>(
        self,
    ) -> impl abi::Fiber<'d, Output = super::Result<B::Stream, B::Cause, B::Mailbox>> + 'call
    where
        'd: 'app + 'call,
        'app: 'call,
        B: Backend<'app, 'd> + 'call,
    {
        dope_fiber::fiber!('d => async move {
            let (backend, mut budget, headers, request) =
                self.prepare(clock::Clock::now().await, 0)?;
            Retry::new(backend, &request, headers)
                .stream(&mut budget)
                .await
        })
    }
}
