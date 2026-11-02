use std::pin;

use dope_fiber::{abi, context, task::time::budget};
use reply::decode::Decoder;
use sark_client::{Error, Failure, Safety, reply, timeout};
use sark_http::payload::compress::inflate::Checked;

pub(in crate::client) fn scoped<'app, 'd, E>(
    decoder: &'app Decoder<'d>,
    response: reply::Scoped<'app, 'd>,
    check: Option<budget::Check<'d>>,
) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Failure<E>>> {
    let mut response = Some(response);
    abi::poll_fn(
        move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
            use std::task::Poll;

            let Some(response) = response.take() else {
                return Poll::Pending;
            };
            let token = context.as_mut().region_token();
            let result = match check {
                Some(check) => decoder.decode_with(token, response, || check.check()),
                None => decoder.decode(token, response).map_err(Checked::Work),
            };
            Poll::Ready(result.map_err(|error| match error {
                Checked::Work(cause) => Failure::new(cause.generalize(), Safety::PossiblyCommitted),
                Checked::Check(elapsed) => expired(elapsed),
            }))
        },
    )
}

pub(in crate::client) fn expired<E>(elapsed: budget::Elapsed) -> Failure<E> {
    Failure::new(
        Error::Timeout(timeout::Phase::Decode.expire(elapsed)),
        Safety::PossiblyCommitted,
    )
}
