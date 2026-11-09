use consume::Consume;
use o3::buffer::write::consume;
use sark_http::h1::head::{Limit, ParseError, request};

use crate::{
    server::{ConnState, Error, Opening, Phase, Response, app::Sink},
    upgrade::request::Admission,
};

const MAX_BYTES: usize = 16 * 1024;

pub(super) fn drive(
    path: &str,
    state: &mut ConnState,
    out: &mut Response<'_>,
    user: &impl Sink,
) -> Result<(), consume::Error> {
    match admit(path, state.acc.as_ref()) {
        Ok(None) => {
            let error = Error::OpenTail {
                actual: state.acc.len(),
            };
            state.phase = reject(out, Response::REJECTION, error, user)?;
        }
        Err(error) => {
            state.phase = reject(out, Response::REJECTION, error, user)?;
        }
        Ok(Some((tail, accept, opening))) => {
            let head = state.acc.len() - tail.len();
            state.phase = upgrade(user, opening, out, &accept)?;
            state.acc.consume_max(head);
        }
    }
    Ok(())
}

pub(super) const fn too_large(actual: usize) -> Error {
    Error::Open(ParseError::too_large(actual, MAX_BYTES))
}

/// Admits the opening prefix before retaining bytes, independently of output capacity.
pub(super) fn check(state: &mut ConnState, chunk: &[u8]) -> Result<(), Error> {
    let buffered = state.acc.as_ref();
    // Every retained incomplete prefix was admitted by this check or the head parser.
    let remaining = MAX_BYTES - buffered.len();
    let bounded = &chunk[..chunk.len().min(remaining)];
    let complete = (1..4).any(|prefix| {
        buffered.ends_with(&b"\r\n\r\n"[..prefix]) && bounded.starts_with(&b"\r\n\r\n"[prefix..])
    }) || bounded.windows(4).any(|window| window == b"\r\n\r\n");
    if complete {
        state.phase = Phase::Opening;
        return Ok(());
    }
    if chunk.len() > remaining {
        return Err(too_large(buffered.len() + chunk.len()));
    }
    Ok(())
}

pub(super) fn admit<'input>(
    path: &str,
    input: &'input [u8],
) -> Result<Option<(&'input [u8], [u8; 28], Opening<'input>)>, Error> {
    let mut fields = Admission::default();
    let parsed = request::Parts::parse(input, Limit::DEFAULT.with_bytes(MAX_BYTES), |field| {
        fields.field(field.name(), field.value(), field.value_span())
    })
    .map_err(Error::Open)?;
    let Some(head) = parsed else {
        return Ok(None);
    };
    let (method, target) = head.line();
    let accept = fields
        .finish(
            method,
            target,
            head.version() as u8,
            head.admission().body(),
            head.connection(),
            path.as_bytes(),
        )
        .map_err(Error::Handshake)?;
    let route = head.into_route();
    let [origin, cookie] = [fields.origin(), fields.cookie()]
        .map(|span| span.map(|span| span.get(input).unwrap_or_else(|| unreachable!("opening"))));
    Ok(Some((
        route.body(),
        accept,
        Opening {
            authority: route.authority(),
            target: route.target(),
            origin,
            cookie,
        },
    )))
}

/// Runs only after output reservation, so a deferred write cannot repeat admission.
pub(super) fn upgrade(
    user: &impl Sink,
    opening: Opening<'_>,
    out: &mut Response<'_>,
    accept: &[u8; 28],
) -> Result<Phase, consume::Error> {
    if let Err(error) = user.opening(opening) {
        return reject(out, Response::FORBIDDEN, error, user);
    }
    out.upgrade(accept)?;
    Ok(Phase::Active)
}

fn reject(
    out: &mut Response<'_>,
    response: &[u8],
    cause: Error,
    user: &impl Sink,
) -> Result<Phase, consume::Error> {
    let result = out.put_raw(response);
    user.failed(cause);
    result?;
    Ok(Phase::Closed)
}
