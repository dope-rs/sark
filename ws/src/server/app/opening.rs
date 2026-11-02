use consume::Consume;
use o3::buffer::write::consume;
use sark_http::{
    field,
    h1::{
        codec::decode::fields::Sink as _,
        head::{Limit, ParseError, request},
    },
};

use crate::{
    server::{ConnState, Error, Opening, Phase, Response, app::Sink},
    upgrade::{self, request::Admission},
};

const MAX_BYTES: usize = 16 * 1024;

pub(super) enum Outcome<'input> {
    NeedMore,
    Accept {
        tail: &'input [u8],
        accept: [u8; 28],
        opening: Opening<'input>,
    },
    Reject(Error),
}

pub(super) const REJECTION: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nSec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n";

pub(super) fn drive(
    path: &str,
    state: &mut ConnState,
    out: &mut Response<'_>,
    user: &impl Sink,
) -> Result<(), consume::Error> {
    if !state.handshake_complete {
        return Ok(());
    }
    match admit(path, state.acc.as_ref()) {
        Outcome::NeedMore => {
            let error = Error::OpenTail {
                actual: state.acc.len(),
            };
            state.phase = reject(out, REJECTION, error, user)?;
        }
        Outcome::Reject(error) => {
            state.phase = reject(out, REJECTION, error, user)?;
        }
        Outcome::Accept {
            tail,
            accept,
            opening,
        } => {
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
    if state.handshake_complete {
        return Ok(());
    }
    let buffered = state.acc.as_ref();
    // Every retained incomplete prefix was admitted by this check or the head parser.
    let remaining = MAX_BYTES - buffered.len();
    let bounded = &chunk[..chunk.len().min(remaining)];
    let complete = (1..4).any(|prefix| {
        buffered.ends_with(&b"\r\n\r\n"[..prefix]) && bounded.starts_with(&b"\r\n\r\n"[prefix..])
    }) || bounded.windows(4).any(|window| window == b"\r\n\r\n");
    if complete {
        state.handshake_complete = true;
        return Ok(());
    }
    if chunk.len() > remaining {
        return Err(too_large(buffered.len() + chunk.len()));
    }
    Ok(())
}

pub(super) fn admit<'input>(path: &str, input: &'input [u8]) -> Outcome<'input> {
    let mut fields = Admission::default();
    let mut origin = None;
    let mut cookie = None;
    let parsed = request::Parts::parse(input, Limit::DEFAULT.with_bytes(MAX_BYTES), |field| {
        let (slot, header) = if field.name().eq_ignore_ascii_case(b"origin") {
            (&mut origin, upgrade::Header::Origin)
        } else if field.name().eq_ignore_ascii_case(b"cookie") {
            (&mut cookie, upgrade::Header::Cookie)
        } else {
            return fields.entry(field);
        };
        if slot
            .replace(field::Error::trim_ows(field.value()))
            .is_some()
        {
            Err(upgrade::Error::Duplicate(header))
        } else {
            Ok(())
        }
    });
    let head = match parsed {
        Ok(Some(head)) => head,
        Ok(None) => return Outcome::NeedMore,
        Err(error) => return Outcome::Reject(Error::Open(error)),
    };
    let (method, target) = head.line();
    let accept = match fields.finish(
        method,
        target,
        head.version() as u8,
        head.admission().body(),
        head.connection(),
        path.as_bytes(),
    ) {
        Ok(accept) => accept,
        Err(error) => return Outcome::Reject(Error::Handshake(error)),
    };
    let route = head.into_route();
    Outcome::Accept {
        tail: route.body(),
        accept,
        opening: Opening {
            authority: route.authority(),
            target: route.target(),
            origin,
            cookie,
        },
    }
}

/// Runs only after output reservation, so a deferred write cannot repeat admission.
pub(super) fn upgrade(
    user: &impl Sink,
    opening: Opening<'_>,
    out: &mut Response<'_>,
    accept: &[u8; 28],
) -> Result<Phase, consume::Error> {
    if let Err(error) = user.opening(opening) {
        return reject(
            out,
            b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            error,
            user,
        );
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
