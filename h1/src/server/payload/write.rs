//! HTTP/1 projection into the final-link transaction.

use conn::write;
use data::compose;
use dope::{
    core::driver::{self, route::Identity},
    manifold::network::listener::conn,
    net::{
        link::{
            egress,
            egress::{data, load::Value},
        },
        wire,
    },
};
use o3::buffer::{hold, resident, resident::bound};
use sark_core::life::{self, exchange, sequence};
use sark_http::payload::fixed;
use write::{body, body::Admit, error};

use crate::{
    pump,
    server::{deadline, payload, task::turn},
    wire::{head, output},
};

type Bound<'d, K, G> =
    data::Bound<compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>, G>;
type Rejected<'d, K, R> = (egress::Excess<payload::Body<'d, K>>, R);
type DraftResult<'d, K, G, R> = Result<write::Draft<Bound<'d, K, G>, R>, Rejected<'d, K, R>>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::server) enum Wake {
    None,
    Pending,
}

/// Protocol effects returned after the final link owns the response.
pub struct CycleCommit<K, H> {
    pub(in crate::server) progress: deadline::Progress,
    pub(in crate::server) discard_remaining: usize,
    pub state: Result<(), pump::Reject>,
    pub(in crate::server) transport: Option<K>,
    pub(in crate::server) wake: Wake,
    pub tail: pump::Tail<H>,
}

/// Final-link failure before or after response production.
pub enum Failure<R> {
    Capacity(write::Reject),
    Rejected { cause: body::Failure, receipt: R },
}

const _: () = assert!(core::mem::size_of::<Wake>() == 1);

fn try_finish<'d, K: 'd, G: bound::Bound, R>(
    egress: pump::Egress<'d>,
    tag: K,
    connection: head::Connection,
    receipt: R,
) -> DraftResult<'d, K, G, R> {
    let (written, body) = match egress {
        pump::Egress::None | pump::Egress::Wake => (0, None),
        pump::Egress::Inline { prefix } => (prefix, None),
        pump::Egress::Static { prefix, body } => (
            prefix,
            Some(payload::Body::Shared {
                owner: resident::Shared::from(body.as_slice()),
                tag,
            }),
        ),
        pump::Egress::Shared { prefix, body } => (
            prefix,
            Some(payload::Body::Shared {
                owner: resident::Shared::from(body),
                tag,
            }),
        ),
        pump::Egress::Frozen { prefix, body } => {
            (prefix, Some(payload::Body::Frozen { owner: body, tag }))
        }
        pump::Egress::Segmented { prefix, body } => (
            prefix,
            body.take().map(|body| payload::Body::duplex(body, tag)),
        ),
    };
    let body = match body {
        Some(body) => match egress::Load::new(body).try_bound::<G>() {
            Ok(body) => Some(Value::Unchecked(body.bound_left())),
            Err(cause) => return Err((cause, receipt)),
        },
        None => None,
    };
    let draft = write::Draft::new(written, body, receipt);
    Ok(match connection {
        head::Connection::KeepAlive => draft,
        head::Connection::Close => draft.close_after(),
    })
}

/// Projects one pump cycle into a recoverable final-link draft.
pub fn try_cycle<'d, K: Copy + Default + 'd, G: bound::Bound, H>(
    cycle: pump::Cycle<'d, H>,
    tagger: impl FnOnce(hold::Charge) -> K,
) -> DraftResult<'d, K, G, CycleCommit<K, H>> {
    let charge = match cycle.state.as_ref() {
        Ok(pump::Egress::Shared { body, .. }) => output::Owner::shared(body).charge,
        Ok(pump::Egress::Frozen { body, .. }) => output::Owner::shared(body).charge,
        Ok(pump::Egress::Segmented { body, .. }) => body.charge(),
        _ => hold::Charge::default(),
    };
    let (tag, tx) = if charge.resident_bytes == 0 {
        (K::default(), None)
    } else {
        let tag = tagger(charge);
        (tag, Some(tag))
    };
    let (egress, state) = match cycle.state {
        Ok(egress) => (egress, Ok(())),
        Err(error) => (pump::Egress::None, Err(error)),
    };
    let wake = match &egress {
        pump::Egress::Wake => Wake::Pending,
        _ => Wake::None,
    };
    try_finish(
        egress,
        tag,
        cycle.connection,
        CycleCommit {
            progress: cycle.progress,
            discard_remaining: cycle.discard_remaining,
            state,
            transport: tx,
            wake,
            tail: cycle.tail,
        },
    )
}

/// Projects one output decision into a recoverable final-link draft.
pub fn try_outcome<'d, G: bound::Bound, C: Copy, M: sequence::Mode<C>, R>(
    outcome: output::Outcome<'d>,
    life: &mut M,
    exchange: exchange::Id<C, u64>,
    settle: impl FnOnce(head::Connection, turn::Completion<M::Retention>) -> R,
) -> DraftResult<'d, M::TransportTag, G, R>
where
    M::Retention: turn::Mode,
    M::TransportTag: 'd,
{
    if let output::Outcome::Failed(error) = &outcome {
        life::Mode::diagnostic(life, error);
    }
    let held = outcome.retention();
    let charge = match held {
        output::Retention::Response(owner) | output::Retention::Linked(owner) => owner.charge,
        output::Retention::Rejected => hold::Charge::default(),
    };
    let done = <M::Retention as turn::Mode>::complete(held);
    if let Some(sink) = life::Mode::observer(life)
        && let output::Copy::Copied(copy) = outcome.copy()
    {
        life::Observer::observe(
            sink,
            life::Event::ResponseBodyCopied {
                exchange,
                read: copy.read,
                written: copy.written,
                cause: copy.cause,
            },
        );
    }
    let (egress, conn) = pump::Egress::output(outcome);
    let receipt = settle(conn, done);
    let tag = if charge.resident_bytes == 0 {
        M::TransportTag::default()
    } else {
        sequence::Mode::tag(life, exchange, charge)
    };
    try_finish(egress, tag, conn, receipt)
}

/// Releases rejected physical owners and returns the exact route action.
pub fn reject<'d, K: 'd, G: bound::Bound, R>(
    cause: error::Error<Bound<'d, K, G>, R, Rejected<'d, K, R>>,
) -> Failure<R> {
    match cause {
        error::Error::Capacity(cause) => Failure::Capacity(cause),
        error::Error::Build((cause, receipt)) => Failure::Rejected {
            cause: cause.into(),
            receipt,
        },
        error::Error::Mismatch(error) => {
            let (cause, receipt) = error.into_parts();
            Failure::Rejected {
                cause: body::Failure::Mismatch(cause),
                receipt,
            }
        }
    }
}

/// Returns the same immutable borrow when its projection remains unadmitted.
pub(in crate::server) fn send_static<
    'd,
    K: Default + 'd,
    ID: Identity,
    W,
    C,
    G: bound::Bound + 'd,
>(
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, Bound<'d, K, G>>,
    body: fixed::Body<'d>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<fixed::Body<'d>>
where
    W: wire::Spec,
{
    let reason = body;
    let body = body.as_slice();
    let body = payload::Body::Shared {
        owner: resident::Shared::from(body),
        tag: K::default(),
    };
    let body = match egress::NonEmpty::try_new(body) {
        Ok(body) => body,
        Err(cause) => {
            conn.write().reject(body::Failure::Payload(cause));
            return None;
        }
    };
    let body = match body.into_left().try_bound::<G>() {
        Ok(body) => body,
        Err(rejected) => {
            let cause = {
                let parts = rejected.into_parts();
                body::Failure::Bound {
                    charge: parts.0.charge(),
                    maximum: parts.1.get(),
                }
            };
            conn.write().reject(cause);
            return None;
        }
    };
    let rejected = match conn.write().admit(body, driver) {
        Admit::Committed | Admit::Closed(_) => None,
        Admit::Deferred { .. } => {
            return Some(reason);
        }
        Admit::Rejected { cause, .. } => Some(body::Failure::Capacity(cause)),
        Admit::Unavailable(cause) => {
            let parts = cause.into_parts();
            Some(body::Failure::Reserve(parts.1))
        }
    };
    if let Some(cause) = rejected {
        conn.write().reject(cause);
    }
    None
}
