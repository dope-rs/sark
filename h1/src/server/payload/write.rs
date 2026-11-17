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
use o3::{
    buffer::{resident, resident::bound},
    mem::slot,
};
use sark_http::payload::fixed;
use write::{body, body::Admit};

use crate::{pump, server::payload};

type Bound<'d, K, G> =
    data::Bound<compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>, G>;
type Prepared<'d, K, G> =
    Result<Option<Value<Bound<'d, K, G>>>, egress::Excess<payload::Body<'d, K>>>;

/// Retains an exact rejected body until its native reservation is released.
pub fn try_prepare<'d, K: 'd, G: bound::Bound>(
    egress: pump::Egress<'d>,
    tag: K,
) -> Prepared<'d, K, G> {
    let body = match egress {
        pump::Egress::None | pump::Egress::Wake | pump::Egress::Inline => None,
        pump::Egress::Static { body } => Some(payload::Body::Shared {
            owner: resident::Shared::from(body.as_slice()),
            tag,
        }),
        pump::Egress::Shared { body } => Some(payload::Body::Shared {
            owner: resident::Shared::from(body),
            tag,
        }),
        pump::Egress::Frozen { body } => Some(payload::Body::Frozen { owner: body, tag }),
        pump::Egress::Segmented { body } => {
            body.take().map(|body| payload::Body::duplex(body, tag))
        }
    };
    match body {
        Some(body) => Ok(Some(Value::Unchecked(
            egress::Load::new(body).try_bound::<G>()?.bound_left(),
        ))),
        None => Ok(None),
    }
}

/// Consumes native admission before releasing a rejected body.
pub fn try_publish<'d, K: 'd, G: bound::Bound + 'd, ID: Identity, W, C>(
    native: slot::Occupied<
        '_,
        write::Output<
            '_,
            '_,
            '_,
            '_,
            '_,
            '_,
            '_,
            'd,
            ID,
            W,
            C,
            Bound<'d, K, G>,
            egress::reserve::Bounds,
            G,
        >,
    >,
    body: Prepared<'d, K, G>,
    end: write::End,
) -> Result<(), body::Failure>
where
    W: wire::Spec + 'd,
{
    let body = match body {
        Ok(body) => body,
        Err(cause) => {
            drop(native);
            return Err(cause.into());
        }
    };
    match body {
        Some(body) => write::Output::try_publish(native, body, end),
        None => write::Output::finish(native, end),
    }
    .map_err(|cause| body::Failure::Mismatch(cause.into_cause()))
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
