//! HTTP/1 projection into the final-link transaction.

use conn::write;
use data::compose;
use dope::{
    core::driver::route::Identity,
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
use write::body;

use crate::{pump, server::payload, wire::head};

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
    connection: head::Connection,
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
    let end = match connection {
        head::Connection::KeepAlive => write::End::Open,
        head::Connection::Close => write::End::Close,
    };
    match body {
        Some(body) => write::Output::try_publish(native, body, end),
        None => write::Output::finish(native, end),
    }
    .map_err(|cause| body::Failure::Mismatch(cause.into_cause()))
}
