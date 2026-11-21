//! HTTP/1 projection into the final-link transaction.

use conn::write;
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

impl<'d> pump::Egress<'d> {
    #[inline(always)]
    pub fn into_body<K: Copy>(self, tag: K) -> Option<payload::Body<'d, K>> {
        match self {
            Self::None | Self::Wake | Self::Inline => None,
            Self::Static { body } => Some(payload::Body::Shared {
                owner: resident::Shared::from(body.as_slice()),
                tag,
            }),
            Self::Shared { body } => Some(payload::Body::Shared {
                owner: resident::Shared::from(body),
                tag,
            }),
            Self::Frozen { body } => Some(payload::Body::Frozen { owner: body, tag }),
            Self::Segmented { body } => body.take().map(|body| payload::Body::duplex(body, tag)),
        }
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
            data::Bound<payload::Owner<'d, K>, G>,
            egress::reserve::Bounds,
            G,
        >,
    >,
    body: Option<payload::Body<'d, K>>,
    connection: head::Connection,
) -> Result<(), body::Failure>
where
    W: wire::Spec + 'd,
{
    let end = match connection {
        head::Connection::KeepAlive => write::End::Open,
        head::Connection::Close => write::End::Close,
    };
    match body {
        Some(body) => {
            let load = match egress::Load::new(body).try_bound::<G>() {
                Ok(load) => load,
                Err(cause) => {
                    drop(native);
                    return Err(cause.into());
                }
            };
            write::Output::try_publish(native, Value::Unchecked(load.bound_left()), end)
        }
        None => write::Output::finish(native, end),
    }
    .map_err(|cause| body::Failure::Mismatch(cause.into_cause()))
}
