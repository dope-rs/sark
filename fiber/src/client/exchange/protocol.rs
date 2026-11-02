use std::convert::Infallible;

use dope::{
    core::driver::{route::Identity, schedule::clock::timer},
    manifold::client::{port, session},
};
use o3::buffer::{pool, resident, segments};
use reply::decode::Decoder;
use sark_client::{protocol::codec::Codec, reply};
use session::ingress;

use crate::client::{
    config::report,
    exchange::{net, request},
};

pub(in crate::client) struct Protocol<
    'f,
    'e,
    'b,
    'c,
    'd,
    ID: Identity,
    Tag = (),
    E = Infallible,
    F = report::Never,
> where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) io: port::Port<'e, 'd, pool::Cursor, ID>,
    pub(in crate::client) codec: Codec,
    pub(in crate::client) decode: Decoder<'d>,
    pub(in crate::client) ingress: ingress::Plan<ingress::Local>,
    pub(in crate::client) transport: net::Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
    pub(in crate::client) requests: request::Request,
    pub(in crate::client) response_segments: segments::Pool<resident::Shared<'d>>,
    pub(in crate::client) timer: timer::Timer<'d>,
}
