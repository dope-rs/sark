//! Request-side ownership for one live gRPC call.

use std::mem;

use o3::buffer::storage;
use sark_core::life::session;

use crate::{message, metadata, protocol::request, server::call::chunks};

pub(in crate::server) struct Input<C: session::Storage = storage::Shared> {
    pub(in crate::server) head: Option<request::Head>,
    pub(in crate::server) deframer: message::Deframer<C>,
    pub(in crate::server) data: chunks::State,
    pub(in crate::server) trailers: metadata::Decoded,
}

pub(in crate::server) enum Turn<C: session::Storage = storage::Shared> {
    Frame(message::Frame<C>),
    Pending,
    Complete,
    Incomplete,
    Idle,
}

const _: () = {
    assert!(mem::size_of::<Turn>() <= mem::size_of::<(message::Frame<storage::Shared>, usize)>());
    assert!(mem::align_of::<Turn>() == mem::align_of::<storage::Shared>());
};

impl<C: session::Storage> Input<C> {
    pub(in crate::server::call) fn new(head: Option<request::Head>, limit: usize) -> Self {
        Self {
            head,
            deframer: message::Deframer::new(limit),
            data: chunks::State::default(),
            trailers: metadata::Decoded::new(),
        }
    }
}
