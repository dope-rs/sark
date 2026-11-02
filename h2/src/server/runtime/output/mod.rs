use dope::{
    manifold::network::listener::conn::write::{self, error},
    net::link::egress::{self, load},
};
use o3::{
    base::num::bounded,
    buffer::{bytes::bounded::Overflow, resident},
};
use sark_core::manifold::client::io;
use sark_http::field::metadata::section::rules;

use crate::{
    ServerRole,
    conn::{event, machine},
};

pub(in crate::server) mod flush;
pub(in crate::server) mod track;

/// Exact output failure after rejected payload owners and receipts are released.
#[derive(Debug)]
pub enum Failure {
    Capacity(write::Reject),
    Input(io::Error),
    Prefix(bounded::Error),
    Rejected(load::Error),
    Overflow(Overflow),
    Mismatch(error::Cause),
}

/// One output failure with its unmodified owner and retirement receipt.
enum Error<B, E, R> {
    Input(io::Error),
    /// A refused owner retained until its bound failure and receipt are retired.
    Bound {
        cause: egress::Excess<B>,
        receipt: R,
    },
    Commit(error::Build<E, R>),
}

pub(in crate::server) trait State<'d> {
    type Fields: rules::Policy;
    type Evidence: event::Evidence;

    fn protocol(
        &mut self,
    ) -> &mut machine::Connection<
        ServerRole,
        Self::Fields,
        resident::Shared<'d>,
        rules::RejectPush,
        Self::Evidence,
    >;

    fn settle(&mut self) -> flush::End;
}
