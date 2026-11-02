use core::convert::Infallible;
use o3::buffer::resident;
use sark_core::{body::credit, duplex, manifold::schedule::typed};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, machine},
    server::task::response,
    stream,
};

pub(in crate::server::task) struct Slot<'a, 'd, P, F: rules::Policy, X, D: duplex::Session<'d>> {
    pub(in crate::server::task) connection:
        &'a mut machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    pub(in crate::server::task) response: typed::Vacant<
        'a,
        stream::Key,
        response::Response<'d, P, X, D>,
        response::Data<'d, D>,
        Infallible,
        conn::DataBlock,
    >,
    pub(in crate::server::task) body_credit: &'a mut credit::raw::Account,
}
