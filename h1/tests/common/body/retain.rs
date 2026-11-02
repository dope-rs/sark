use std::{cell::Cell, convert::Infallible, ptr};

use o3::alloc;
use sark_core::body::credit;
use sark_h1::{
    route::sync,
    session,
    wire::{cache, output, parsing::framing},
};
use sark_http::{
    h1::head::request::Coding,
    payload::compress::{Compression as _, Gzip},
};

use crate::common::{
    Request,
    fields::{DATE, RESPONSE},
};

/// Exclusively owned response resources for the retained-request fixtures.
pub struct Retain {
    gzip: Gzip,
    write: [u8; 512],
    pub state: Cell<(*const u8, bool, bool)>,
}

impl Retain {
    pub const CASES: [(usize, usize, bool, usize); 3] = [
        (0, Request::HEAD.len(), false, 1),
        (128, 128 + Request::HEAD.len(), true, 0),
        (128, 128 * 1024, true, 1),
    ];

    pub fn try_new() -> Result<Self, alloc::Error> {
        Ok(Self {
            gzip: Gzip::try_new()?,
            write: [0; 512],
            state: Cell::new((ptr::null(), false, false)),
        })
    }

    pub fn dispatch<'d>(
        &mut self,
        conn: &mut session::Conn<'d>,
        permit: session::Permit,
        head: framing::Head<'_, '_, Request, Coding>,
    ) -> session::RouteOutcome<'d, sync::Pending<'d, Request>> {
        sync::Endpoint::new(
            sync::Egress::new(
                output::Egress::new(&mut self.write, DATE, head.response()),
                cache::Cache::empty(),
                &mut self.gzip,
            ),
            conn,
        )
        .dispatch::<Request, _, _>(permit, head, &self.state, credit::Disabled::new())
    }

    pub fn finish<'d>(
        &mut self,
        pending: sync::Pending<'d, Request>,
    ) -> session::Outcome<'d, Infallible> {
        pending.finish::<_, _, Infallible>(
            session::Complete {
                consumed: 1,
                body_len: 1,
            },
            session::Permit::new(),
            &self.state,
            sync::Egress::new(
                output::Egress::new(&mut self.write, DATE, RESPONSE),
                cache::Cache::empty(),
                &mut self.gzip,
            ),
            credit::Disabled::new(),
        )
    }
}
