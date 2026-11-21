use std::{cell::Cell, ptr};

use dope_test::alloc::Tracker;
use o3::{alloc, buffer::storage};
use sark_core::body::credit;
use sark_h1::{route::Pending, session, wire::parsing::framing};
use sark_http::payload::{
    compress::{Compression as _, Gzip},
    owned,
};

use crate::common::{Request, fields::DATE};

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

    pub fn pending<'d>(
        &mut self,
        conn: &mut session::Conn<'d>,
        head: framing::Head<'_, '_, Request>,
    ) -> Result<Pending<'d, Request>, session::Outcome<'d>> {
        let mut proof = credit::Disabled::new();
        let response = head.response();
        let mut output = o3::buffer::write::Slice::new(&mut self.write);
        let egress = sark_h1::wire::output::Egress::new(output.reborrow(), DATE, response);
        match head.sync(
            &mut conn.ingress,
            &mut proof,
            &self.state,
            egress,
            &mut self.gzip,
        ) {
            Ok(framing::Progress::Pending(wait)) => Ok(wait.retain(&mut conn.ingress, proof).2),
            Ok(framing::Progress::Complete((used, reply))) => {
                Err(reply.into_consume(used, response.connection()))
            }
            Err(error) => Err(error.into_rejection().into()),
        }
    }

    pub fn finish(&mut self, mut pending: Pending<'_, Request>) -> *const u8 {
        let body = storage::Shared::try_copy(b"z").expect("body storage");
        pending
            .push_body(owned::Segments::from(body), credit::Disabled::new())
            .expect("body");
        let (outcome, allocations) = Tracker::count(|| {
            pending.respond(
                1,
                &self.state,
                o3::buffer::write::Slice::new(&mut self.write).reborrow(),
                DATE,
                &mut self.gzip,
                credit::Disabled::new(),
            )
        });
        assert!(outcome.is_ok());
        assert_eq!(allocations, 0);
        let (pointer, headers, body) = self.state.get();
        assert!(!pointer.is_null());
        assert!(headers && body, "retained request headers and body");
        pointer
    }
}
