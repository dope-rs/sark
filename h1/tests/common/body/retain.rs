use std::{cell::Cell, convert::Infallible, ptr};

use o3::alloc;
use sark_core::body::credit;
use sark_h1::{route::Pending, session, wire::parsing::framing};
use sark_http::payload::compress::{Compression as _, Gzip};

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

    pub fn finish<'d>(
        &mut self,
        pending: Pending<'d, Request>,
    ) -> session::Outcome<'d, Infallible> {
        pending.respond::<_, _, Infallible>(
            session::Complete {
                consumed: 1,
                body_len: 1,
            },
            &self.state,
            o3::buffer::write::Slice::new(&mut self.write).reborrow(),
            DATE,
            &mut self.gzip,
            credit::Disabled::new(),
        )
    }
}
