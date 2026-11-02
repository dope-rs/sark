use o3::buffer::resident;
use sark_core::manifold::stream::Boundary;
use sark_http::{
    field::{Response, metadata::section::rules},
    payload::response::{headers::Fields, status::Code},
};

use crate::{
    conn::{self, event, machine},
    server::response::Head,
    stream,
};

/// Canonical empty-status response transaction.
#[doc(hidden)]
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Status {
    code: Code,
}

impl Status {
    #[doc(hidden)]
    pub const fn new(code: Code) -> Self {
        Self { code }
    }

    #[doc(hidden)]
    pub(crate) fn prepare<'d, F: rules::Policy, E: event::Evidence>(
        self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        boundary: Boundary,
    ) -> Result<Head<'d>, conn::Error> {
        conn.streams().headers().prepare(
            stream,
            Response::new(self.code, Fields::new(())),
            boundary,
        )?;
        Ok(Head::empty(stream))
    }
}
