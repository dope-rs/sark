use o3::buffer::{self, pool, write::consume::Consume as _};
use sark_core::manifold::{
    client::{self, io},
    stream::Boundary,
};

use crate::{
    Reason,
    client::state::output::{Output, Pending, RequestBody},
    details::RequestError,
    message,
};

pub(super) struct Buffer<'a, 'p> {
    body: &'a [u8],
    pool: &'p pool::Pool,
}

impl<'a, 'p> Buffer<'a, 'p> {
    pub(super) fn new(body: &'a [u8], pool: &'p pool::Pool) -> Self {
        Self { body, pool }
    }
}

impl<'a> AsRef<[u8]> for Buffer<'a, '_> {
    fn as_ref(&self) -> &'a [u8] {
        self.body
    }
}

impl RequestBody for Buffer<'_, '_> {
    fn try_send<C: client::Connection>(
        &self,
        conn: &mut C,
        key: C::Key,
        header: &[u8; message::HEADER_LEN],
        boundary: Boundary,
    ) -> Result<io::Outcome<C::Block>, C::Error> {
        conn.send(key, header, self.body, 0, boundary)
    }

    fn try_advance(&mut self, _total: usize, used: usize) -> Result<usize, RequestError> {
        let mut view = buffer::View::new(self.body);
        view.try_consume(used.saturating_sub(message::HEADER_LEN))
            .map_err(RequestError::Progress)?;
        self.body = view.as_slice();
        Ok(used)
    }

    fn into_pending(
        self,
        header: [u8; message::HEADER_LEN],
        _total: usize,
        used: usize,
        boundary: Boundary,
    ) -> Result<Pending, crate::Status> {
        let pos = used.min(header.len());
        let total = header.len() + self.body.len();
        let frozen = match self.pool.try_copy(self.body) {
            Ok(Some(frozen)) => frozen,
            Ok(None) => {
                return Err(Output::exhausted(Reason::PendingRequests));
            }
            Err(error) => {
                return Err(crate::Status::capacity(Reason::PendingRequestLarge, error));
            }
        };
        Ok(Pending::message(
            header,
            frozen.into_shared(),
            total,
            pos,
            boundary,
        ))
    }
}
