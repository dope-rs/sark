use o3::buffer::{resident, segments};
use sark_core::body::{buffered, credit, input};

use crate::{
    conn::event,
    server::payload::{self, Buffer, buffer},
};

/// The selected carrier's buffer, borrowed under its current account identity.
pub(crate) struct Loan<'a, 'd, O: credit::Origin> {
    inner: buffered::Loan<'a, segments::Chain<resident::Shared<'d>>, O>,
}

impl<'a, 'd, O: credit::Origin> Loan<'a, 'd, O> {
    /// # Safety
    /// The exclusive carrier must own this exact buffer and ledger account
    /// throughout the buffer's charged lifetime. Foreign buffers are forbidden.
    pub(crate) unsafe fn new(
        body: &'a mut Buffer<'d>,
        ledger: &credit::raw::Ledger<'_, O>,
    ) -> Self {
        Self {
            // SAFETY: the selected carrier provides exact account custody.
            inner: unsafe { ledger.bind_mut(&mut body.inner) },
        }
    }

    pub(crate) const fn max_body(&self) -> usize {
        self.inner.max_body()
    }

    pub(crate) fn try_push<E: credit::Evidence>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        evidence: E,
        data: event::DataPayload<resident::Shared<'d>>,
        req: credit::Limits,
        conn: credit::Limits,
    ) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        let len = data.len();
        let chunks = data.count();
        buffer::classify(self.inner.try_push(
            ledger,
            evidence,
            input::Input::new(data, len, chunks),
            req,
            conn,
            buffer::retain,
        ))
    }
}

// SAFETY: Buffer is a transparent owner of the core accounting state. Delegation
// changes only its origin and preserves its storage, amounts and obligations.
unsafe impl<'d, O: credit::Origin> credit::raw::Bind for Buffer<'d, O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = Buffer<'d, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        Buffer {
            // SAFETY: the caller proves exact account custody for this owner.
            inner: unsafe { credit::raw::Bind::bind(self.inner, origin) },
        }
    }
}
