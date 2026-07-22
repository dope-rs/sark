use o3::buffer::Shared;

#[doc(hidden)]
pub struct RequestStorage {
    request: Shared,
    decoded_body: Option<Shared>,
    head_len: usize,
}

impl RequestStorage {
    pub(crate) fn new(request: Shared, decoded_body: Option<Shared>, head_len: usize) -> Self {
        debug_assert!(head_len <= request.len());
        Self {
            request,
            decoded_body,
            head_len,
        }
    }

    /// Returns request views with the erased task lifetime.
    ///
    /// # Safety
    /// The caller must move this storage into the same `RequestDomain` as every
    /// value borrowing the returned slices, and must drop those values before
    /// the storage.
    pub(crate) unsafe fn task_views<'task>(&self) -> (&'task [u8], &'task [u8]) {
        let head = &self.request.as_slice()[..self.head_len];
        let body = match &self.decoded_body {
            Some(body) => body.as_slice(),
            None => &self.request.as_slice()[self.head_len..],
        };
        unsafe { (&*(head as *const [u8]), &*(body as *const [u8])) }
    }
}
