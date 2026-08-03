use o3::buffer::Shared;

/// Immutable request bytes retained for an async route.
#[doc(hidden)]
pub struct RequestStorage {
    request: Shared,
    body: Option<Shared>,
    split: usize,
}

impl RequestStorage {
    pub(crate) fn new(request: Shared, body: Option<Shared>, split: usize) -> Self {
        debug_assert!(split <= request.len());
        Self {
            request,
            body,
            split,
        }
    }

    /// Returns the request head and body backed by this storage.
    #[doc(hidden)]
    pub fn as_parts(&self) -> (&[u8], &[u8]) {
        let head = &self.request.as_slice()[..self.split];
        let body = match &self.body {
            Some(body) => body.as_slice(),
            None => &self.request.as_slice()[self.split..],
        };
        (head, body)
    }
}
