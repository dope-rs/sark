/// A route call whose body has already passed its request projection.
#[repr(C)]
pub struct Call<H, B> {
    request: H,
    body: B,
}

impl<H, B> Call<H, B> {
    #[doc(hidden)]
    pub const fn new(request: H, body: B) -> Self {
        Self { request, body }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (H, B) {
        (self.request, self.body)
    }
}
