#[derive(Clone, Copy)]
pub struct Limit {
    pub(super) body_bytes: usize,
    pub(super) wire_bytes: usize,
    pub(super) line_bytes: usize,
    pub(super) trailer_bytes: usize,
    pub(super) trailer_fields: usize,
}

impl Limit {
    pub(in crate::h1) const fn trailers(self) -> usize {
        self.trailer_bytes
    }

    pub const fn with_body(body: usize) -> Self {
        Self::new(body, usize::MAX, 1024, 16 * 1024, 128)
    }

    pub const fn new(
        body: usize,
        wire: usize,
        line: usize,
        trailers: usize,
        fields: usize,
    ) -> Self {
        Self {
            body_bytes: body,
            wire_bytes: wire,
            line_bytes: line,
            trailer_bytes: trailers,
            trailer_fields: fields,
        }
    }
}
