#[derive(Clone, Copy)]
pub struct Limit {
    pub(super) body_bytes: usize,
    pub(super) framing: Framing,
}

#[derive(Clone, Copy)]
pub(in crate::h1) struct Framing {
    pub(super) wire_bytes: usize,
    pub(super) line_bytes: usize,
    pub(in crate::h1) trailer_bytes: usize,
    pub(super) trailer_fields: usize,
}

impl Framing {
    pub(in crate::h1) const DEFAULT: Self = Self {
        wire_bytes: usize::MAX,
        line_bytes: 1024,
        trailer_bytes: 16 * 1024,
        trailer_fields: 128,
    };
}

impl Limit {
    pub const fn with_body(body: usize) -> Self {
        Self {
            body_bytes: body,
            framing: Framing::DEFAULT,
        }
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
            framing: Framing {
                wire_bytes: wire,
                line_bytes: line,
                trailer_bytes: trailers,
                trailer_fields: fields,
            },
        }
    }
}
