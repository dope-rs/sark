use o3::buffer::resident::bound;

use crate::payload::response::{Body, dispatch, headers, status::Code};

/// Response retaining its generated field types without an erased value enum.
pub struct Direct<H, B> {
    status: Code,
    fields: H,
    body: B,
}

impl<H, B> Direct<H, B> {
    pub fn new(status: Code, fields: H, body: B) -> Self {
        Self {
            status,
            fields,
            body,
        }
    }

    pub fn status(&self) -> Code {
        self.status
    }

    pub fn body(&self) -> &B {
        &self.body
    }
}

impl<'req, 'd, H, B> dispatch::IntoOutput<'req, 'd> for Direct<H, B>
where
    H: headers::Source,
    B: Body<'d>,
{
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'req, 'd, H, B> dispatch::Complete<'req, 'd> for Direct<H, B>
where
    H: headers::Source,
    B: Body<'d>,
{
    const BODY: crate::payload::Kind = crate::payload::Kind::Inline;
    type Bound = bound::Unbounded;

    fn complete<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: dispatch::Accept<'d>,
    {
        let Self {
            status,
            fields,
            body,
        } = self;
        dispatch::Accept::fixed(sink, status, fields, body)
    }
}
