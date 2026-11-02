use crate::payload::response::status::Code;
mod overflow;
mod plan;

use o3::buffer::resident::bound;
pub use overflow::Overflow;
pub use plan::Plan;

use crate::payload::response::{Encode, dispatch, headers};

pub trait Body: Encode {
    fn encoded_len(&self) -> usize;
}

/// An encoder whose size must be admitted before transport output is prepared.
pub trait TryBody: Encode {
    fn try_len(&self) -> Result<usize, Overflow>;
}

impl<B: Body> TryBody for B {
    fn try_len(&self) -> Result<usize, Overflow> {
        Ok(self.encoded_len())
    }
}

/// Exact response retaining every generated field in its original type.
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
}

impl<'req, 'd, H, B> dispatch::IntoOutput<'req, 'd> for Direct<H, B>
where
    H: headers::Source,
    B: TryBody,
{
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'req, 'd, H, B> dispatch::Complete<'req, 'd> for Direct<H, B>
where
    H: headers::Source,
    B: TryBody,
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
        dispatch::Accept::exact(sink, status, fields, body)
    }
}
