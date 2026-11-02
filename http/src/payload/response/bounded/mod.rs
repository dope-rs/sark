use std::convert::Infallible;

use crate::payload::response::status::Code;
mod plan;

use o3::buffer::resident::bound;
pub use plan::Plan;

use crate::payload::response::{
    Encode, dispatch,
    headers::{head, set},
};

pub trait Body: Encode {
    fn max_len(&self) -> usize;
}

/// An encoder whose upper bound is admitted before preparing its output.
pub trait TryBody: Encode {
    type Error;

    fn try_max_len(&self) -> Result<usize, Self::Error>;
}

impl<B: Body> TryBody for B {
    type Error = Infallible;

    fn try_max_len(&self) -> Result<usize, Self::Error> {
        Ok(self.max_len())
    }
}

pub struct Output<'req, B, const N: usize = { set::DEFAULT_CAPACITY }> {
    status: Code,
    head: head::Head<'req, N>,
    body: B,
}

impl<'req, B, const N: usize> Output<'req, B, N>
where
    B: Body,
{
    pub fn new(
        status: Code,
        template: &'req head::Template<'req>,
        headers: set::Set<'req, N>,
        body: B,
    ) -> Self {
        Self {
            status,
            head: head::Head::new(template, headers),
            body,
        }
    }

    pub fn status(&self) -> Code {
        self.status
    }

    pub fn into_parts(self) -> (Code, head::Head<'req, N>, B) {
        (self.status, self.head, self.body)
    }
}

impl<'req, 'd, 'head: 'req, B, const N: usize> dispatch::IntoOutput<'req, 'd>
    for Output<'head, B, N>
where
    B: Body,
{
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'req, 'd, 'head: 'req, B, const N: usize> dispatch::Complete<'req, 'd> for Output<'head, B, N>
where
    B: Body,
{
    const BODY: crate::payload::Kind = crate::payload::Kind::Inline;
    type Bound = bound::Unbounded;

    fn complete<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: dispatch::Accept<'d>,
    {
        let Self { status, head, body } = self;
        dispatch::Accept::bounded(sink, status, head, body)
    }
}
