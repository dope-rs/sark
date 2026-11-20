use std::marker::PhantomData;

use o3::buffer::resident::bound;

use super::{Body, dispatch, encoded, headers::Application, status::Code};
use crate::{field, payload};

pub struct Output<H, B, F> {
    status: Code,
    fields: H,
    body: B,
    form: PhantomData<fn() -> F>,
}

impl<H: Copy> Copy for Output<H, payload::fixed::Body<'_>, Static> {}
impl<H: Copy> Clone for Output<H, payload::fixed::Body<'_>, Static> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H, B, F> Output<H, B, F> {
    pub fn new(status: Code, fields: H, body: B) -> Self {
        Self {
            status,
            fields,
            body,
            form: PhantomData,
        }
    }

    pub fn status(&self) -> Code {
        self.status
    }

    pub fn body(&self) -> &B {
        &self.body
    }
}

pub enum Fixed {}
pub enum Static {}
pub struct Encoded<const EXACT: bool>;

mod sealed {
    pub trait Form {}
    impl Form for super::Fixed {}
    impl Form for super::Static {}
    impl<const EXACT: bool> Form for super::Encoded<EXACT> {}
}

pub trait Form<'d, B>: sealed::Form {
    type Bound: bound::Bound + 'd;
    const BODY: payload::Kind;

    fn emit<S: dispatch::Accept<'d>>(
        sink: S,
        status: Code,
        headers: impl field::Source<Application>,
        body: B,
    ) -> Result<S::Output, S::Error>;
}

impl<'d, B: Body<'d>> Form<'d, B> for Fixed {
    type Bound = bound::Unbounded;
    const BODY: payload::Kind = payload::Kind::Inline;

    fn emit<S: dispatch::Accept<'d>>(
        sink: S,
        status: Code,
        headers: impl field::Source<Application>,
        body: B,
    ) -> Result<S::Output, S::Error> {
        sink.fixed(status, headers, body)
    }
}

impl<'body: 'd, 'd> Form<'d, payload::fixed::Body<'body>> for Static {
    type Bound = bound::AtMost<0>;
    const BODY: payload::Kind = payload::Kind::Static;

    fn emit<S: dispatch::Accept<'d>>(
        sink: S,
        status: Code,
        headers: impl field::Source<Application>,
        body: payload::fixed::Body<'body>,
    ) -> Result<S::Output, S::Error> {
        sink.statics(status, headers, body)
    }
}

impl<'d, B: encoded::Source<EXACT>, const EXACT: bool> Form<'d, B> for Encoded<EXACT> {
    type Bound = bound::Unbounded;
    const BODY: payload::Kind = payload::Kind::Inline;

    fn emit<S: dispatch::Accept<'d>>(
        sink: S,
        status: Code,
        headers: impl field::Source<Application>,
        body: B,
    ) -> Result<S::Output, S::Error> {
        sink.encoded::<EXACT>(status, headers, body)
    }
}

impl<'req, 'd, H: field::Source<Application>, B, F: Form<'d, B>> dispatch::Complete<'req, 'd>
    for Output<H, B, F>
{
    type Bound = F::Bound;
    const BODY: payload::Kind = F::BODY;

    fn complete<S: dispatch::Accept<'d>>(self, sink: S) -> Result<S::Output, S::Error> {
        F::emit(sink, self.status, self.fields, self.body)
    }
}
