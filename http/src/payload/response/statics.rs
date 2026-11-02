use o3::buffer::resident::bound;

use crate::payload::{
    fixed,
    response::{dispatch, headers, status::Code},
};

/// Static response retaining every generated field in its original type.
#[derive(Copy)]
pub struct Direct<'body, H> {
    status: Code,
    fields: H,
    body: fixed::Body<'body>,
}

impl<H: Copy> Clone for Direct<'_, H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'body, H> Direct<'body, H> {
    pub fn new(status: Code, fields: H, body: fixed::Body<'body>) -> Self {
        Self {
            status,
            fields,
            body,
        }
    }

    pub fn status(&self) -> Code {
        self.status
    }

    pub fn body(&self) -> &fixed::Body<'body> {
        &self.body
    }
}

impl<'body, 'req, 'd, H: headers::Source> dispatch::IntoOutput<'req, 'd> for Direct<'body, H>
where
    'body: 'd,
{
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'body, 'req, 'd, H: headers::Source> dispatch::Complete<'req, 'd> for Direct<'body, H>
where
    'body: 'd,
{
    const BODY: crate::payload::Kind = crate::payload::Kind::Static;

    type Bound = bound::AtMost<0>;

    fn complete<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: dispatch::Accept<'d>,
    {
        let Self {
            status,
            fields,
            body,
        } = self;
        dispatch::Accept::statics(sink, status, fields, body)
    }
}
