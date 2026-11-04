use core::{convert::Infallible, ops};

use o3::{buffer, buffer::storage};

use crate::h1::{
    field,
    head::{
        Limit, ParseError,
        request::{Kind, Plan, Route, trace},
        state,
    },
};

/// Parsed head with a nonempty effective authority and the exact input that produced it.
pub struct Parts<S, T = ()> {
    source: S,
    head: Plan,
    trace: T,
}

impl Copy for Parts<&[u8]> {}

impl Clone for Parts<&[u8]> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'owner> Parts<&'owner [u8]> {
    pub fn parse<E>(
        bytes: &'owner [u8],
        limit: Limit,
        sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<Option<Self>, ParseError<E>> {
        let (_, output) = state::State::new(Kind, limit).parse(bytes, sink)?;
        Ok(output.map(|head| Self::new(bytes, head)))
    }

    pub fn scan(bytes: &'owner [u8], limit: Limit) -> Result<Option<Self>, ParseError<Infallible>> {
        let (_, output) = state::State::new(Kind, limit).scan(bytes)?;
        Ok(output.map(|head| Self::new(bytes, head)))
    }

    pub fn line(self) -> (&'owner [u8], &'owner [u8]) {
        let route = self.into_route();
        (route.method().as_bytes(), route.target().as_bytes())
    }

    pub const fn bytes(self) -> &'owner [u8] {
        self.source
    }
}

impl<S: buffer::raw::Source> Parts<S> {
    pub fn as_ref(&self) -> Parts<&[u8]> {
        Parts::new(self.source.as_ref(), self.head)
    }

    pub fn into_route(self) -> Route<S> {
        Route::new(
            self.source,
            self.head.route,
            self.head.wire_len(),
            self.head.upgrades(),
        )
    }
}

impl<S> Parts<S> {
    pub(in crate::h1::head::request) const fn new(source: S, head: Plan) -> Self {
        Self {
            source,
            head,
            trace: (),
        }
    }
}

impl<S, T> ops::Deref for Parts<S, T> {
    type Target = Plan;

    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

impl Parts<storage::Owned, trace::Fields> {
    pub fn traced(
        source: storage::Owned,
        limit: Limit,
    ) -> Result<Option<Self>, ParseError<trace::Error>> {
        let mut fields = trace::Fields::default();
        let (_, output) =
            state::State::new(Kind, limit).parse(source.as_ref(), |field| fields.observe(field))?;
        Ok(output.map(|head| Self {
            source,
            head,
            trace: fields,
        }))
    }

    pub fn into_owned(self) -> trace::Owned {
        trace::Owned {
            route: Route::new(
                self.source,
                self.head.route,
                self.head.wire_len(),
                self.head.upgrades(),
            ),
            context: self.trace.context,
            line: self.trace.line.map(trace::Line::from_span),
        }
    }
}
