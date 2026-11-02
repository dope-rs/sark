use o3::buffer::{hold::ranges, storage};
use sark_core::{
    body::{Body, admit},
    route,
};
use sark_http::{
    payload::owned,
    request::body::{self, Ref, Source},
};

/// A sync-only logical body over borrowed or retained protocol storage.
#[repr(transparent)]
pub(crate) struct View<'req, 'd> {
    kind: Kind<'req, 'd>,
}

enum Kind<'req, 'd> {
    Slice(&'req [u8]),
    Ranges(ranges::Set<'req, 'req, [u8]>),
    Shared(storage::Shared),
    Owned(&'req mut owned::Segments<'d>),
}

impl<'req, 'd> View<'req, 'd> {
    pub(crate) const fn from_slice(bytes: &'req [u8]) -> Self {
        Self {
            kind: Kind::Slice(bytes),
        }
    }

    pub(crate) const fn from_body(body: &'req mut owned::Segments<'d>) -> Self {
        Self {
            kind: Kind::Owned(body),
        }
    }

    pub(crate) fn from_ranges(ranges: ranges::Set<'req, 'req, [u8]>) -> Self {
        Self {
            kind: Kind::Ranges(ranges),
        }
    }

    pub(crate) fn finish<R: route::Spec>(
        self,
        len: usize,
        length: admit::Length,
    ) -> Result<Body<Self>, admit::Error<Self>> {
        let plan = admit::Plan::new(
            <R::Request as route::Schema>::BODY_RETENTION,
            R::MAX_BODY,
            length,
            admit::Stream::Buffered,
        );
        plan.try_finish(R::MAX_BODY, len, self)
    }
}

impl<'d> Source<'d> for View<'_, 'd> {
    type Body<'a>
        = Ref<'a, 'd>
    where
        Self: 'a,
        'd: 'a;

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a,
    {
        match &self.kind {
            Kind::Slice(bytes) => Ref::from_slice(bytes),
            Kind::Ranges(ranges) => Ref::from_ranges(ranges),
            Kind::Shared(owner) => Ref::from(body::View::Shared(owner)),
            Kind::Owned(body) => Ref::from(&**body),
        }
    }

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, body::View<'a, 'd>) -> T,
    ) -> Result<T, (V, body::Error)> {
        match &mut self.kind {
            Kind::Slice(bytes) => Ok(project(value, body::View::Slice(bytes))),
            Kind::Shared(owner) => Ok(project(value, body::View::Shared(owner))),
            Kind::Ranges(ranges) => {
                let view = Ref::from_ranges(ranges);
                if let Some(bytes) = view.as_slice() {
                    return Ok(project(value, body::View::Slice(bytes)));
                }
                let bytes = match view.try_copy() {
                    Ok(bytes) => bytes,
                    Err(error) => return Err((value, body::Error::Owned(error))),
                };
                let output = project(value, body::View::Shared(&bytes));
                self.kind = Kind::Shared(bytes);
                Ok(output)
            }
            Kind::Owned(segments) => Source::try_project(&mut **segments, value, project),
        }
    }
}

const _: () = assert!(size_of::<View<'static, 'static>>() == 6 * size_of::<usize>());
