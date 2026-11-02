use o3::buffer::{hold::ranges, storage};
use sark_http::request::body::{self, Ref, Source, View};

#[derive(Debug)]
pub struct Segments<'a> {
    body: ranges::Set<'a, 'a, [u8]>,
    contiguous: Option<storage::Shared>,
    pub coalesces: usize,
}

impl<'a> Segments<'a> {
    pub fn new(body: ranges::Set<'a, 'a, [u8]>) -> Self {
        Self {
            body,
            contiguous: None,
            coalesces: 0,
        }
    }
}

impl<'d> Source<'d> for Segments<'_> {
    type Body<'a>
        = Ref<'a, 'd>
    where
        Self: 'a,
        'd: 'a;

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a,
    {
        match &self.contiguous {
            Some(bytes) => Ref::from_slice(bytes.as_ref()),
            None => Ref::from_ranges(&self.body),
        }
    }

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, View<'a, 'd>) -> T,
    ) -> Result<T, (V, body::Error)> {
        self.coalesces += 1;
        if let Some(bytes) = self.contiguous.as_deref() {
            return Ok(project(value, View::Slice(bytes)));
        }
        let body = Ref::from_ranges(&self.body);
        let bytes = match body.try_copy() {
            Ok(bytes) => bytes,
            Err(error) => return Err((value, body::Error::Owned(error))),
        };
        let output = project(value, View::Slice(&bytes));
        self.contiguous = Some(bytes);
        Ok(output)
    }
}
