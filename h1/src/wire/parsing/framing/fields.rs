use std::num::NonZero;

use o3::buffer::storage;
use sark_core::{request, route};
use sark_http::request::Layout;

use crate::wire::{parsing, parsing::framing::Parsed};

type Split<'req, R> = Result<
    (Parsed<&'req [u8], R>, parsing::Frame<'req>),
    parsing::Request<<R as route::Spec>::Request>,
>;

pub(in crate::wire::parsing::framing) struct Fields<'a, 'req, R: route::Spec> {
    pub(in crate::wire::parsing::framing) source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    pub(in crate::wire::parsing::framing) headers: <R::Request as route::Schema>::RawHeaders,
    pub(in crate::wire::parsing::framing) len: NonZero<u32>,
    pub(in crate::wire::parsing::framing) layout: Layout,
}

impl<'a, 'req, R: route::Spec> Fields<'a, 'req, R> {
    pub(in crate::wire::parsing::framing) fn into_head(
        mut self,
    ) -> Result<Parsed<&'req [u8], R>, parsing::Request<R::Request>> {
        let wire = self
            .source
            .head
            .try_head::<R>(&mut self.headers, self.len)?;
        // SAFETY: admission and both field sets use this exact immutable head
        // prefix; the checked projection preserves their source and offsets.
        unsafe {
            request::raw::Parsed::try_bind(wire, self.layout, self.source.params, self.headers)
        }
        .map_err(parsing::Request::bind)
    }

    pub(in crate::wire::parsing::framing) fn into_frame(mut self, total: usize) -> Split<'req, R> {
        self.source.head.parse_query::<R>(&mut self.headers)?;
        let (wire, frame) = self.source.head.try_frame::<R>(self.len, total)?;
        // SAFETY: the checked split retains the admitted prefix of the matched
        // input, preserving the layout and both field sets' exact byte origin.
        let request = unsafe {
            request::raw::Parsed::try_bind(wire, self.layout, self.source.params, self.headers)
        }
        .map_err(parsing::Request::bind)?;
        Ok((request, frame))
    }

    pub(in crate::wire::parsing::framing) fn retain(
        mut self,
    ) -> Result<Parsed<storage::Shared, R>, parsing::Request<R::Request>> {
        self.source.head.parse_query::<R>(&mut self.headers)?;
        let wire = self
            .source
            .head
            .input()
            .try_head(self.len)
            .map_err(parsing::Request::input)?;
        // SAFETY: try_head shares or copies this exact admitted head prefix;
        // its immutable bytes preserve the layout and both schema field sets.
        unsafe {
            request::raw::Parsed::try_bind(wire, self.layout, self.source.params, self.headers)
        }
        .map_err(parsing::Request::bind)
    }
}
