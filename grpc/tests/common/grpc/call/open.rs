use core::iter;

use sark_grpc::{
    Status,
    client::{
        call::{bidi, request, unary, unary::Session as _},
        session,
    },
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{client, stream};

pub trait Open<'scope> {
    fn try_open(&mut self, count: usize) -> Result<Vec<bidi::Stream<'scope, stream::Key>>, Status>;
    fn try_unary(
        &mut self,
        count: usize,
    ) -> Result<Vec<unary::Handle<'scope, stream::Key>>, Status>;
    fn try_requests(
        &mut self,
        count: usize,
    ) -> Result<Vec<unary::Handle<'scope, stream::Key>>, Status>;
}

impl<'scope> Open<'scope> for session::Session<'scope, client::Plan<section::Plan>> {
    fn try_open(&mut self, count: usize) -> Result<Vec<bidi::Stream<'scope, stream::Key>>, Status> {
        iter::repeat_with(|| bidi::Session::open(self, b"/svc/Method", None, &Metadata::new()))
            .take(count)
            .collect()
    }

    fn try_unary(
        &mut self,
        count: usize,
    ) -> Result<Vec<unary::Handle<'scope, stream::Key>>, Status> {
        iter::repeat_with(|| {
            let stream = request::Session::open(self, b"/svc/Method", None, &Metadata::new())?;
            request::Session::finish(self, stream)
        })
        .take(count)
        .collect()
    }

    fn try_requests(
        &mut self,
        count: usize,
    ) -> Result<Vec<unary::Handle<'scope, stream::Key>>, Status> {
        let mut calls = Vec::with_capacity(count);
        while calls.len() < count {
            calls.push(self.start_raw(b"/svc/Method", None, &Metadata::new(), b"request")?);
        }
        Ok(calls)
    }
}
