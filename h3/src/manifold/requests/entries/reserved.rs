use crate::manifold::owners;
use o3::mem::Rejected;
use sark_core::manifold::task::app;
use sark_core::{
    body::{self, admit, stream::Stream},
    decode::{exec, request, state},
};
use sark_http::payload::{
    owned,
    response::{Method, status::Code},
};

use crate::manifold::{
    encoder,
    requests::{Id, duplex, entries, task},
};

type Failure<'d, K, P> = request::Failure<
    K,
    P,
    Rejected<(admit::Track<admit::Finished>, owned::Segments<'d>), body::Error>,
>;

#[repr(transparent)]
pub(in crate::manifold) struct Reserved<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> {
    pub(super) entry: entries::Detached<'a, 'd, A, M>,
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Reserved<'_, 'd, A, M> {
    pub(in crate::manifold::requests) fn id(&self) -> Id {
        let entry = &self.entry.owner.entry;
        Id::from_entry(entry.get().identity, entry.key())
    }

    pub(in crate::manifold::requests) fn admit(&mut self, method: Method) {
        self.entry.owner.entry.get_mut().method = method;
    }

    pub(in crate::manifold::requests) fn method(&self) -> Method {
        self.entry.owner.entry.get().method
    }

    pub(in crate::manifold::requests) fn discard(self) {
        self.entry.owner.entry.discard();
    }

    pub(in crate::manifold::requests) fn respond(mut self, code: Code) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Responding(code);
        self.entry.push::<false>();
    }

    pub(in crate::manifold) fn close(mut self, response: encoder::Pending<'d, M>) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Closing(response);
        self.entry.push::<false>();
    }

    pub(in crate::manifold) fn response(mut self, response: encoder::Pending<'d, M>) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Response(response);
        self.entry.push::<false>();
    }

    pub(in crate::manifold::requests) fn connect(mut self, state: duplex::State<'d, A::Duplex>) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Duplex(state);
        self.entry.push::<false>();
    }

    pub(in crate::manifold) fn buffer(
        mut self,
        request: <<A::Prepared as request::Prepared>::Family as state::Family<A::Prepared>>::State<
            entries::Receiving<'d>,
        >,
    ) {
        self.entry.owner.entry.get_mut().entry =
            entries::Entry::Pending(entries::Buffered::new(request));
    }

    pub(in crate::manifold) fn build(mut self, start: admit::Start, build: A::Build) {
        let body = match start {
            admit::Start::Open(open) => Stream::new(open, build),
            admit::Start::Finished(done) => Stream::finished(done, build),
        };
        self.entry.owner.entry.get_mut().entry = entries::Entry::Building(body);
        self.entry.push::<false>();
    }

    pub(in crate::manifold) fn sync(
        mut self,
        request: request::Admitted<exec::Sync, A::Prepared, admit::Track<admit::Finished>>,
    ) -> Result<(Self, task::Buffered<'d, A::Prepared>), Failure<'d, exec::Sync, A::Prepared>> {
        let request = self.finish(request)?;
        let method = self.entry.owner.entry.get().method;
        Ok((self, task::Buffered { request, method }))
    }

    pub(in crate::manifold) fn task(
        mut self,
        request: request::Admitted<exec::Task, A::Prepared, admit::Track<admit::Finished>>,
    ) -> Result<(), Failure<'d, exec::Task, A::Prepared>> {
        let request = self.finish(request)?;
        self.entry.owner.entry.get_mut().entry =
            entries::Entry::Deferred(task::Input::Buffered(request));
        self.entry.push::<false>();
        Ok(())
    }

    fn finish<K>(
        &mut self,
        request: request::Admitted<K, A::Prepared, admit::Track<admit::Finished>>,
    ) -> Result<request::Ready<K, A::Prepared, owned::Segments<'d>>, Failure<'d, K, A::Prepared>>
    {
        match request.try_map_body(|done| body::Body::try_new(done, owned::Segments::new())) {
            Ok(request) => Ok(request.ready()),
            Err(error) => {
                self.entry.owner.entry.get_mut().entry = entries::Entry::Stopped;
                Err(error)
            }
        }
    }
}
