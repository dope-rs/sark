use entries::status;
use o3::mem::Rejected;
use sark_core::{
    body::{self, admit, stream::Stream},
    decode::{exec, request, state},
    duplex::Session,
};
use sark_http::payload::{owned, response::Method};

use crate::manifold::requests::{Id, duplex, entries, task};

type Failure<'d, K, P> = request::Failure<
    K,
    P,
    Rejected<(admit::Track<admit::Finished>, owned::Segments<'d>), body::Error>,
>;

#[repr(transparent)]
pub(in crate::manifold) struct Reserved<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    pub(super) entry: entries::Detached<'a, 'd, P, B, T, D>,
}

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Reserved<'_, 'd, P, B, T, D> {
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

    pub(in crate::manifold::requests) fn respond(mut self, state: status::State) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Responding(state);
        self.entry.push();
    }

    pub(in crate::manifold::requests) fn connect(mut self, state: duplex::State<'d, D>) {
        self.entry.owner.entry.get_mut().entry = entries::Entry::Duplex(state);
        self.entry.push();
    }

    pub(in crate::manifold) fn buffer(
        mut self,
        request: <P::Family as state::Family<P>>::State<entries::Receiving<'d>>,
    ) {
        self.entry.owner.entry.get_mut().entry =
            entries::Entry::Pending(entries::Buffered::new(request));
    }

    pub(in crate::manifold) fn build(mut self, start: admit::Start, build: B) {
        let body = match start {
            admit::Start::Open(open) => Stream::new(open, build),
            admit::Start::Finished(done) => Stream::finished(done, build),
        };
        self.entry.owner.entry.get_mut().entry = entries::Entry::Building(body);
        self.entry.push();
    }

    pub(in crate::manifold) fn sync(
        mut self,
        request: request::Admitted<exec::Sync, P, admit::Track<admit::Finished>>,
    ) -> Result<task::Buffered<'d, P>, Failure<'d, exec::Sync, P>> {
        let request = self.finish(request)?;
        let method = self.entry.owner.entry.get().method;
        self.entry.owner.entry.discard();
        Ok(task::Buffered { request, method })
    }

    pub(in crate::manifold) fn task(
        mut self,
        request: request::Admitted<exec::Task, P, admit::Track<admit::Finished>>,
    ) -> Result<(), Failure<'d, exec::Task, P>> {
        let request = self.finish(request)?;
        self.entry.owner.entry.get_mut().entry =
            entries::Entry::Deferred(task::Input::Buffered(request));
        self.entry.push();
        Ok(())
    }

    fn finish<K>(
        &mut self,
        request: request::Admitted<K, P, admit::Track<admit::Finished>>,
    ) -> Result<request::Ready<K, P, owned::Segments<'d>>, Failure<'d, K, P>> {
        match request.try_map_body(|done| body::Body::try_new(done, owned::Segments::new())) {
            Ok(request) => Ok(request.ready()),
            Err(error) => {
                self.entry.owner.entry.get_mut().entry = entries::Entry::Stopped;
                Err(error)
            }
        }
    }
}
