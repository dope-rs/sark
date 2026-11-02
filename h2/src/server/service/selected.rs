use o3::mem::Rejected;
use sark_core::{
    body::{self, admit},
    decode::{request, state},
    manifold::stream::Boundary,
    request::Head,
};
use sark_http::field::metadata::packed;

use crate::server::service::Prepared;

#[doc(hidden)]
pub struct Selected {
    head: Head<packed::Decoded>,
    body: admit::Plan,
}

impl Selected {
    pub(super) const fn new(head: Head<packed::Decoded>, body: admit::Plan) -> Self {
        Self { head, body }
    }

    pub(super) const fn plan(len: admit::Length) -> admit::Plan {
        admit::Plan::new(
            admit::Retention::Retained,
            usize::MAX,
            len,
            admit::Stream::Buffered,
        )
    }
}

impl request::Head for Selected {
    type Prepared = Prepared;
    type Start = request::Plain<state::Uniform<Prepared, state::Routed>>;

    fn try_start(
        self,
        limit: usize,
        boundary: Boundary,
    ) -> Result<Self::Start, Rejected<Self, body::Error>> {
        let Self { head, body } = self;
        match body.try_start(limit, boundary) {
            Ok(admit::Start::Open(body)) => Ok(request::Plain::new(state::Uniform::new(
                Prepared::new(head),
                request::Body::Open(body),
            ))),
            Ok(admit::Start::Finished(body)) => Ok(request::Plain::new(state::Uniform::new(
                Prepared::new(head),
                request::Body::Finished(body),
            ))),
            Err(error) => Err(Rejected::new(Self { head, body }, error)),
        }
    }
}
