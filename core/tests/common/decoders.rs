use std::ops::ControlFlow;

use o3::mem::Rejected;
use sark_core::{
    body::{
        self,
        admit::{self, Length, Plan, Retention, Stream},
    },
    decode::{exec, request, state},
    manifold::stream::Boundary,
};

#[derive(Debug, PartialEq, Eq)]
pub struct Prepared {
    pub len: usize,
}

pub enum Buffered {}

impl state::Set for Buffered {
    type SyncOpen = usize;
    type SyncFinished = usize;
    type TaskOpen = usize;
    type TaskFinished = usize;
    type DuplexOpen = usize;
    type DuplexFinished = usize;
    type UnsupportedOpen = usize;
    type UnsupportedFinished = usize;
}

impl state::Finish for Buffered {
    fn sync(body: usize) -> usize {
        body
    }

    fn task(body: usize) -> usize {
        body
    }

    fn duplex(body: usize) -> usize {
        body
    }

    fn reject(body: usize) -> usize {
        body
    }
}

impl request::Prepared for Prepared {
    type Execution = exec::Sync;
    type Family = state::Uniforms<Self>;
}

impl request::Head for Prepared {
    type Prepared = Self;
    type Start = request::Plain<state::Uniform<Self, state::Routed>>;

    fn try_start(
        self,
        limit: usize,
        boundary: Boundary,
    ) -> Result<Self::Start, Rejected<Self, body::Error>> {
        let body = Plan::new(
            Retention::Retained,
            self.len,
            Length::Known(self.len),
            Stream::Buffered,
        );
        match body.try_start(limit, boundary) {
            Ok(admit::Start::Open(body)) => Ok(request::Plain::new(state::Uniform::new(
                self,
                request::Body::Open(body),
            ))),
            Ok(admit::Start::Finished(body)) => Ok(request::Plain::new(state::Uniform::new(
                self,
                request::Body::Finished(body),
            ))),
            Err(error) => Err(Rejected::new(self, error)),
        }
    }
}

pub fn try_admit(
    prepared: Prepared,
) -> Result<state::Uniform<Prepared, state::Admitted>, Rejected<Prepared, body::Error>> {
    let routed = request::Head::try_start(prepared, usize::MAX, Boundary::More)?;
    let routed = routed.into_state();
    match state::Flow::map::<state::Admitted, _>(routed, state::Pass) {
        ControlFlow::Continue(admitted) => Ok(admitted),
        ControlFlow::Break(never) => match never {},
    }
}

pub struct Take;

impl<P> exec::Fold<P, Buffered, exec::Sync> for Take {
    type Output = (P, usize, bool);

    fn open<S: state::Slot<P>>(self, slot: S, body: usize) -> Self::Output {
        (slot.prepare(), body, false)
    }

    fn finished<S: state::Slot<P>>(self, slot: S, body: usize) -> Self::Output {
        (slot.prepare(), body, true)
    }
}
