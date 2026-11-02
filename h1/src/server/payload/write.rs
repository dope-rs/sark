//! HTTP/1 projection into the final-link transaction.

use conn::write;
use data::compose;
use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener::conn,
    net::{
        link::{
            egress,
            egress::{data, load::Value},
        },
        wire,
    },
};
use o3::buffer::{hold, pool, pool::owners, resident, resident::bound, segments, storage};
use sark_http::payload::{compress::Gzip, fixed, owned};
use write::{body, body::Admit, error};

use crate::{
    pump,
    server::{deadline, payload},
    wire::{head, output},
};

type Bound<'d, K, G> =
    data::Bound<compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>, G>;
type Rejected<'d, K, R> = (egress::Excess<payload::Body<'d, K>>, R);
type DraftResult<'d, K, G, R> = Result<write::Draft<Bound<'d, K, G>, R>, Rejected<'d, K, R>>;

#[repr(transparent)]
struct Projection<'a, 'd, K>(&'a mut Response<'d, K>);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::server) enum Wake {
    None,
    Pending,
}

/// One final-link response projection retaining its exact owner and tag.
pub struct Response<'d, K> {
    body: Option<egress::Load<payload::Body<'d, K>>>,
    connection: head::Connection,
    written: usize,
    wake: Wake,
    tag: K,
}

/// Protocol effects returned after the final link owns the response.
pub struct CycleCommit<K, H> {
    pub(in crate::server) progress: deadline::Progress,
    pub(in crate::server) discard_remaining: usize,
    pub state: Result<(), pump::Reject>,
    pub(in crate::server) transport: Option<K>,
    pub(in crate::server) wake: Wake,
    pub tail: pump::Tail<H>,
}

/// Final-link failure before or after response production.
pub enum Failure<R> {
    Capacity(write::Reject),
    Rejected { cause: body::Failure, receipt: R },
}

impl<'d, K> Response<'d, K> {
    fn new(tag: K) -> Self {
        Self {
            body: None,
            connection: head::Connection::KeepAlive,
            written: 0,
            wake: Wake::None,
            tag,
        }
    }

    fn accept(&mut self, written: usize, body: Option<payload::Body<'d, K>>) {
        self.written = written;
        self.body = body.map(egress::Load::new);
    }

    fn try_finish<G: bound::Bound, R>(self, receipt: R) -> DraftResult<'d, K, G, R> {
        let body = match self.body {
            Some(body) => match body.try_bound::<G>() {
                Ok(body) => Some(Value::Unchecked(body.bound_left())),
                Err(cause) => return Err((cause, receipt)),
            },
            None => None,
        };
        let draft = write::Draft::new(self.written, body, receipt);
        Ok(match self.connection {
            head::Connection::KeepAlive => draft,
            head::Connection::Close => draft.close_after(),
        })
    }
}

impl<'d, K: Copy> pump::Sink<'d> for Projection<'_, 'd, K> {
    fn close(&mut self) {
        self.0.connection = head::Connection::Close;
    }

    fn wake(&mut self) {
        self.0.wake = Wake::Pending;
    }

    fn inline(self, written: usize) {
        self.0.accept(written, None);
    }

    fn static_body(self, written: usize, body: fixed::Body<'d>) {
        let body = body.as_slice();
        let body = payload::Body::Shared {
            owner: resident::Shared::from(body),
            tag: self.0.tag,
        };
        self.0.accept(written, Some(body));
    }

    fn shared(self, written: usize, body: storage::Shared) {
        let body = payload::Body::Shared {
            owner: resident::Shared::from(body),
            tag: self.0.tag,
        };
        self.0.accept(written, Some(body));
    }

    fn frozen(
        self,
        written: usize,
        body: pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
    ) {
        let body = payload::Body::Frozen {
            owner: body,
            tag: self.0.tag,
        };
        self.0.accept(written, Some(body));
    }

    fn segments(self, written: usize, body: owned::Segments<'d>) {
        let body = match body.into_shape() {
            segments::Shape::Empty => {
                self.0.accept(written, None);
                return;
            }
            segments::Shape::One(owner) => payload::Body::Shared {
                owner: owner.into_inner(),
                tag: self.0.tag,
            },
            segments::Shape::Many(owners) => payload::Body::Segments {
                owners,
                tag: self.0.tag,
            },
        };
        self.0.accept(written, Some(body));
    }
}

const _: () = assert!(core::mem::size_of::<Wake>() == 1);

impl<'d, K: Copy + Default + 'd> Response<'d, K> {
    /// Projects one pump cycle into a recoverable final-link draft.
    pub fn try_cycle<G: bound::Bound, H>(
        cycle: pump::Cycle<'d, H>,
        tagger: impl FnOnce(hold::Charge) -> K,
    ) -> DraftResult<'d, K, G, CycleCommit<K, H>> {
        let owner = match cycle.retention() {
            output::Retention::Response(owner) | output::Retention::Linked(owner) => owner,
            output::Retention::Rejected => output::Owner::default(),
        };
        let (tag, tx) = if owner.charge.is_empty() {
            let tag = K::default();
            (tag, None)
        } else {
            let tag = tagger(owner.charge);
            (tag, Some(tag))
        };
        let progress = cycle.progress;
        let left = cycle.discard_remaining;
        let mut reply = Self::new(tag);
        let (state, tail) = cycle.commit(Projection(&mut reply));
        let wake = reply.wake;
        reply.try_finish(CycleCommit {
            progress,
            discard_remaining: left,
            state,
            transport: tx,
            wake,
            tail,
        })
    }

    /// Projects one transport-neutral plan into a recoverable final-link draft.
    pub fn try_plan<G: bound::Bound, R>(
        plan: output::Plan<'d>,
        receipt: impl FnOnce(head::Connection) -> R,
        tagger: impl FnOnce(hold::Charge) -> K,
    ) -> DraftResult<'d, K, G, R> {
        let (egress, conn) = pump::Egress::output(plan);
        let receipt = receipt(conn);
        let owner = egress.retention();
        let tag = if owner.charge.is_empty() {
            K::default()
        } else {
            tagger(owner.charge)
        };
        let mut reply = Self::new(tag);
        egress.commit(conn, Projection(&mut reply));
        reply.try_finish(receipt)
    }

    /// Releases rejected physical owners and returns the exact route action.
    pub fn reject<G: bound::Bound, R>(
        cause: error::Error<Bound<'d, K, G>, R, Rejected<'d, K, R>>,
    ) -> Failure<R> {
        match cause {
            error::Error::Capacity(cause) => Failure::Capacity(cause),
            error::Error::Build((cause, receipt)) => Failure::Rejected {
                cause: cause.into(),
                receipt,
            },
            error::Error::Mismatch(error) => {
                let (cause, receipt) = error.into_parts();
                Failure::Rejected {
                    cause: body::Failure::Mismatch(cause),
                    receipt,
                }
            }
        }
    }

    /// Returns the same immutable borrow when its projection remains unadmitted.
    pub(in crate::server) fn send_static<ID: Identity, W, C, G: bound::Bound + 'd>(
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, Bound<'d, K, G>>,
        body: fixed::Body<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Option<fixed::Body<'d>>
    where
        W: wire::Spec,
    {
        let reason = body;
        let body = body.as_slice();
        let body = payload::Body::Shared {
            owner: resident::Shared::from(body),
            tag: K::default(),
        };
        let body = match egress::NonEmpty::try_new(body) {
            Ok(body) => body,
            Err(cause) => {
                conn.write().reject(body::Failure::Payload(cause));
                return None;
            }
        };
        let body = match body.into_left().try_bound::<G>() {
            Ok(body) => body,
            Err(rejected) => {
                let cause = {
                    let parts = rejected.into_parts();
                    body::Failure::Bound {
                        charge: parts.0.charge(),
                        maximum: parts.1.get(),
                    }
                };
                conn.write().reject(cause);
                return None;
            }
        };
        let rejected = match conn.write().admit(body, driver) {
            Admit::Committed | Admit::Closed(_) => None,
            Admit::Deferred { .. } => {
                return Some(reason);
            }
            Admit::Rejected { cause, .. } => Some(body::Failure::Capacity(cause)),
            Admit::Unavailable(cause) => {
                let parts = cause.into_parts();
                Some(body::Failure::Reserve(parts.1))
            }
        };
        if let Some(cause) = rejected {
            conn.write().reject(cause);
        }
        None
    }
}
