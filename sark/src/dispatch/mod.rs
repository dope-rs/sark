pub mod conn_state;
mod driver;
mod egress;
mod invocation;
pub mod pipeline;
mod requests;
pub mod response_cache;
mod routes;
pub mod routing;
mod tasks;

use std::ops::Range;
use std::pin::Pin;

pub use conn_state::{ConsumeOutcome, Outcome};
use dope::DriverContext;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::state::{EgressCtx, State};
use dope_net::link;
use dope_net::wire::Wire;
pub use driver::{H1Driver, HeadDeadline};
pub use invocation::{Invocation, StreamRoute, SyncRoute};
use o3::buffer::RetainBytes;
pub use pipeline::{Pipeline, identity_mut};
pub use requests::{Ctx, Matched};
pub use routes::{Complete, FiberRoute, TaskPoll};
pub use routing::{H1Host, RouteCore, Routing};
use sark_core::http::{HeadPlan, PlannedHead, ResponseSink, Shape};
pub use tasks::TaskRunner;

use crate::service::{RouteSpec, manifold};

pub trait ResponseEncoder: ResponseSink {}

impl<T> ResponseEncoder for T where T: ResponseSink {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded {
    Emitted,
    NotFound,
    Bad,
    Unsupported,
}

/// Body storage requirements selected while routing the request head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyPlan {
    /// Whether the route observes body bytes or only their wire length.
    pub policy: crate::service::BodyPolicy,
    /// Maximum accepted wire-body length.
    pub max_body: usize,
    /// Length declared by the request head, when present.
    pub content_length: Option<usize>,
}

/// A request head whose route and borrowed field ranges are already resolved.
pub trait PreparedRequest {
    fn body_plan(&self) -> BodyPlan;
}

/// Protocol-owned request body storage presented to a generated route.
///
/// `contiguous` may materialize segmented storage. Generated dispatch calls it
/// only for request types whose body mode requires contiguous bytes. `body_len`
/// is the received wire length, including for sources that discarded the bytes.
pub trait BodySource {
    fn body_len(&self) -> usize;
    fn contiguous(&mut self) -> &[u8];
}

impl BodySource for &[u8] {
    fn body_len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn contiguous(&mut self) -> &[u8] {
        self
    }
}

impl BodySource for Vec<u8> {
    fn body_len(&self) -> usize {
        Vec::len(self)
    }

    fn contiguous(&mut self) -> &[u8] {
        self
    }
}

pub trait Decode {
    type Prepared: PreparedRequest;
    type Plan: HeadPlan;

    fn prepare_full_head(
        &self,
        fields: sark_core::http::DecodedFieldBlock,
    ) -> Result<Self::Prepared, Decoded>;

    fn prepare_planned_head(
        &self,
        head: PlannedHead<Self::Plan>,
    ) -> Result<Self::Prepared, Decoded>;

    fn dispatch_prepared<B: BodySource, E: ResponseEncoder>(
        &self,
        prepared: Self::Prepared,
        body: B,
        encoder: &mut E,
    ) -> Decoded;
}

pub trait DecodeRoute<R: RouteSpec, S> {
    #[allow(clippy::too_many_arguments)]
    fn decode<E: ResponseEncoder>(
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        _method: http::Method,
        target: Range<usize>,
        head: &[u8],
        body: &[u8],
        declared_body_len: usize,
        state: &S,
        encoder: &mut E,
    ) -> Decoded;
}

impl<R, S> DecodeRoute<R, S> for manifold::Sync
where
    R: RouteSpec + manifold::Route<S> + 'static,
{
    fn decode<E: ResponseEncoder>(
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        _method: http::Method,
        target: Range<usize>,
        head: &[u8],
        body: &[u8],
        declared_body_len: usize,
        state: &S,
        encoder: &mut E,
    ) -> Decoded {
        match Invocation::new(target, head, body, declared_body_len).invoke::<R, S>(
            raw_params,
            raw_headers,
            state,
        ) {
            Ok(response) => {
                if response.emit(encoder) {
                    Decoded::Emitted
                } else {
                    Decoded::Unsupported
                }
            }
            Err(_) => Decoded::Bad,
        }
    }
}

macro_rules! unsupported_decode_routes {
    ($($kind:ty),+ $(,)?) => {
        $(
            impl<R: RouteSpec, S> DecodeRoute<R, S> for $kind {
                fn decode<E: ResponseEncoder>(
                    _raw_params: R::RawParams,
                    _raw_headers: R::RawHeaders,
                    _method: http::Method,
                    _target: Range<usize>,
                    _head: &[u8],
                    _body: &[u8],
                    _declared_body_len: usize,
                    _state: &S,
                    _encoder: &mut E,
                ) -> Decoded {
                    Decoded::Unsupported
                }
            }
        )+
    };
}

unsupported_decode_routes!(manifold::NativeFiber, manifold::NativeStream);

pub trait H1Project<'d, W: Wire> {
    fn chunk_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, State<C>>,
        bytes: &[u8],
        egress: &mut EgressCtx<'_, 'd, '_>,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
    ) -> bool
    where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn send_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, State<C>>,
        project: PJ,
        sent: usize,
        egress: &mut EgressCtx<'_, 'd, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn activate_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, State<C>>,
        project: PJ,
        egress: &mut EgressCtx<'_, 'd, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn close_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, State<C>>,
        project: PJ,
        egress: &mut EgressCtx<'_, 'd, '_>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;
}

#[doc(hidden)]
pub struct H1Hooks;

impl<'d, A, W> ApplicationHooks<'d, A> for H1Hooks
where
    A: Application<'d, Conn = conn_state::ConnState, Wire = W> + H1Project<'d, W>,
    W: Wire,
{
    fn chunk<R: RetainBytes>(
        app: Pin<&mut A>,
        slot: &mut link::slot::Slot<'d, W, State<conn_state::ConnState>>,
        mut egress: EgressCtx<'_, 'd, '_>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> dope::manifold::Outcome {
        if A::chunk_proj(
            app,
            slot,
            chunk.as_slice(),
            &mut egress,
            driver,
            identity_mut,
        ) {
            dope::manifold::Outcome::Overrun
        } else {
            dope::manifold::Outcome::Ok
        }
    }

    fn send(
        app: Pin<&mut A>,
        slot: &mut link::slot::Slot<'d, W, State<conn_state::ConnState>>,
        mut egress: EgressCtx<'_, 'd, '_>,
        sent: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        A::send_proj(app, slot, identity_mut, sent, &mut egress, driver);
    }

    fn activate(
        app: Pin<&mut A>,
        slot: &mut link::slot::Slot<'d, W, State<conn_state::ConnState>>,
        mut egress: EgressCtx<'_, 'd, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        A::activate_proj(app, slot, identity_mut, &mut egress, driver);
    }

    fn close(
        app: Pin<&mut A>,
        slot: &mut link::slot::Slot<'d, W, State<conn_state::ConnState>>,
        mut egress: EgressCtx<'_, 'd, '_>,
    ) {
        A::close_proj(app, slot, identity_mut, &mut egress);
    }
}
