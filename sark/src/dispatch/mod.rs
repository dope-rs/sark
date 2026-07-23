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
use dope::manifold::listener;
use dope_net::link;
use dope_net::wire::Wire;
pub use driver::{H1Driver, HeadDeadline};
pub use invocation::{Invocation, SyncRoute};
pub use pipeline::{Pipeline, identity_mut};
pub use requests::{Ctx, Framed, Matched};
pub use routes::{Complete, Dispatch, RequestTask, TaskPoll};
pub use routing::{H1Host, RouteCore, Routing};
use sark_core::http::Shape;
pub use tasks::TaskRunner;

use crate::service::{RouteSpec, manifold};

pub trait ResponseEncoder {
    fn emit(&mut self, status: http::StatusCode, headers_wire: &[u8], body: &[u8]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded {
    Emitted,
    NotFound,
    Bad,
    Unsupported,
}

pub trait Decode {
    fn dispatch_decoded<E: ResponseEncoder>(
        &self,
        _method: http::Method,
        path: &[u8],
        headers: &[(&[u8], Range<usize>)],
        head_bytes: &[u8],
        body_bytes: &[u8],
        encoder: &mut E,
    ) -> Decoded;
}

pub trait DecodeRoute<R: RouteSpec, S> {
    #[allow(clippy::too_many_arguments)]
    fn decode<E: ResponseEncoder>(
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        _method: http::Method,
        head: &[u8],
        body: &[u8],
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
        head: &[u8],
        body: &[u8],
        state: &S,
        encoder: &mut E,
    ) -> Decoded {
        match Invocation::new(0..0, head, body, body.len()).invoke::<R, S>(
            raw_params,
            raw_headers,
            state,
        ) {
            Ok(response) => match response.response_view() {
                Some(view) => {
                    ResponseEncoder::emit(
                        encoder,
                        view.status,
                        view.headers.as_ref(),
                        view.body.as_ref(),
                    );
                    Decoded::Emitted
                }
                None => Decoded::Unsupported,
            },
            Err(_) => Decoded::Bad,
        }
    }
}

impl<R: RouteSpec, S> DecodeRoute<R, S> for manifold::NativeFiber {
    fn decode<E: ResponseEncoder>(
        _raw_params: R::RawParams,
        _raw_headers: R::RawHeaders,
        _method: http::Method,
        _head: &[u8],
        _body: &[u8],
        _state: &S,
        _encoder: &mut E,
    ) -> Decoded {
        Decoded::Unsupported
    }
}

impl<R: RouteSpec, S> DecodeRoute<R, S> for manifold::NativeStream {
    fn decode<E: ResponseEncoder>(
        _raw_params: R::RawParams,
        _raw_headers: R::RawHeaders,
        _method: http::Method,
        _head: &[u8],
        _body: &[u8],
        _state: &S,
        _encoder: &mut E,
    ) -> Decoded {
        Decoded::Unsupported
    }
}

pub trait H1Project<'d, W: Wire> {
    fn chunk_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        bytes: &[u8],
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
    ) -> bool
    where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn send_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn activate_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn close_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        aux: &mut listener::Aux,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;
}
