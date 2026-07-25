use std::marker::PhantomData;
use std::ops::Range;
use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::listener;
use dope_fiber::{Fiber, SplitTask, SplitView};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::Shared;
use sark_core::http::Shape;
use sark_core::http::compress::Gzip;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit};
use super::invocation::{StreamRoute, SyncRoute};
use super::requests::{Ctx, Matched, RequestDomainInput, assemble_matched};
use super::response_cache::Cache;
use super::tasks::TaskRunner;
use crate::fiber::FixedSlab;
use crate::request::{Ref, RequestStorage};
use crate::service::manifold::{self, InvokeKind, Kind, NativeFiber, NativeStream, TaskRoute};
use crate::service::{RouteRequestImpl, RouteSpec};
use crate::{CANNED_400, CANNED_503, Timer};

pub struct RequestTask<R, S>(PhantomData<fn() -> (R, S)>);

impl<'d, R, S> SplitTask<'d> for RequestTask<R, S>
where
    R: RouteSpec + TaskRoute<'d, S> + 'static,
    R::Kind: InvokeKind<R, Output = R::AsyncResponse>,
{
    type Input = (R::RawParams, R::RawHeaders, Range<usize>);
    type State = S;
    type Context = Timer<'d>;
    type Output = R::AsyncResponse;
    type Error = &'static [u8];

    fn build<'req>(
        view: SplitView<'req>,
        (raw_params, raw_headers, target): Self::Input,
        state: &'req Self::State,
        timer: &'req Self::Context,
    ) -> Result<impl Fiber<'d, Output = Self::Output> + 'req, Self::Error>
    where
        'd: 'req,
        S: 'req,
    {
        let (head, body) = view.into_parts();
        let request = Ref::from_slice(target, head, body);
        let params = R::Request::build_params(&request, raw_params).ok_or(CANNED_400)?;
        let headers = R::Request::build_headers(&request, raw_headers).map_err(|_| CANNED_400)?;
        let parsed_body = R::parse_body(body).map_err(|_| CANNED_400)?;
        Ok(R::invoke_task(
            params,
            request,
            headers,
            parsed_body,
            state,
            timer,
        ))
    }
}

pub enum TaskPoll {
    Complete,
    Stream(Option<Shared>),
}

pub trait Complete<'d, R, F>: Kind<'d, R, F>
where
    R: RouteSpec,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as Kind<'d, R, F>>::Output,
        slot: &mut Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll;
}

impl<'d, R: RouteSpec, F> Complete<'d, R, F> for manifold::Sync {
    fn complete<'a, W: Wire, C: Default + 'static>(
        _output: <Self as Kind<'d, R, F>>::Output,
        _slot: &mut Slot<'a, W, listener::State<C>>,
        _aux: &mut listener::Aux,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        unreachable!()
    }
}

impl<'d, R, F> Complete<'d, R, F> for NativeFiber
where
    R: RouteSpec,
    F: Fiber<'d, Output = R::AsyncResponse>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as Kind<'d, R, F>>::Output,
        slot: &mut Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll {
        TaskRunner::new(date).finish::<R, W, C>(output, slot, aux, driver, close);
        TaskPoll::Complete
    }
}

impl<'d, R, F> Complete<'d, R, F> for NativeStream
where
    R: RouteSpec,
    R::Stream: Fiber<'d, Output = Option<Shared>>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as Kind<'d, R, F>>::Output,
        _slot: &mut Slot<'a, W, listener::State<C>>,
        _aux: &mut listener::Aux,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        TaskPoll::Stream(output)
    }
}

pub trait Dispatch<'env, 'd, R, S, F>
where
    R: RouteSpec,
    'd: 'env,
{
    #[allow(clippy::too_many_arguments)]
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: DispatchPermit,
        matched: Matched<R>,
        tasks: Pin<&mut FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        timer: &'env Timer<'d>,
        conn: &mut ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> ConsumeOutcome
    where
        T: Fiber<'d>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(<Self as Kind<'d, R, F>>::Task, <Self as Kind<'d, R, F>>::Owner) -> T,
        Self: Kind<'d, R, F>;
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for manifold::Sync
where
    R: RouteSpec + manifold::Route<S> + 'static,
    'd: 'env,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: DispatchPermit,
        matched: Matched<R>,
        _tasks: Pin<&mut FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        _timer: &'env Timer<'d>,
        _conn: &mut ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        _wrap: Wrap,
    ) -> ConsumeOutcome
    where
        T: Fiber<'d>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(<Self as Kind<'d, R, F>>::Task, <Self as Kind<'d, R, F>>::Owner) -> T,
        Self: Kind<'d, R, F>,
    {
        SyncRoute::new(ctx, date, cache, gzip, write).dispatch(permit, matched, state)
    }
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for NativeFiber
where
    R: RouteSpec + 'static,
    F: Fiber<'d, Output = R::AsyncResponse>,
    'd: 'env,
    NativeFiber: Kind<'d, R, F, Task = F, Owner = ()>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: DispatchPermit,
        matched: Matched<R>,
        mut tasks: Pin<&mut FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        timer: &'env Timer<'d>,
        conn: &mut ConnState,
        _date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        _write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> ConsumeOutcome
    where
        T: Fiber<'d>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(<Self as Kind<'d, R, F>>::Task, <Self as Kind<'d, R, F>>::Owner) -> T,
        Self: Kind<'d, R, F>,
    {
        let RequestDomainInput {
            storage,
            raw_params,
            raw_headers,
            target,
            total,
            conn_close,
        } = match assemble_matched(permit, matched, ctx, conn) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return ConsumeOutcome::Close(CANNED_503);
        };
        let task = match make(storage, raw_params, raw_headers, target, state, timer) {
            Ok(task) => wrap(task, ()),
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        let task = entry.insert(task);
        conn.async_state.task = Some(task.erase());
        conn.async_state.task_stream = false;
        ConsumeOutcome::Park {
            consumed: total,
            close: conn_close,
        }
    }
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for NativeStream
where
    R: RouteSpec + manifold::Route<S> + 'static,
    for<'req> R::Response<'req>: Shape<'req, StreamInner = R::Stream>,
    R::Stream: Fiber<'d, Output = Option<Shared>>,
    'd: 'env,
    NativeStream: Kind<'d, R, F, Task = R::Stream, Owner = ()>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: DispatchPermit,
        matched: Matched<R>,
        tasks: Pin<&mut FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        _timer: &'env Timer<'d>,
        conn: &mut ConnState,
        date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        wrap: Wrap,
    ) -> ConsumeOutcome
    where
        T: Fiber<'d>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(<Self as Kind<'d, R, F>>::Task, <Self as Kind<'d, R, F>>::Owner) -> T,
        Self: Kind<'d, R, F>,
    {
        StreamRoute::new(ctx, write, date, conn)
            .dispatch(permit, matched, tasks, state, |task| wrap(task, ()))
    }
}
