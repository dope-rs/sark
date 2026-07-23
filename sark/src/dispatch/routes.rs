use std::ops::Range;
use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::listener;
use dope_net::link;
use dope_net::wire::Wire;
use o3::buffer::Shared;
use sark_core::http::Shape;
use sark_core::http::compress::Gzip;

use super::conn_state;
use super::invocation::{StreamRoute, SyncRoute};
use super::requests::{Ctx, Matched, RequestDomainInput, assemble_matched};
use super::response_cache::Cache;
use super::tasks::TaskRunner;
use crate::request;
use crate::service::{self, RouteRequestImpl, RouteSpec, manifold};

pub struct RequestTask<R, S>(core::marker::PhantomData<fn() -> (R, S)>);

impl<'d, R, S> dope_fiber::SplitTask<'d> for RequestTask<R, S>
where
    R: RouteSpec + manifold::TaskRoute<'d, S> + 'static,
    R::Kind: manifold::InvokeKind<R, Output = R::AsyncResponse>,
{
    type Input = (R::RawParams, R::RawHeaders, Range<usize>);
    type State = S;
    type Context = crate::Timer<'d>;
    type Output = R::AsyncResponse;
    type Error = &'static [u8];

    fn build<'req>(
        view: dope_fiber::SplitView<'req>,
        (raw_params, raw_headers, target): Self::Input,
        state: &'req Self::State,
        timer: &'req Self::Context,
    ) -> Result<impl dope_fiber::Fiber<'d, Output = Self::Output> + 'req, Self::Error>
    where
        'd: 'req,
        S: 'req,
    {
        let (head, body) = view.into_parts();
        let request = request::Ref::from_slice(target, head, body);
        let params = R::Request::build_params(&request, raw_params).ok_or(crate::CANNED_400)?;
        let headers =
            R::Request::build_headers(&request, raw_headers).map_err(|_| crate::CANNED_400)?;
        let parsed_body = R::parse_body(body).map_err(|_| crate::CANNED_400)?;
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

pub trait Complete<'d, R, F>: service::manifold::Kind<'d, R, F>
where
    R: RouteSpec,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll;
}

impl<'d, R: RouteSpec, F> Complete<'d, R, F> for service::manifold::Sync {
    fn complete<'a, W: Wire, C: Default + 'static>(
        _output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        _slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        _aux: &mut listener::Aux,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        unreachable!()
    }
}

impl<'d, R, F> Complete<'d, R, F> for service::manifold::NativeFiber
where
    R: RouteSpec,
    F: dope_fiber::Fiber<'d, Output = R::AsyncResponse>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll {
        TaskRunner::new(date).finish::<R, W, C>(output, slot, aux, driver, close);
        TaskPoll::Complete
    }
}

impl<'d, R, F> Complete<'d, R, F> for service::manifold::NativeStream
where
    R: RouteSpec,
    R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        _slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
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
        permit: conn_state::DispatchPermit,
        matched: Matched<R>,
        tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        timer: &'env crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d>,
        MK: FnOnce(
            request::RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env crate::Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>;
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for service::manifold::Sync
where
    R: RouteSpec + manifold::Route<S> + 'static,
    'd: 'env,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<R>,
        _tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        _timer: &'env crate::Timer<'d>,
        _conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        _wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d>,
        MK: FnOnce(
            request::RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env crate::Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
    {
        SyncRoute::new(ctx, date, cache, gzip, write).dispatch(permit, matched, state)
    }
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for service::manifold::NativeFiber
where
    R: RouteSpec + 'static,
    F: dope_fiber::Fiber<'d, Output = R::AsyncResponse>,
    'd: 'env,
    service::manifold::NativeFiber: service::manifold::Kind<'d, R, F, Task = F, Owner = ()>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<R>,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        timer: &'env crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        _date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        _write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d>,
        MK: FnOnce(
            request::RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env crate::Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
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
            return conn_state::ConsumeOutcome::Close(crate::CANNED_503);
        };
        let task = match make(storage, raw_params, raw_headers, target, state, timer) {
            Ok(task) => wrap(task, ()),
            Err(reason) => return conn_state::ConsumeOutcome::Close(reason),
        };
        let task = entry.insert(task);
        conn.async_state.task = Some(task.erase());
        conn.async_state.task_stream = false;
        conn_state::ConsumeOutcome::Park {
            consumed: total,
            close: conn_close,
        }
    }
}

impl<'env, 'd, R, S, F> Dispatch<'env, 'd, R, S, F> for service::manifold::NativeStream
where
    R: RouteSpec + manifold::Route<S> + 'static,
    for<'req> R::Response<'req>: Shape<'req, StreamInner = R::Stream>,
    R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>>,
    'd: 'env,
    service::manifold::NativeStream:
        service::manifold::Kind<'d, R, F, Task = R::Stream, Owner = ()>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<R>,
        tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        ctx: &Ctx<'_>,
        _timer: &'env crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d>,
        MK: FnOnce(
            request::RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env crate::Timer<'d>,
        ) -> Result<F, &'static [u8]>,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
    {
        StreamRoute::new(ctx, write, date, conn)
            .dispatch(permit, matched, tasks, state, |task| wrap(task, ()))
    }
}
