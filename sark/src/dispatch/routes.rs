use std::marker::PhantomData;
use std::ops::Range;
use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::listener::state::{EgressCtx, State};
use dope_fiber::abi::Fiber;
use dope_fiber::owner::{SplitTask, SplitView};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::Shared;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit};
use super::requests::{Ctx, Matched, RequestDomainInput, assemble_matched};
use super::tasks::TaskRunner;
use crate::fiber::FixedSlab;
use crate::request::{Ref, RequestStorage};
use crate::service::manifold::{NativeFiber, NativeStream, TaskRoute};
use crate::service::{RouteRequestImpl, RouteSpec};
use crate::{CANNED_400, CANNED_503, Timer};

pub struct RequestTask<R, S>(PhantomData<fn() -> (R, S)>);

impl<'d, R, S> SplitTask<'d> for RequestTask<R, S>
where
    R: RouteSpec + TaskRoute<'d, S> + 'static,
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

pub trait Complete<'d, R, T>
where
    R: RouteSpec,
    T: Fiber<'d>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <T as Fiber<'d>>::Output,
        slot: &mut Slot<'a, W, State<C>>,
        egress: &mut EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll;
}

impl<'d, R, F> Complete<'d, R, F> for NativeFiber
where
    R: RouteSpec<Kind = NativeFiber>,
    F: Fiber<'d, Output = R::AsyncResponse>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <F as Fiber<'d>>::Output,
        slot: &mut Slot<'a, W, State<C>>,
        egress: &mut EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll {
        TaskRunner::new(date).finish::<R, W, C>(output, slot, egress, driver, close);
        TaskPoll::Complete
    }
}

impl<'d, R, T> Complete<'d, R, T> for NativeStream
where
    R: RouteSpec<Kind = NativeStream, Stream = T>,
    T: Fiber<'d, Output = Option<Shared>> + 'static,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <T as Fiber<'d>>::Output,
        _slot: &mut Slot<'a, W, State<C>>,
        _egress: &mut EgressCtx<'_, '_>,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        TaskPoll::Stream(output)
    }
}

pub struct FiberRoute<'a, 'req, 'env, 'd, S>
where
    'd: 'env,
{
    ctx: &'a Ctx<'req>,
    state: &'env S,
    timer: &'env Timer<'d>,
    conn: &'a mut ConnState,
}

impl<'a, 'req, 'env, 'd, S> FiberRoute<'a, 'req, 'env, 'd, S>
where
    'd: 'env,
{
    pub fn new(
        ctx: &'a Ctx<'req>,
        state: &'env S,
        timer: &'env Timer<'d>,
        conn: &'a mut ConnState,
    ) -> Self {
        Self {
            ctx,
            state,
            timer,
            conn,
        }
    }

    pub fn dispatch<R, F, Tag, MK, const N: usize>(
        self,
        permit: DispatchPermit,
        matched: Matched<R>,
        mut tasks: Pin<&mut FixedSlab<'d, F, N, Tag>>,
        make: MK,
    ) -> ConsumeOutcome
    where
        R: RouteSpec<Kind = NativeFiber> + 'static,
        F: Fiber<'d, Output = R::AsyncResponse>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> Result<F, &'static [u8]>,
    {
        let RequestDomainInput {
            storage,
            raw_params,
            raw_headers,
            target,
            total,
            conn_close,
        } = match assemble_matched(permit, matched, self.ctx, self.conn) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return ConsumeOutcome::Close(CANNED_503);
        };
        let task = match make(
            storage,
            raw_params,
            raw_headers,
            target,
            self.state,
            self.timer,
        ) {
            Ok(task) => task,
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        let task = entry.insert(task);
        self.conn.async_state.task = Some(task.erase());
        self.conn.async_state.task_stream = false;
        ConsumeOutcome::Park {
            consumed: total,
            close: conn_close,
        }
    }
}
