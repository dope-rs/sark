use std::ops::Range;
use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::listener::state::{EgressCtx, State};
use dope_fiber::abi::Fiber;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::Shared;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit, Outcome};
use super::requests::{Ctx, Matched, RequestDomainInput, assemble_matched};
use super::tasks::TaskRunner;
use crate::fiber::FixedSlab;
use crate::request::RequestStorage;
use crate::service::RouteSpec;
use crate::service::manifold::{NativeFiber, NativeStream};
use crate::{CANNED_503, Timer};

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
        egress: &mut EgressCtx<'_, 'a, '_>,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll;
}

impl<'d, R, F> Complete<'d, R, F> for NativeFiber
where
    R: RouteSpec<Kind = NativeFiber>,
    F: Fiber<'d, Output = Result<R::AsyncResponse, &'static [u8]>>,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <F as Fiber<'d>>::Output,
        slot: &mut Slot<'a, W, State<C>>,
        egress: &mut EgressCtx<'_, 'a, '_>,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll {
        match output {
            Ok(response) => {
                TaskRunner::new(date).finish::<R, W, C>(response, slot, egress, driver, close);
            }
            Err(reason) => {
                Outcome::Close(reason).apply(slot, egress, driver);
            }
        }
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
        _egress: &mut EgressCtx<'_, 'a, '_>,
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
        F: Fiber<'d, Output = Result<R::AsyncResponse, &'static [u8]>>,
        MK: FnOnce(
            RequestStorage,
            R::RawParams,
            R::RawHeaders,
            Range<usize>,
            &'env S,
            &'env Timer<'d>,
        ) -> F,
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
        let task = make(
            storage,
            raw_params,
            raw_headers,
            target,
            self.state,
            self.timer,
        );
        let task = entry.insert(task);
        self.conn.async_state.task = Some(task.erase());
        self.conn.async_state.task_stream = false;
        ConsumeOutcome::Park {
            consumed: total,
            close: conn_close,
        }
    }
}
