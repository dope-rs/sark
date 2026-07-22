use std::marker::PhantomData;
use std::pin::Pin;

use dope::DriverContext;
use dope::driver::token::Token;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, SlotEgress as _};
use dope_fiber::{Fiber, SlotExt as _, TaskQueue};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;

use super::Config;
use super::connection::{ConnectionState, Dispatch, EventSink, Limits, Request, Response};
use super::scheduler::{Resumed, Scheduler, Started};
use super::task::TaskTarget;
use crate::conn::{Conn, ConnError};
use crate::frame::ErrorCode;
use crate::role::ServerRole;
use crate::stream::StreamId;

pub trait Handler: 'static {
    type Fut<'h>: Fiber<'h, Output = Response> + 'h
    where
        Self: 'h;

    fn request<'h>(&'h self, request: Request) -> Self::Fut<'h>;
}

pub trait SyncHandler: 'static {
    fn request(&self, request: Request) -> Response;
}

impl<F> SyncHandler for F
where
    F: Fn(Request) -> Response + 'static,
{
    fn request(&self, request: Request) -> Response {
        self(request)
    }
}

trait ConnectionContainer: Default + 'static {
    fn connection(&mut self) -> &mut ConnectionState;
}

fn flush_into<'d, W, C>(
    slot: &mut Slot<'d, W, listener::State<C>>,
    aux: &mut listener::Aux,
    driver: &mut DriverContext<'_, 'd>,
    close_after: bool,
) where
    W: Wire,
    C: ConnectionContainer,
{
    let send_token = slot.token();
    let mut write_buffer = aux.write_buf_for(slot);
    let state = slot.state.conn.connection();
    let written = state.connection.drain_into(&mut write_buffer);
    if close_after {
        slot.set_close_after();
    }
    slot.submit_buffered(write_buffer, written, send_token, driver);
}

fn finish_ingest<'d, W, C>(
    slot: &mut Slot<'d, W, listener::State<C>>,
    aux: &mut listener::Aux,
    driver: &mut DriverContext<'_, 'd>,
    error: Option<ConnError>,
) -> Outcome
where
    W: Wire,
    C: ConnectionContainer,
{
    if let Some(error) = error {
        let state = slot.state.conn.connection();
        let _ = state.connection.goaway(ErrorCode::from(&error), b"");
        flush_into(slot, aux, driver, true);
    } else {
        flush_connection(slot, aux, driver);
    }
    Outcome::Ok
}

fn resume_egress<'d, W, C>(
    slot: &mut Slot<'d, W, listener::State<C>>,
    limits: Limits,
    aux: &mut listener::Aux,
    driver: &mut DriverContext<'_, 'd>,
) where
    W: Wire,
    C: ConnectionContainer,
{
    let state = slot.state.conn.connection();
    state.pump_pending(limits, false);
    if !state.connection.outbound().is_empty() {
        flush_connection(slot, aux, driver);
    }
}

#[derive(Default)]
pub struct SyncConnState {
    state: ConnectionState,
}

impl ConnectionContainer for SyncConnState {
    fn connection(&mut self) -> &mut ConnectionState {
        &mut self.state
    }
}

pub struct SyncApp<'h, H: SyncHandler, W: Wire = Identity> {
    user: &'h H,
    limits: Limits,
    wire: PhantomData<fn() -> W>,
}

impl<'h, H: SyncHandler, W: Wire> SyncApp<'h, H, W> {
    pub fn new(user: &'h H, config: Config) -> Self {
        Self {
            user,
            limits: config.into(),
            wire: PhantomData,
        }
    }

    pub fn handler(&self) -> &H {
        self.user
    }
}

struct SyncSink<'h, H: SyncHandler> {
    user: &'h H,
}

impl<H: SyncHandler> EventSink for SyncSink<'_, H> {
    fn request(&mut self, _stream_id: StreamId, request: Request) -> Dispatch {
        Dispatch::Response(self.user.request(request))
    }

    fn reset(&mut self, _stream_id: StreamId) {}
}

struct SyncTransport<'a, 'h, H: SyncHandler> {
    user: &'h H,
    limits: Limits,
    state: &'a mut ConnectionState,
}

impl<H: SyncHandler> super::driver::Transport for SyncTransport<'_, '_, H> {
    fn connection(&mut self) -> &mut Conn<ServerRole> {
        &mut self.state.connection
    }

    fn drain_events(&mut self) -> usize {
        self.state
            .drain_events(self.limits, &mut SyncSink { user: self.user })
    }
}

impl<'d, H: SyncHandler + 'd, W: Wire> Application<'d> for SyncApp<'d, H, W> {
    type Conn = SyncConnState;
    type Wire = W;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<SyncConnState>>,
        chunk: R,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        let state = &mut slot.state.conn.state;
        let error = super::driver::Driver::new(&mut SyncTransport {
            user: this.user,
            limits: this.limits,
            state,
        })
        .ingest(chunk.as_slice())
        .err();
        finish_ingest(slot, aux, driver, error)
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<SyncConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        resume_egress(slot, self.get_mut().limits, aux, driver);
    }

    fn close(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<SyncConnState>>,
        _aux: &mut listener::Aux,
    ) {
        slot.state.conn.state.close();
    }
}

pub struct ConnState {
    state: ConnectionState,
    ready: Pin<Box<TaskQueue<TaskTarget>>>,
    task_head: Option<u32>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            state: ConnectionState::default(),
            ready: Box::pin(TaskQueue::new()),
            task_head: None,
        }
    }
}

impl ConnectionContainer for ConnState {
    fn connection(&mut self) -> &mut ConnectionState {
        &mut self.state
    }
}

pub struct App<'d, H: Handler + 'd, W: Wire = Identity> {
    user: &'d H,
    limits: Limits,
    scheduler: Scheduler<'d, H::Fut<'d>>,
    wire: PhantomData<fn() -> W>,
}

impl<'d, H: Handler + 'd, W: Wire> App<'d, H, W> {
    pub fn new(user: &'d H, config: Config) -> Self {
        Self {
            user,
            limits: config.into(),
            scheduler: Scheduler::with_capacity(config.max_handler_tasks),
            wire: PhantomData,
        }
    }

    pub fn handler(&self) -> &H {
        self.user
    }

    fn drain_events(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> usize {
        let connection_id = slot.token();
        let parent = slot.root_waker();
        let ConnState {
            state,
            ready,
            task_head,
        } = &mut slot.state.conn;
        let ready = ready.as_ref();
        let mut sink = AsyncSink {
            user: self.user,
            scheduler: &mut self.scheduler,
            connection_id,
            ready,
            task_head,
            parent,
            driver,
        };
        state.drain_events(self.limits, &mut sink)
    }
}

struct AsyncSink<'a, 'turn, 'd, H>
where
    H: Handler + 'd,
{
    user: &'d H,
    scheduler: &'a mut Scheduler<'d, H::Fut<'d>>,
    connection_id: Token,
    ready: Pin<&'a TaskQueue<TaskTarget>>,
    task_head: &'a mut Option<u32>,
    parent: dope_fiber::RootWaker<'d>,
    driver: &'a mut DriverContext<'turn, 'd>,
}

impl<'d, H> EventSink for AsyncSink<'_, '_, 'd, H>
where
    H: Handler + 'd,
{
    fn request(&mut self, stream_id: StreamId, request: Request) -> Dispatch {
        match self.scheduler.start(
            self.user.request(request),
            self.connection_id,
            stream_id,
            self.task_head,
            self.ready,
            self.parent,
            self.driver,
        ) {
            Started::Ready(response) => Dispatch::Response(response),
            Started::Pending => Dispatch::Pending,
            Started::Refused => Dispatch::Reset(ErrorCode::RefusedStream),
            Started::Failed => Dispatch::Reset(ErrorCode::InternalError),
        }
    }

    fn reset(&mut self, stream_id: StreamId) {
        self.scheduler
            .cancel(self.task_head, self.connection_id, stream_id);
    }
}

struct AsyncTransport<'a, 'turn, 'd, H: Handler + 'd, W: Wire> {
    app: &'a mut App<'d, H, W>,
    slot: &'a mut Slot<'d, W, listener::State<ConnState>>,
    driver: &'a mut DriverContext<'turn, 'd>,
}

impl<'d, H: Handler + 'd, W: Wire> super::driver::Transport for AsyncTransport<'_, '_, 'd, H, W> {
    fn connection(&mut self) -> &mut Conn<ServerRole> {
        &mut self.slot.state.conn.state.connection
    }

    fn drain_events(&mut self) -> usize {
        self.app.drain_events(self.slot, self.driver)
    }
}

impl<'d, H: Handler + 'd, W: Wire> Application<'d> for App<'d, H, W> {
    type Conn = ConnState;
    type Wire = W;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        chunk: R,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        let error = super::driver::Driver::new(&mut AsyncTransport {
            app: this,
            slot,
            driver,
        })
        .ingest(chunk.as_slice())
        .err();
        finish_ingest(slot, aux, driver, error)
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        resume_egress(slot, self.get_mut().limits, aux, driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let this = self.get_mut();
        let parent = slot.root_waker();
        let connection_id = slot.token();
        let ConnState {
            state,
            ready,
            task_head,
        } = &mut slot.state.conn;
        let ready = ready.as_ref();
        if ready.is_empty() {
            return;
        }
        let Some(snapshot) = ready.snapshot_root(parent) else {
            return;
        };
        for key in snapshot.filter_map(TaskTarget::key) {
            match this.scheduler.resume(key, connection_id, task_head, driver) {
                Resumed::Ready(Some(stream_id), response) => {
                    state.begin_response(stream_id, response, this.limits);
                }
                Resumed::Ready(None, _) => {
                    debug_assert!(false, "registered task must be releasable");
                }
                Resumed::Failed(Some(stream_id)) => {
                    let _ = state
                        .connection
                        .reset_stream(stream_id, ErrorCode::InternalError);
                }
                Resumed::Pending | Resumed::Failed(None) | Resumed::Stale => {}
            }
        }
        if !state.connection.outbound().is_empty() {
            flush_connection(slot, aux, driver);
        }
    }

    fn close(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
        let this = self.get_mut();
        let state = &mut slot.state.conn;
        this.scheduler.close(&mut state.task_head);
        state.state.close();
    }
}

fn flush_connection<'d, W, C>(
    slot: &mut Slot<'d, W, listener::State<C>>,
    aux: &mut listener::Aux,
    driver: &mut DriverContext<'_, 'd>,
) where
    W: Wire,
    C: ConnectionContainer,
{
    let close_after = slot.state.conn.connection().connection.goaway_sent();
    flush_into(slot, aux, driver, close_after);
}
