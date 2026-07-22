use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use dope::DriverContext;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, SlotEgress};
use dope::runtime::profile::Throughput;
use dope_fiber::{
    Context, ErasedTaskId, Fiber, Slab, SlotExt as _, TaskContext, TaskId, TaskQueue, Waker,
};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;
use o3::collections::{FixedHashTable, FixedQueue};

use super::{Body, Config};
use crate::conn::{self, Conn, ConnError};
use crate::frame::ErrorCode;
use crate::hpack::{Header, HeaderBlock, OwnedHeader};
use crate::role::ServerRole;
use crate::stream::StreamId;

type ServerProfile = Throughput;

/// The listener keeps the ready target alive while the returned waker is used.
unsafe fn rebrand_waker<'from, 'to>(waker: Waker<'from>) -> Waker<'to> {
    unsafe { std::mem::transmute(waker) }
}

#[derive(Clone, Copy)]
struct Limits {
    max_request_body_bytes: usize,
    max_connection_body_bytes: usize,
    max_outbound_bytes: usize,
}

impl From<Config> for Limits {
    fn from(config: Config) -> Self {
        Self {
            max_request_body_bytes: config.max_request_body_bytes,
            max_connection_body_bytes: config.max_connection_body_bytes,
            max_outbound_bytes: config.max_outbound_bytes,
        }
    }
}

pub struct Request {
    pub headers: HeaderBlock,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &[u8]) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|header| header.name == name)
            .map(|header| header.value)
    }

    pub fn path(&self) -> Option<&[u8]> {
        self.header(b":path")
    }
}

enum ResponseHeaders {
    Owned(Vec<OwnedHeader>),
    Static(&'static [Header<'static>]),
}

pub struct Response {
    headers: ResponseHeaders,
    body: Body,
    pub trailers: Vec<OwnedHeader>,
}

impl Response {
    pub fn new(headers: Vec<OwnedHeader>, body: impl Into<Body>) -> Self {
        Self {
            headers: ResponseHeaders::Owned(headers),
            body: body.into(),
            trailers: Vec::new(),
        }
    }

    pub fn from_static(headers: &'static [Header<'static>], body: impl Into<Body>) -> Self {
        Self {
            headers: ResponseHeaders::Static(headers),
            body: body.into(),
            trailers: Vec::new(),
        }
    }

    pub fn text(body: impl Into<Body>) -> Self {
        const HEADERS: &[Header<'static>] = &[
            Header::new(b":status", b"200"),
            Header::new(b"content-type", b"text/plain; charset=utf-8"),
        ];
        Self::from_static(HEADERS, body)
    }

    pub fn body(&self) -> &Body {
        &self.body
    }
}

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

struct Incoming {
    stream_id: StreamId,
    headers: HeaderBlock,
    body: Vec<u8>,
}

impl From<Incoming> for Request {
    fn from(incoming: Incoming) -> Self {
        Self {
            headers: incoming.headers,
            body: incoming.body,
        }
    }
}

struct PendingBody {
    stream_id: StreamId,
    body: Body,
    offset: usize,
    stalled: bool,
    trailers: Vec<OwnedHeader>,
    trailers_sent: bool,
}

impl PendingBody {
    fn emit_trailers(&mut self, connection: &mut Conn<ServerRole>) -> Result<(), ConnError> {
        if self.trailers.is_empty() || self.trailers_sent {
            return Ok(());
        }
        let fields: Vec<Header<'_>> = self.trailers.iter().map(OwnedHeader::as_ref).collect();
        connection.send_trailers(self.stream_id, &fields)?;
        self.trailers_sent = true;
        Ok(())
    }

    fn pump(
        &mut self,
        connection: &mut Conn<ServerRole>,
        max_outbound_bytes: usize,
    ) -> Result<bool, ConnError> {
        loop {
            if self.offset >= self.body.len() {
                self.emit_trailers(connection)?;
                return Ok(true);
            }
            if connection.outbound().len() >= max_outbound_bytes {
                self.stalled = false;
                return Ok(false);
            }
            let remaining = &self.body.as_slice()[self.offset..];
            let end_stream = self.trailers.is_empty();
            let written = connection.send_data(self.stream_id, remaining, end_stream)?;
            if written == 0 {
                self.stalled = true;
                return Ok(false);
            }
            self.offset += written;
        }
    }
}

struct ConnectionState {
    connection: Conn<ServerRole>,
    incoming: FixedHashTable<Incoming>,
    pending: FixedQueue<PendingBody>,
    buffered_body_bytes: usize,
}

impl Default for ConnectionState {
    fn default() -> Self {
        let connection = Conn::<ServerRole>::with_tuning::<ServerProfile>();
        let capacity = connection.local_settings().max_concurrent_streams.unwrap() as usize;
        Self {
            connection,
            incoming: FixedHashTable::with_capacity(capacity),
            pending: FixedQueue::with_capacity(capacity),
            buffered_body_bytes: 0,
        }
    }
}

impl ConnectionState {
    fn incoming(&self, stream_id: StreamId) -> Option<&Incoming> {
        self.incoming
            .get(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
    }

    fn incoming_mut(&mut self, stream_id: StreamId) -> Option<&mut Incoming> {
        self.incoming
            .get_mut(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
    }

    fn insert_incoming(&mut self, incoming: Incoming) -> bool {
        let stream_id = incoming.stream_id;
        self.incoming
            .try_insert(u64::from(stream_id.0), incoming, |entry| {
                entry.stream_id == stream_id
            })
            .is_ok()
    }

    fn take_incoming(&mut self, stream_id: StreamId) -> Option<Incoming> {
        let incoming = self
            .incoming
            .remove(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)?;
        self.buffered_body_bytes = self.buffered_body_bytes.saturating_sub(incoming.body.len());
        Some(incoming)
    }

    fn receive_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        end_stream: bool,
        limits: Limits,
    ) -> Option<Request> {
        let body_bytes = self
            .incoming(stream_id)?
            .body
            .len()
            .saturating_add(data.len());
        let connection_body_bytes = self.buffered_body_bytes.saturating_add(data.len());
        if body_bytes > limits.max_request_body_bytes
            || connection_body_bytes > limits.max_connection_body_bytes
        {
            self.take_incoming(stream_id);
            let _ = self
                .connection
                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
            return None;
        }
        let incoming = self.incoming_mut(stream_id).unwrap();
        incoming.body.extend_from_slice(data);
        self.buffered_body_bytes = connection_body_bytes;
        end_stream
            .then(|| self.take_incoming(stream_id).map(Request::from))
            .flatten()
    }

    fn begin_response(&mut self, stream_id: StreamId, response: Response, limits: Limits) {
        if !self.connection.has_stream(stream_id) {
            return;
        }
        let has_trailers = !response.trailers.is_empty();
        let end_stream = response.body.is_empty() && !has_trailers;
        let sent = match &response.headers {
            ResponseHeaders::Owned(headers) => self.connection.send_response(
                stream_id,
                headers.iter().map(OwnedHeader::as_ref),
                end_stream,
            ),
            ResponseHeaders::Static(headers) => {
                self.connection
                    .send_response(stream_id, headers.iter().copied(), end_stream)
            }
        };
        if sent.is_err() || end_stream {
            return;
        }
        let mut body = PendingBody {
            stream_id,
            body: response.body,
            offset: 0,
            stalled: false,
            trailers: response.trailers,
            trailers_sent: false,
        };
        match body.pump(&mut self.connection, limits.max_outbound_bytes) {
            Ok(true) => {}
            Ok(false) | Err(_) => match self.pending.vacant_entry() {
                Some(entry) => entry.push_back(body),
                None => {
                    let _ = self
                        .connection
                        .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                }
            },
        }
    }

    fn resume_pending(&mut self, limits: Limits) {
        if !self.connection.take_window_opened() {
            return;
        }
        self.pump_pending(limits, true);
    }

    fn pump_pending(&mut self, limits: Limits, resume: bool) {
        let len = self.pending.len();
        for _ in 0..len {
            let mut body = self.pending.pop_front().unwrap();
            if resume {
                body.stalled = false;
            }
            if body.stalled || self.connection.outbound().len() >= limits.max_outbound_bytes {
                self.pending.vacant_entry().unwrap().push_back(body);
                continue;
            }
            if matches!(
                body.pump(&mut self.connection, limits.max_outbound_bytes),
                Ok(false)
            ) {
                self.pending.vacant_entry().unwrap().push_back(body);
            }
        }
    }

    fn reset_stream(&mut self, stream_id: StreamId) {
        self.take_incoming(stream_id);
        self.pending.retain(|body| body.stream_id != stream_id);
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

    fn drain_events(&self, state: &mut ConnectionState) -> usize {
        let mut drained = 0;
        while let Some(event) = state.connection.poll_event() {
            drained += 1;
            match event {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } => {
                    if trailing {
                        if let Some(mut incoming) = state.take_incoming(stream_id) {
                            if incoming.headers.append(headers).is_err() {
                                let _ = state
                                    .connection
                                    .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                                continue;
                            }
                            let response = self.user.request(incoming.into());
                            state.begin_response(stream_id, response, self.limits);
                        }
                    } else if end_stream {
                        let request = Request {
                            headers,
                            body: Vec::new(),
                        };
                        let response = self.user.request(request);
                        state.begin_response(stream_id, response, self.limits);
                    } else {
                        if !state.insert_incoming(Incoming {
                            stream_id,
                            headers,
                            body: Vec::new(),
                        }) {
                            let _ = state
                                .connection
                                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                        }
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(request) =
                        state.receive_data(stream_id, &data, end_stream, self.limits)
                    {
                        let response = self.user.request(request);
                        state.begin_response(stream_id, response, self.limits);
                    }
                }
                conn::Event::StreamReset { stream_id, .. } => state.reset_stream(stream_id),
                _ => {}
            }
        }
        state.resume_pending(self.limits);
        drained
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
        if state.connection.goaway_sent() || state.connection.goaway_received().is_some() {
            return Outcome::Ok;
        }
        let mut result = state.connection.ingest(chunk.as_slice());
        let error = loop {
            let drained = this.drain_events(state);
            match result {
                Ok(()) => break None,
                Err(conn::ConnError::Overload) if drained != 0 => {
                    result = state.connection.resume();
                }
                Err(error) => break Some(error),
            }
        };
        if let Some(error) = error {
            let _ = state.connection.goaway(ErrorCode::from(&error), b"");
            flush_into(slot, aux, driver, true);
            return Outcome::Ok;
        }
        let close_after = slot.state.conn.state.connection.goaway_sent();
        flush_into(slot, aux, driver, close_after);
        Outcome::Ok
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<SyncConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let limits = self.get_mut().limits;
        let state = &mut slot.state.conn.state;
        state.pump_pending(limits, false);
        let close_after = state.connection.goaway_sent();
        if !state.connection.outbound().is_empty() {
            flush_into(slot, aux, driver, close_after);
        }
    }

    fn close(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<SyncConnState>>,
        _aux: &mut listener::Aux,
    ) {
        let state = &mut slot.state.conn.state;
        state.pending.clear();
        state.incoming.clear();
    }
}

type TaskTarget = Option<ErasedTaskId>;

struct TaskWake<'d> {
    task: TaskContext<TaskTarget>,
    bound: bool,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> TaskWake<'d> {
    fn new() -> Self {
        Self {
            task: TaskContext::with_target(None),
            bound: false,
            driver: PhantomData,
        }
    }

    unsafe fn bind(
        mut self: Pin<&mut Self>,
        key: ErasedTaskId,
        ready: Pin<&TaskQueue<TaskTarget>>,
        parent: Waker<'_>,
    ) {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let task = unsafe { Pin::new_unchecked(&this.task) };
        let _ = unsafe { task.bind_child(ready, Some(key), parent) };
        this.bound = true;
    }

    fn waker(self: Pin<&Self>) -> Waker<'d> {
        unsafe { self.map_unchecked(|this| &this.task).context_unchecked() }
    }

    unsafe fn unbind(mut self: Pin<&mut Self>) {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        if !this.bound {
            return;
        }
        unsafe { Pin::new_unchecked(&this.task).unbind() };
        this.bound = false;
    }

    fn at_mut(wakes: Pin<&mut [Self]>, index: usize) -> Pin<&mut Self> {
        unsafe { wakes.map_unchecked_mut(|wakes| &mut wakes[index]) }
    }
}

struct UnbindOnDrop<'a, 'd>(Option<Pin<&'a mut TaskWake<'d>>>);

impl Drop for UnbindOnDrop<'_, '_> {
    fn drop(&mut self) {
        unsafe { self.0.take().unwrap().unbind() };
    }
}

struct RunningTask {
    connection_id: dope::driver::token::Token,
    stream_id: StreamId,
    task: TaskId,
    key: ErasedTaskId,
    previous: Option<u32>,
    next: Option<u32>,
}

#[derive(Clone, Copy)]
struct TaskMapEntry {
    connection_id: dope::driver::token::Token,
    stream_id: StreamId,
    index: u32,
}

struct TaskMap {
    entries: FixedHashTable<TaskMapEntry>,
}

impl TaskMap {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: FixedHashTable::with_capacity(capacity),
        }
    }

    fn hash(connection_id: dope::driver::token::Token, stream_id: StreamId) -> u64 {
        let value = connection_id.raw() ^ u64::from(stream_id.0).wrapping_mul(0x9E37_79B9);
        value.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    fn insert(
        &mut self,
        connection_id: dope::driver::token::Token,
        stream_id: StreamId,
        index: usize,
    ) -> bool {
        self.entries
            .try_insert(
                Self::hash(connection_id, stream_id),
                TaskMapEntry {
                    connection_id,
                    stream_id,
                    index: index as u32,
                },
                |entry| entry.connection_id == connection_id && entry.stream_id == stream_id,
            )
            .is_ok()
    }

    fn remove(
        &mut self,
        connection_id: dope::driver::token::Token,
        stream_id: StreamId,
    ) -> Option<usize> {
        self.entries
            .remove(Self::hash(connection_id, stream_id), |entry| {
                entry.connection_id == connection_id && entry.stream_id == stream_id
            })
            .map(|entry| entry.index as usize)
    }

    fn get(&self, connection_id: dope::driver::token::Token, stream_id: StreamId) -> Option<usize> {
        self.entries
            .get(Self::hash(connection_id, stream_id), |entry| {
                entry.connection_id == connection_id && entry.stream_id == stream_id
            })
            .map(|entry| entry.index as usize)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub struct ConnState {
    state: ConnectionState,
    ready: TaskQueue<TaskTarget>,
    task_head: Option<u32>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            state: ConnectionState::default(),
            ready: TaskQueue::new(),
            task_head: None,
        }
    }
}

impl ConnectionContainer for ConnState {
    fn connection(&mut self) -> &mut ConnectionState {
        &mut self.state
    }
}

impl ConnState {
    unsafe fn ready_pin(&self) -> Pin<&TaskQueue<TaskTarget>> {
        unsafe { Pin::new_unchecked(&self.ready) }
    }
}

pub struct App<'d, H: Handler + 'd, W: Wire = Identity> {
    user: &'d H,
    limits: Limits,
    slab: Slab<'d, H::Fut<'d>>,
    tasks: Box<[Option<RunningTask>]>,
    wakes: Pin<Box<[TaskWake<'d>]>>,
    task_map: TaskMap,
    wire: PhantomData<fn() -> W>,
}

trait TaskPollState<'d, H: Handler + 'd, W: Wire> {
    fn index(&self) -> usize;
    fn task<'a>(&'a self, tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId>;
    fn release(&mut self, app: &mut App<'d, H, W>, task_head: &mut Option<u32>)
    -> Option<StreamId>;
}

struct NewTask {
    task: Option<TaskId>,
    index: usize,
}

impl NewTask {
    fn new(task: TaskId) -> Self {
        let index = task.index();
        Self {
            task: Some(task),
            index,
        }
    }
}

impl<'d, H: Handler + 'd, W: Wire> TaskPollState<'d, H, W> for NewTask {
    fn index(&self) -> usize {
        self.index
    }

    fn task<'a>(&'a self, _tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId> {
        self.task.as_ref()
    }

    fn release(
        &mut self,
        app: &mut App<'d, H, W>,
        _task_head: &mut Option<u32>,
    ) -> Option<StreamId> {
        let task = self.task.take()?;
        app.release_bound_task(self.index, task);
        None
    }
}

struct RegisteredTask {
    index: usize,
}

impl<'d, H: Handler + 'd, W: Wire> TaskPollState<'d, H, W> for RegisteredTask {
    fn index(&self) -> usize {
        self.index
    }

    fn task<'a>(&'a self, tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId> {
        tasks
            .get(self.index)
            .and_then(Option::as_ref)
            .map(|task| &task.task)
    }

    fn release(
        &mut self,
        app: &mut App<'d, H, W>,
        task_head: &mut Option<u32>,
    ) -> Option<StreamId> {
        app.release_task(task_head, self.index)
    }
}

struct TaskPoll<'a, 'd, H: Handler + 'd, W: Wire, S: TaskPollState<'d, H, W>> {
    app: &'a mut App<'d, H, W>,
    task_head: &'a mut Option<u32>,
    poll_state: S,
}

impl<'a, 'd, H, W, S> TaskPoll<'a, 'd, H, W, S>
where
    H: Handler + 'd,
    W: Wire,
    S: TaskPollState<'d, H, W>,
{
    fn new(app: &'a mut App<'d, H, W>, task_head: &'a mut Option<u32>, poll_state: S) -> Self {
        Self {
            app,
            task_head,
            poll_state,
        }
    }

    fn poll(&mut self, driver: &mut DriverContext<'_, 'd>) -> Option<Poll<Response>> {
        let index = self.poll_state.index();
        let App {
            slab, tasks, wakes, ..
        } = self.app;
        let task = self.poll_state.task(tasks)?;
        let wake = TaskWake::at_mut(wakes.as_mut(), index);
        let waker = wake.as_ref().waker();
        let mut context = std::pin::pin!(Context::from_waker(waker, driver.reborrow()));
        slab.poll(task, context.as_mut())
    }

    fn complete(mut self) -> Option<StreamId> {
        let stream_id = self.poll_state.release(self.app, self.task_head);
        std::mem::forget(self);
        stream_id
    }

    fn preserve(self) {
        std::mem::forget(self);
    }
}

impl<'a, 'd, H: Handler + 'd, W: Wire> TaskPoll<'a, 'd, H, W, NewTask> {
    unsafe fn bind(
        &mut self,
        key: ErasedTaskId,
        ready: Pin<&TaskQueue<TaskTarget>>,
        parent: Waker<'_>,
    ) {
        let wake = TaskWake::at_mut(self.app.wakes.as_mut(), self.poll_state.index);
        unsafe { wake.bind(key, ready, parent) };
    }

    fn register(
        mut self,
        connection_id: dope::driver::token::Token,
        stream_id: StreamId,
        key: ErasedTaskId,
    ) -> bool {
        let registered = self.app.register_task(
            self.task_head,
            self.poll_state.index,
            &mut self.poll_state.task,
            connection_id,
            stream_id,
            key,
        );
        if registered {
            std::mem::forget(self);
        }
        registered
    }
}

impl<'d, H, W, S> Drop for TaskPoll<'_, 'd, H, W, S>
where
    H: Handler + 'd,
    W: Wire,
    S: TaskPollState<'d, H, W>,
{
    fn drop(&mut self) {
        let _ = self.poll_state.release(self.app, self.task_head);
    }
}

impl<'d, H: Handler + 'd, W: Wire> App<'d, H, W> {
    pub fn new(user: &'d H, config: Config) -> Self {
        let capacity = config.max_handler_tasks;
        assert!(capacity > 0);
        assert!(u32::try_from(capacity).is_ok());
        Self {
            user,
            limits: config.into(),
            slab: Slab::with_capacity(capacity),
            tasks: (0..capacity).map(|_| None).collect(),
            wakes: Box::into_pin((0..capacity).map(|_| TaskWake::new()).collect()),
            task_map: TaskMap::with_capacity(capacity),
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
        let parent = unsafe { rebrand_waker(slot.waker()) };
        let state = &mut slot.state.conn;
        let mut drained = 0;
        while let Some(event) = state.state.connection.poll_event() {
            drained += 1;
            match event {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } => {
                    if trailing {
                        if let Some(mut incoming) = state.state.take_incoming(stream_id) {
                            if incoming.headers.append(headers).is_err() {
                                let _ = state
                                    .state
                                    .connection
                                    .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                                continue;
                            }
                            self.dispatch(
                                connection_id,
                                state,
                                stream_id,
                                incoming.into(),
                                parent,
                                driver,
                            );
                        }
                    } else if end_stream {
                        let request = Request {
                            headers,
                            body: Vec::new(),
                        };
                        self.dispatch(connection_id, state, stream_id, request, parent, driver);
                    } else {
                        if !state.state.insert_incoming(Incoming {
                            stream_id,
                            headers,
                            body: Vec::new(),
                        }) {
                            let _ = state
                                .state
                                .connection
                                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                        }
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(request) =
                        state
                            .state
                            .receive_data(stream_id, &data, end_stream, self.limits)
                    {
                        self.dispatch(connection_id, state, stream_id, request, parent, driver);
                    }
                }
                conn::Event::StreamReset { stream_id, .. } => {
                    state.state.reset_stream(stream_id);
                    self.cancel_task(state, connection_id, stream_id);
                }
                _ => {}
            }
        }
        state.state.resume_pending(self.limits);
        drained
    }

    fn dispatch(
        &mut self,
        connection_id: dope::driver::token::Token,
        state: &mut ConnState,
        stream_id: StreamId,
        request: Request,
        parent: Waker<'_>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let Some(entry) = self.slab.vacant_entry() else {
            let _ = state
                .state
                .connection
                .reset_stream(stream_id, ErrorCode::RefusedStream);
            return;
        };
        let fiber = self.user.request(request);
        let task = entry.insert(fiber);
        let key = task.erase();
        let task = TaskId::from_erased(key);
        let ready = unsafe { Pin::new_unchecked(&state.ready) };
        let mut task = TaskPoll::new(self, &mut state.task_head, NewTask::new(task));
        unsafe { task.bind(key, ready, parent) };
        match task.poll(driver) {
            Some(Poll::Ready(response)) => {
                let _ = task.complete();
                state.state.begin_response(stream_id, response, self.limits);
            }
            Some(Poll::Pending) => {
                if !task.register(connection_id, stream_id, key) {
                    let _ = state
                        .state
                        .connection
                        .reset_stream(stream_id, ErrorCode::RefusedStream);
                }
            }
            None => {
                debug_assert!(false, "live task must exist in fiber slab");
                let _ = task.complete();
                let _ = state
                    .state
                    .connection
                    .reset_stream(stream_id, ErrorCode::InternalError);
            }
        }
    }

    fn register_task(
        &mut self,
        task_head: &mut Option<u32>,
        index: usize,
        task: &mut Option<TaskId>,
        connection_id: dope::driver::token::Token,
        stream_id: StreamId,
        key: ErasedTaskId,
    ) -> bool {
        let Some(slot) = self.tasks.get(index) else {
            return false;
        };
        if slot.is_some() {
            return false;
        }
        if let Some(next) = *task_head {
            let Some(next_task) = self.tasks.get(next as usize).and_then(Option::as_ref) else {
                return false;
            };
            if next_task.connection_id != connection_id {
                return false;
            }
        }
        if !self.task_map.insert(connection_id, stream_id, index) {
            return false;
        }
        let Some(task) = task.take() else {
            self.task_map.remove(connection_id, stream_id);
            return false;
        };
        self.tasks[index] = Some(RunningTask {
            connection_id,
            stream_id,
            task,
            key,
            previous: None,
            next: *task_head,
        });
        let next = *task_head;
        *task_head = Some(index as u32);
        if let Some(next) = next
            && let Some(next) = self.tasks[next as usize].as_mut()
        {
            next.previous = Some(index as u32);
        }
        true
    }

    fn release_task(&mut self, task_head: &mut Option<u32>, index: usize) -> Option<StreamId> {
        let running = self.tasks.get_mut(index)?.take()?;
        let RunningTask {
            connection_id,
            stream_id,
            task,
            previous,
            next,
            ..
        } = running;
        self.task_map.remove(connection_id, stream_id);
        if let Some(previous) = previous {
            if let Some(previous) = self.tasks[previous as usize].as_mut() {
                previous.next = next;
            }
        } else if *task_head == Some(index as u32) {
            *task_head = next;
        }
        if let Some(next) = next
            && let Some(next) = self.tasks[next as usize].as_mut()
        {
            next.previous = previous;
        }
        self.release_bound_task(index, task);
        Some(stream_id)
    }

    fn release_bound_task(&mut self, index: usize, task: TaskId) {
        let App { slab, wakes, .. } = self;
        let unbind = UnbindOnDrop(Some(TaskWake::at_mut(wakes.as_mut(), index)));
        let removed = slab.remove(task);
        debug_assert!(removed, "live task must be removable");
        drop(unbind);
    }

    fn cancel_task(
        &mut self,
        state: &mut ConnState,
        connection_id: dope::driver::token::Token,
        stream_id: StreamId,
    ) {
        let Some(index) = self.task_map.get(connection_id, stream_id) else {
            return;
        };
        self.release_task(&mut state.task_head, index);
    }
}

impl<'d, H: Handler + 'd, W: Wire> Drop for App<'d, H, W> {
    fn drop(&mut self) {
        assert!(self.tasks.iter().all(Option::is_none));
        assert!(self.task_map.is_empty());
        assert!(self.wakes.iter().all(|wake| !wake.bound));
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
        if slot.state.conn.state.connection.goaway_sent()
            || slot.state.conn.state.connection.goaway_received().is_some()
        {
            return Outcome::Ok;
        }
        let mut result = slot.state.conn.state.connection.ingest(chunk.as_slice());
        let error = loop {
            let drained = this.drain_events(slot, driver);
            match result {
                Ok(()) => break None,
                Err(conn::ConnError::Overload) if drained != 0 => {
                    result = slot.state.conn.state.connection.resume();
                }
                Err(error) => break Some(error),
            }
        };
        if let Some(error) = error {
            let state = &mut slot.state.conn.state;
            let _ = state.connection.goaway(ErrorCode::from(&error), b"");
            flush_into(slot, aux, driver, true);
            return Outcome::Ok;
        }
        let close_after = slot.state.conn.state.connection.goaway_sent();
        flush_into(slot, aux, driver, close_after);
        Outcome::Ok
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let limits = self.get_mut().limits;
        let state = &mut slot.state.conn.state;
        state.pump_pending(limits, false);
        let close_after = state.connection.goaway_sent();
        if !state.connection.outbound().is_empty() {
            flush_into(slot, aux, driver, close_after);
        }
    }

    fn activate(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let this = self.get_mut();
        if unsafe { slot.state.conn.ready_pin() }.is_empty() {
            return;
        }
        let connection_id = slot.token();
        let parent = unsafe { rebrand_waker(slot.waker()) };
        {
            let state = &mut slot.state.conn;
            let ready = unsafe { Pin::new_unchecked(&state.ready) };
            let snapshot = unsafe { ready.snapshot(parent) };
            for key in snapshot.flatten() {
                let index = key.index();
                let Some((running_key, running_connection_id)) = this
                    .tasks
                    .get(index)
                    .and_then(Option::as_ref)
                    .map(|running| (running.key, running.connection_id))
                else {
                    continue;
                };
                if running_key != key || running_connection_id != connection_id {
                    continue;
                }
                let mut task = TaskPoll::new(this, &mut state.task_head, RegisteredTask { index });
                match task.poll(driver) {
                    Some(Poll::Ready(response)) => {
                        let Some(stream_id) = task.complete() else {
                            debug_assert!(false, "registered task must be releasable");
                            continue;
                        };
                        state.state.begin_response(stream_id, response, this.limits);
                    }
                    Some(Poll::Pending) => task.preserve(),
                    None => {
                        debug_assert!(false, "live task must exist in fiber slab");
                        if let Some(stream_id) = task.complete() {
                            let _ = state
                                .state
                                .connection
                                .reset_stream(stream_id, ErrorCode::InternalError);
                        }
                    }
                }
            }
        }
        let close_after = slot.state.conn.state.connection.goaway_sent();
        if !slot.state.conn.state.connection.outbound().is_empty() {
            flush_into(slot, aux, driver, close_after);
        }
    }

    fn close(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
        let this = self.get_mut();
        let state = &mut slot.state.conn;
        while let Some(index) = state.task_head {
            if this
                .release_task(&mut state.task_head, index as usize)
                .is_none()
            {
                state.task_head = None;
            }
        }
        state.state.pending.clear();
        state.state.incoming.clear();
    }
}
