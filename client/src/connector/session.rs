use std::cell::Cell;
use std::io::{self, Read};
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use cartel_core::{Arena, Limits};
use dope::driver::token::Token;
use dope::manifold::connector;
use dope::manifold::timer::Timer;
use dope::runtime::Idle;
use dope_fiber::WaitQueue;
use o3::buffer::{Lease, Pool as BufferPool, PoolLayout};
use o3::cell::RawCell;
use o3::collections::SlotQueue;
use o3::mem::ByteBudget;
use sark_core::http::Response;
use sark_core::http::codec::{DecodeMode, HeaderLookup, ResponseDecoder};

use crate::connector::codec::{self, Head};
use crate::connector::error::Error;
use crate::connector::retry::RetryPolicy;

pub(super) type Outcome = Result<Response, Error>;

pub(super) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub const DEFAULT_MAX_INFLIGHT_PER_CONNECTION: usize = 256;

pub const DEFAULT_RESPONSE_BUFFER_CAPACITY: usize = 64 * 1024 * 1024;

pub const DEFAULT_REQUEST_SLOTS: u32 = 256;

pub const DEFAULT_REQUEST_CAPACITY: u32 = 64 * 1024;

const KEEPALIVE_MARGIN: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecompressionPolicy {
    #[default]
    Strict,
    Lenient,
}
#[derive(Default)]
pub struct ConnState {
    pending_close: bool,
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> connector::Close {
        if self.pending_close {
            connector::Close::Reconnect
        } else {
            connector::Close::Keep
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

struct ConnEntry<'d> {
    conn_id: Cell<Option<Token>>,
    arena: Arena<'d, Outcome>,
    last_activity: Cell<Option<Instant>>,
    keepalive: Cell<Option<Duration>>,
    queued: Cell<bool>,
}

struct Pool<'d> {
    entries: Box<[ConnEntry<'d>]>,
    ready: RawCell<SlotQueue<Token>>,
    live: Cell<usize>,
    _budget: Pin<Rc<ByteBudget>>,
}

impl<'d> Pool<'d> {
    fn new(capacity: usize, max_inflight: usize, limit: usize) -> Self {
        let budget = Rc::pin(ByteBudget::new(limit));
        let limits = Limits::new(1, limit, 1);
        Self {
            entries: (0..capacity)
                .map(|_| ConnEntry {
                    conn_id: Cell::new(None),
                    arena: Arena::with_shared_budget(max_inflight, limits, budget.clone()),
                    last_activity: Cell::new(None),
                    keepalive: Cell::new(None),
                    queued: Cell::new(false),
                })
                .collect(),
            ready: RawCell::new(SlotQueue::with_capacity(capacity)),
            live: Cell::new(0),
            _budget: budget,
        }
    }

    fn entry<'a>(&'a self, conn_id: Token) -> Option<&'a ConnEntry<'d>> {
        let entry = self.entries.get(conn_id.slot().raw() as usize)?;
        (entry.conn_id.get() == Some(conn_id)).then_some(entry)
    }

    fn push_ready(&self, entry: &ConnEntry<'d>, conn_id: Token) {
        if entry.queued.replace(true) {
            return;
        }
        unsafe {
            self.ready.with_mut(|ready| {
                ready
                    .vacant_entry(conn_id.slot().raw() as usize)
                    .expect("ready queue entry must be vacant")
                    .push_back(conn_id);
            })
        };
    }

    fn pop_ready(&self) -> Option<Token> {
        unsafe { self.ready.with_mut(SlotQueue::pop_front) }
    }

    fn remove_ready(&self, conn_id: Token) {
        unsafe {
            self.ready
                .with_mut(|ready| ready.remove(conn_id.slot().raw() as usize))
        };
    }
}

pub struct Shared<'d> {
    pool: Pool<'d>,
    active_waiters: Pin<Box<WaitQueue>>,
    pub(super) host: String,
    pub(super) origin: http::Uri,
    pub(super) decompression: DecompressionPolicy,
    pub(super) max_redirects: u32,
    pub(super) retry: RetryPolicy,
    pub(super) idle_timeout: Duration,
    pub(super) request_timeout: Duration,
}

impl<'d> Shared<'d> {
    pub fn has_connection(&self) -> bool {
        self.pool.live.get() != 0
    }

    pub(super) fn connection_count(&self) -> usize {
        self.pool.live.get()
    }

    pub(super) fn wake(&self) {
        self.active_waiters.as_ref().wake();
    }

    pub(super) fn try_register_active(
        &self,
        waiter: Pin<&dope_fiber::Waiter<'d>>,
        context: Pin<&dope_fiber::Context<'_, 'd>>,
    ) -> bool {
        self.active_waiters.as_ref().try_register(waiter, context)
    }

    fn note_connect(&self, conn_id: Token, now: Instant) {
        let entry = &self.pool.entries[conn_id.slot().raw() as usize];
        if entry.conn_id.replace(Some(conn_id)).is_none() {
            self.pool.live.set(self.pool.live.get() + 1);
        }
        entry.last_activity.set(Some(now));
        entry.keepalive.set(None);
        self.pool.push_ready(entry, conn_id);
        self.wake();
    }

    fn push_response(
        &self,
        conn_id: Token,
        outcome: Outcome,
        bytes: usize,
        keepalive: Option<Duration>,
        now: Instant,
    ) {
        let Some(entry) = self.pool.entry(conn_id) else {
            return;
        };
        entry.last_activity.set(Some(now));
        if keepalive.is_some() {
            entry.keepalive.set(keepalive);
        }
        entry.arena.try_push(outcome, bytes, 1);
        entry.arena.complete();
        if entry.arena.can_register() {
            self.pool.push_ready(entry, conn_id);
        }
        self.wake();
    }

    pub(super) fn close_connection(&self, conn_id: Token) {
        let Some(entry) = self.pool.entry(conn_id) else {
            return;
        };
        entry.arena.fail_all(|| Err(Error::Closed));
        entry.conn_id.set(None);
        self.pool.live.set(self.pool.live.get() - 1);
        entry.last_activity.set(None);
        entry.keepalive.set(None);
        entry.queued.set(false);
        self.pool.remove_ready(conn_id);
        self.wake();
    }

    pub fn acquire(
        &self,
        now: Instant,
        idle_timeout: Duration,
        mut recycle: impl FnMut(Token),
    ) -> Option<Token> {
        while let Some(conn_id) = self.pool.pop_ready() {
            let Some(entry) = self.pool.entry(conn_id) else {
                continue;
            };
            entry.queued.set(false);
            let limit = entry
                .keepalive
                .get()
                .map(|k| k.saturating_sub(KEEPALIVE_MARGIN))
                .unwrap_or(idle_timeout);
            let stale = entry
                .last_activity
                .get()
                .is_some_and(|last| now.saturating_duration_since(last) >= limit);
            if stale {
                self.close_connection(conn_id);
                recycle(conn_id);
                continue;
            }
            if !entry.arena.can_register() {
                continue;
            }
            return Some(conn_id);
        }
        None
    }

    pub fn arena(&'d self, conn_id: Token) -> Option<&'d Arena<'d, Outcome>> {
        Some(&self.pool.entry(conn_id)?.arena)
    }

    pub fn submitted(&self, conn_id: Token, now: Instant) {
        if let Some(entry) = self.pool.entry(conn_id) {
            entry.last_activity.set(Some(now));
            if entry.arena.can_register() {
                self.pool.push_ready(entry, conn_id);
            }
        }
    }

    pub fn make_available(&self, conn_id: Token) {
        if let Some(entry) = self.pool.entry(conn_id)
            && entry.arena.can_register()
        {
            self.pool.push_ready(entry, conn_id);
        }
    }
}

pub struct Config {
    codec: codec::Codec,
    host: String,
    origin: http::Uri,
    decompression: DecompressionPolicy,
    max_redirects: u32,
    retry: RetryPolicy,
    idle_timeout: Duration,
    request_timeout: Duration,
    max_inflight_per_connection: usize,
    response_buffer_capacity: usize,
    request_slots: u32,
    request_capacity: u32,
}

impl Config {
    pub fn new(host: impl Into<String>) -> Self {
        let host = host.into();
        let origin = format!("http://{host}/")
            .parse()
            .expect("invalid HTTP host");
        Self {
            codec: codec::Codec::default(),
            host,
            origin,
            decompression: DecompressionPolicy::Strict,
            max_redirects: 10,
            retry: RetryPolicy::default(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_inflight_per_connection: DEFAULT_MAX_INFLIGHT_PER_CONNECTION,
            response_buffer_capacity: DEFAULT_RESPONSE_BUFFER_CAPACITY,
            request_slots: DEFAULT_REQUEST_SLOTS,
            request_capacity: DEFAULT_REQUEST_CAPACITY,
        }
    }

    pub fn with_decompression(host: impl Into<String>, policy: DecompressionPolicy) -> Self {
        let mut config = Self::new(host);
        config.decompression = policy;
        config
    }

    pub fn max_response_body(mut self, cap: usize) -> Self {
        self.codec.max_response_body = cap;
        self
    }

    pub fn max_redirects(mut self, max: u32) -> Self {
        self.max_redirects = max;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn idle_timeout(mut self, idle: Duration) -> Self {
        self.idle_timeout = idle;
        self
    }

    pub fn request_timeout(mut self, dur: Duration) -> Self {
        self.request_timeout = dur;
        self
    }

    pub fn max_inflight_per_connection(mut self, max: usize) -> Self {
        self.max_inflight_per_connection = max;
        self
    }

    pub fn response_buffer_capacity(mut self, capacity: usize) -> Self {
        self.response_buffer_capacity = capacity;
        self
    }

    pub fn request_pool(mut self, slots: u32, capacity: u32) -> Self {
        self.request_slots = slots;
        self.request_capacity = capacity;
        self
    }

    fn request_pool_layout(&self) -> io::Result<PoolLayout> {
        if self.request_slots == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request pool must have slots",
            ));
        }
        PoolLayout::new(self.request_slots, self.request_capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }
}

pub struct Port<'d> {
    pub(super) io: connector::Port<'d, Lease<'d>>,
    pub(super) shared: Shared<'d>,
    codec: codec::Codec,
    timer: Timer<'d>,
    pub(super) requests: Pin<Box<BufferPool>>,
}

pub struct PortFactory {
    config: Config,
    capacity: usize,
    timer_capacity: usize,
    request_pool: PoolLayout,
}

impl<'d> Port<'d> {
    pub fn new(
        config: Config,
        capacity: usize,
        timer_capacity: usize,
        driver: dope::DriverRef<'d>,
    ) -> io::Result<Self> {
        let request_pool = config.request_pool_layout()?;
        Ok(Self::new_with_pool(
            config,
            capacity,
            timer_capacity,
            request_pool,
            driver,
        ))
    }

    fn new_with_pool(
        config: Config,
        capacity: usize,
        timer_capacity: usize,
        request_pool: PoolLayout,
        driver: dope::DriverRef<'d>,
    ) -> Self {
        let Config {
            codec,
            host,
            origin,
            decompression,
            max_redirects,
            retry,
            idle_timeout,
            request_timeout,
            max_inflight_per_connection,
            response_buffer_capacity,
            request_slots: _,
            request_capacity: _,
        } = config;
        let shared = Shared {
            pool: Pool::new(
                capacity,
                max_inflight_per_connection,
                response_buffer_capacity,
            ),
            active_waiters: Box::pin(WaitQueue::with_capacity(timer_capacity)),
            host,
            origin,
            decompression,
            max_redirects,
            retry,
            idle_timeout,
            request_timeout,
        };
        Self {
            io: connector::Port::with_capacity(capacity, driver),
            shared,
            codec,
            timer: Timer::with_capacity(timer_capacity, driver),
            requests: Box::pin(BufferPool::new(request_pool)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.io.capacity()
    }

    pub fn factory(
        config: Config,
        capacity: usize,
        timer_capacity: usize,
    ) -> io::Result<PortFactory> {
        let request_pool = config.request_pool_layout()?;
        Ok(PortFactory {
            config,
            capacity,
            timer_capacity,
            request_pool,
        })
    }

    pub(super) fn timer(&'d self) -> &'d Timer<'d> {
        &self.timer
    }
}

impl dope::runtime::StorageFactory for PortFactory {
    type Output<'d> = Port<'d>;

    fn build<'d>(self, driver: &mut dope::DriverContext<'_, 'd>) -> Self::Output<'d> {
        Port::new_with_pool(
            self.config,
            self.capacity,
            self.timer_capacity,
            self.request_pool,
            driver.driver_ref(),
        )
    }
}

pub struct Session<'d> {
    pub(super) port: &'d Port<'d>,
}

impl<'d> Session<'d> {
    pub fn new(port: &'d Port<'d>) -> Self {
        Self { port }
    }
}

#[dope_gen::connector_session(codec = port.codec, io = port.io)]
impl<'d> connector::Session<'d> for Session<'d> {
    type Codec = codec::Codec;
    type ConnState = ConnState;
    type Send = Lease<'d>;

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        self.port.shared.note_connect(ctx.conn_id, Instant::now());
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        if let Some(reason) = head.error {
            let bytes = head.full.len();
            self.port.shared.push_response(
                ctx.conn_id,
                Err(Error::Parse(reason.into())),
                bytes,
                None,
                Instant::now(),
            );
            ctx.state.pending_close = true;
            return;
        }
        let buffered = head.full.len();
        let bytes = head.full.as_ref();
        let (outcome, keep_alive, keepalive_timeout) =
            match ResponseDecoder::new(DecodeMode::Response).response(bytes) {
                Ok(Some(mut resp)) => {
                    let keep = Self::should_keep_alive(&resp);
                    let timeout = Self::keepalive_timeout(&resp);
                    let outcome = match Self::decompress(
                        &mut resp,
                        self.port.shared.decompression,
                        self.port.codec.max_response_body,
                    ) {
                        Ok(()) => Ok(resp),
                        Err(e) => Err(e),
                    };
                    (outcome, keep, timeout)
                }
                Ok(None) => (
                    Err(Error::Parse("incomplete response frame".into())),
                    true,
                    None,
                ),
                Err(e) => (Err(Error::Parse(e.to_string())), true, None),
            };
        if !keep_alive {
            ctx.state.pending_close = true;
        }
        let buffered = outcome
            .as_ref()
            .map_or(buffered, |response| buffered.max(response.body().len()));
        self.port.shared.push_response(
            ctx.conn_id,
            outcome,
            buffered,
            keepalive_timeout,
            Instant::now(),
        );
    }

    fn disconnect(&mut self, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        self.port.io.deactivate(ctx.conn_id);
        self.port.shared.close_connection(ctx.conn_id);
        ctx.state.pending_close = false;
    }

    fn pre_park(&mut self) {
        self.port.timer.expire(Instant::now());
    }

    fn idle(&self) -> Idle {
        Idle::Park(self.port.timer.earliest())
    }
}

impl Session<'_> {
    fn should_keep_alive(resp: &Response) -> bool {
        let headers = resp.headers();
        if headers.has_token(http::header::CONNECTION, "close")
            || headers.has_token(http::header::CONNECTION, "upgrade")
        {
            return false;
        }
        if headers.has_token(http::header::CONNECTION, "keep-alive") {
            return true;
        }

        let status = resp.status().as_u16();
        if status < 200 || status == 204 || status == 304 {
            return true;
        }

        headers.contains_key(http::header::CONTENT_LENGTH)
            || headers.value_eq_ascii_case(http::header::TRANSFER_ENCODING, "chunked")
    }

    fn keepalive_timeout(resp: &Response) -> Option<Duration> {
        let name = http::header::HeaderName::from_static("keep-alive");
        let raw = resp.headers().get(name)?;
        let value = raw.to_str().ok()?;
        for part in value.split(',') {
            let part = part.trim();
            if let Some(rest) = part.strip_prefix("timeout=")
                && let Ok(secs) = rest.trim().parse::<u64>()
            {
                return Some(Duration::from_secs(secs));
            }
        }
        None
    }

    fn decompress(
        resp: &mut Response,
        policy: DecompressionPolicy,
        max_body: usize,
    ) -> Result<(), Error> {
        let is_gzip = resp
            .headers()
            .value_eq_ascii_case(http::header::CONTENT_ENCODING, "gzip");
        if !is_gzip || resp.body().is_empty() {
            return Ok(());
        }

        let limit = max_body as u64;
        let mut decoder = flate2::read::GzDecoder::new(resp.body()).take(limit + 1);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) if decompressed.len() as u64 > limit => Err(Error::Parse(
                "decompressed response body exceeds size limit".into(),
            )),
            Ok(_) => {
                resp.set_body(decompressed);
                resp.headers_mut().remove("content-encoding");
                resp.headers_mut().remove("content-length");
                Ok(())
            }
            Err(e) if policy == DecompressionPolicy::Strict => {
                Err(Error::Parse(format!("gzip decompression failed: {e}")))
            }
            Err(_) => Ok(()),
        }
    }
}
