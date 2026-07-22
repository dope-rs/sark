use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime};

use dope::hash;
use dope::io::file::{O_CLOEXEC, O_RDONLY, OpenPath};
use dope::manifold::file::Files;
use dope_fiber::file::{Metadata, Open, Read as FileRead, Source, Stat as FileStat};
use dope_fiber::{Context, Fiber, WaitQueue, Waiter};
use o3::buffer::Shared;
use o3::cell::RawCell;
use o3::collections::{FixedHashTable, PinSlab, Slab, SlabKey};
use o3::mem::{ByteBudget, ByteBudgetHandle, ByteLease};
use sark_core::http::Response;

const DEFAULT_MAX_FILE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_READ_BUDGET: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Br,
    Gzip,
}

impl Encoding {
    fn index(self) -> usize {
        match self {
            Encoding::Br => 0,
            Encoding::Gzip => 1,
        }
    }

    fn token(self) -> &'static [u8] {
        match self {
            Encoding::Br => b"br",
            Encoding::Gzip => b"gzip",
        }
    }

    fn header(self) -> &'static str {
        match self {
            Encoding::Br => "br",
            Encoding::Gzip => "gzip",
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Encoding::Br => ".br",
            Encoding::Gzip => ".gz",
        }
    }
}

const DEFAULT_CACHE_CAPACITY: usize = 256 * 1024 * 1024;
const DEFAULT_CACHE_ENTRIES: usize = 4096;
const DEFAULT_FLIGHT_CAPACITY: usize = 256;
const DEFAULT_CACHE_VALID: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum LoadError {
    NotFound,
    Overloaded,
}

impl LoadError {
    fn response(self) -> Response {
        match self {
            Self::NotFound => Response::not_found(),
            Self::Overloaded => Response::new(sark_core::http::StatusCode::SERVICE_UNAVAILABLE),
        }
    }
}

struct LoadedFile {
    body: Shared,
    metadata: Metadata,
}

struct ReadFile<'f, 'b, 'd, const ID: u8, const N: usize> {
    files: &'f Files<'d, ID, N>,
    path: Option<PathBuf>,
    budget: ByteBudgetHandle<'b>,
    max_file_bytes: usize,
    lease: Option<ByteLease<'b>>,
    metadata: Option<Metadata>,
    source: Option<Source<'d>>,
    open: Option<Open<'f, 'd, ID, N>>,
    stat: Option<FileStat<'f, 'd, ID, N>>,
    read: Option<FileRead<'f, 'd, ID, N>>,
    done: bool,
}

impl<'f, 'b, 'd, const ID: u8, const N: usize> ReadFile<'f, 'b, 'd, ID, N> {
    fn new(
        files: &'f Files<'d, ID, N>,
        path: PathBuf,
        budget: ByteBudgetHandle<'b>,
        max_file_bytes: usize,
    ) -> Self {
        Self {
            files,
            path: Some(path),
            budget,
            max_file_bytes,
            lease: None,
            metadata: None,
            source: None,
            open: None,
            stat: None,
            read: None,
            done: false,
        }
    }

    fn release(&mut self) {
        self.lease = None;
    }

    fn fail(&mut self, error: LoadError) -> Poll<Result<LoadedFile, LoadError>> {
        self.release();
        self.done = true;
        Poll::Ready(Err(error))
    }
}

impl<const ID: u8, const N: usize> Drop for ReadFile<'_, '_, '_, ID, N> {
    fn drop(&mut self) {
        self.release();
    }
}

impl<'f, 'b, 'd, const ID: u8, const N: usize> Fiber<'d> for ReadFile<'f, 'b, 'd, ID, N> {
    type Output = Result<LoadedFile, LoadError>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.done {
            panic!("file fiber polled after completion");
        }

        if this.open.is_none() && this.stat.is_none() && this.read.is_none() {
            let path = this.path.take().expect("file path missing");
            let Some(path) = path.to_str() else {
                return this.fail(LoadError::NotFound);
            };
            let Ok(path) = OpenPath::new(path) else {
                return this.fail(LoadError::NotFound);
            };
            this.open = Some(Open::direct(this.files, path, O_RDONLY | O_CLOEXEC));
        }

        if let Some(open) = this.open.as_mut() {
            let Poll::Ready(result) = Fiber::poll(Pin::new(open), cx.as_mut()) else {
                return Poll::Pending;
            };
            this.open = None;
            let Ok(source) = result else {
                return this.fail(LoadError::NotFound);
            };
            this.source = Some(source);
            this.stat = Some(FileStat::source(
                this.files,
                this.source.as_ref().expect("file source missing"),
            ));
        }

        if let Some(stat) = this.stat.as_mut() {
            let Poll::Ready(result) = Fiber::poll(Pin::new(stat), cx.as_mut()) else {
                return Poll::Pending;
            };
            this.stat = None;
            let Ok(metadata) = result else {
                return this.fail(LoadError::NotFound);
            };
            if !metadata.is_file() {
                return this.fail(LoadError::NotFound);
            }
            let Ok(expected) = usize::try_from(metadata.len()) else {
                return this.fail(LoadError::NotFound);
            };
            if expected > this.max_file_bytes {
                return this.fail(LoadError::NotFound);
            }
            let Some(lease) = this.budget.try_acquire(expected) else {
                return this.fail(LoadError::Overloaded);
            };
            this.lease = Some(lease);
            this.metadata = Some(metadata);
            let source = this.source.take().expect("file source missing");
            this.read = Some(FileRead::new(this.files, &source, vec![0; expected], 0));
        }

        let read = this.read.as_mut().expect("read child missing");
        let Poll::Ready((buffer, result)) = Fiber::poll(Pin::new(read), cx.as_mut()) else {
            return Poll::Pending;
        };
        this.read = None;
        let Ok(count) = result else {
            return this.fail(LoadError::NotFound);
        };
        let expected = this
            .metadata
            .as_ref()
            .and_then(|metadata| usize::try_from(metadata.len()).ok())
            .expect("file metadata missing");
        if count != expected || buffer.len() != expected {
            return this.fail(LoadError::NotFound);
        }
        let body = Shared::from(buffer);
        let metadata = this.metadata.take().expect("file metadata missing");
        this.release();
        this.done = true;
        Poll::Ready(Ok(LoadedFile { body, metadata }))
    }
}

#[derive(Clone)]
struct Variant {
    body: Shared,
}

#[derive(Clone)]
struct Asset {
    body: Shared,
    mime: &'static str,
    variants: [Option<Variant>; 2],
    mtime: Option<SystemTime>,
    size: u64,
    bytes: usize,
}

struct CacheEntry {
    asset: Asset,
    validated: Instant,
}

enum CacheTag {}

type CacheKey = SlabKey<CacheTag>;

struct CacheSlot {
    hash: u64,
    key: Box<[u8]>,
    entry: CacheEntry,
    prev: Option<CacheKey>,
    next: Option<CacheKey>,
}

struct CacheIndex {
    key: CacheKey,
}

enum AsyncCache {
    Hit(Response),
    Stale,
    Miss,
}

impl Asset {
    fn footprint(&self) -> usize {
        self.bytes
    }
}

struct Shard {
    index: Option<FixedHashTable<CacheIndex>>,
    entries: Slab<CacheSlot, CacheTag>,
    head: Option<CacheKey>,
    tail: Option<CacheKey>,
    total: usize,
}

impl Shard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index: (capacity != 0).then(|| FixedHashTable::with_capacity(capacity)),
            entries: Slab::with_capacity(capacity),
            head: None,
            tail: None,
            total: 0,
        }
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<CacheKey> {
        self.index
            .as_ref()?
            .get(hash, |index| {
                self.entries
                    .get(index.key)
                    .is_some_and(|slot| slot.key.as_ref() == key)
            })
            .map(|index| index.key)
    }

    fn unlink(&mut self, key: CacheKey) {
        let slot = self.entries.get(key).expect("cache key missing");
        let prev = slot.prev;
        let next = slot.next;
        match prev {
            Some(prev) => self.entries.get_mut(prev).expect("cache link missing").next = next,
            None => self.head = next,
        }
        match next {
            Some(next) => self.entries.get_mut(next).expect("cache link missing").prev = prev,
            None => self.tail = prev,
        }
    }

    fn link_back(&mut self, key: CacheKey) {
        let tail = self.tail;
        let slot = self.entries.get_mut(key).expect("cache key missing");
        slot.prev = tail;
        slot.next = None;
        match tail {
            Some(tail) => self.entries.get_mut(tail).expect("cache link missing").next = Some(key),
            None => self.head = Some(key),
        }
        self.tail = Some(key);
    }

    fn touch(&mut self, key: CacheKey) {
        if self.tail == Some(key) {
            return;
        }
        self.unlink(key);
        self.link_back(key);
    }

    fn remove(&mut self, key: CacheKey) -> Option<CacheSlot> {
        let hash = self.entries.get(key)?.hash;
        self.index
            .as_mut()?
            .remove(hash, |index| index.key == key)?;
        self.unlink(key);
        let slot = self.entries.remove(key)?;
        self.total -= slot.entry.asset.footprint();
        Some(slot)
    }

    fn evict(&mut self) -> bool {
        self.head.is_some_and(|key| self.remove(key).is_some())
    }

    fn insert(&mut self, hash: u64, key: &[u8], asset: Asset, capacity: usize) {
        if self.index.is_none() {
            return;
        }
        if let Some(existing) = self.find(hash, key) {
            self.remove(existing);
        }
        let incoming = asset.footprint();
        if incoming > capacity {
            return;
        }
        while self.total > capacity - incoming {
            if !self.evict() {
                return;
            }
        }
        if self.entries.is_full() && !self.evict() {
            return;
        }
        let entry = CacheEntry {
            asset,
            validated: Instant::now(),
        };
        let slot = CacheSlot {
            hash,
            key: Box::from(key),
            entry,
            prev: None,
            next: None,
        };
        let Ok(key) = self.entries.insert(slot) else {
            return;
        };
        if self
            .index
            .as_mut()
            .expect("cache index missing")
            .try_insert(hash, CacheIndex { key }, |_| false)
            .is_err()
        {
            self.entries.remove(key);
            return;
        }
        self.link_back(key);
        self.total += incoming;
    }
}

#[derive(Clone)]
enum FlightOutcome {
    Loaded(Asset),
    Failed(LoadError),
}

enum FlightTag {}

type FlightKey = SlabKey<FlightTag>;

struct Flight {
    hash: u64,
    key: Box<[u8]>,
    waiters: usize,
    waiter_capacity: usize,
    outcome: Option<FlightOutcome>,
    wake: WaitQueue,
}

impl Flight {
    fn wait_queue(&self) -> Pin<&WaitQueue> {
        unsafe { Pin::new_unchecked(&self.wake) }
    }
}

struct FlightIndex {
    key: FlightKey,
}

struct Flights {
    index: Option<FixedHashTable<FlightIndex>>,
    entries: PinSlab<Flight, FlightTag>,
    waiter_capacity: usize,
}

impl Flights {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index: (capacity != 0).then(|| FixedHashTable::with_capacity(capacity)),
            entries: PinSlab::with_capacity(capacity),
            waiter_capacity: capacity,
        }
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<FlightKey> {
        self.index
            .as_ref()?
            .get(hash, |index| {
                self.entries
                    .get(index.key)
                    .is_some_and(|flight| flight.key.as_ref() == key)
            })
            .map(|index| index.key)
    }

    fn remove(&mut self, key: FlightKey) {
        let Some(flight) = self.entries.get(key) else {
            return;
        };
        let hash = flight.hash;
        if let Some(index) = self.index.as_mut() {
            index.remove(hash, |index| index.key == key);
        }
        self.entries.remove(key);
    }
}

struct FlightLeader<'a> {
    state: &'a SharedState,
    key: FlightKey,
    done: bool,
}

impl FlightLeader<'_> {
    fn finish(mut self, outcome: FlightOutcome) {
        self.state.finish_flight(self.key, outcome);
        self.done = true;
    }
}

impl Drop for FlightLeader<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.state
                .finish_flight(self.key, FlightOutcome::Failed(LoadError::NotFound));
        }
    }
}

struct FlightWait<'a, 'd> {
    state: &'a SharedState,
    key: FlightKey,
    waiter: Waiter<'d>,
    done: bool,
}

impl<'d> Fiber<'d> for FlightWait<'_, 'd> {
    type Output = FlightOutcome;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state.with_flights(|flights| {
            let Some(outcome) = flights
                .entries
                .get(this.key)
                .map(|flight| flight.outcome.clone())
            else {
                this.done = true;
                return Poll::Ready(FlightOutcome::Failed(LoadError::NotFound));
            };
            if let Some(outcome) = outcome {
                unsafe { Pin::new_unchecked(&this.waiter) }.unregister();
                let waiters = {
                    let flight = flights.entries.get_mut(this.key).expect("flight missing");
                    let flight = unsafe { flight.get_unchecked_mut() };
                    flight.waiters -= 1;
                    flight.waiters
                };
                if waiters == 0 {
                    flights.remove(this.key);
                }
                this.done = true;
                return Poll::Ready(outcome);
            }
            let waiter = unsafe { Pin::new_unchecked(&this.waiter) };
            let registered = flights
                .entries
                .get(this.key)
                .is_some_and(|flight| flight.wait_queue().try_register(waiter, cx.as_ref()));
            if !registered {
                let flight = flights.entries.get_mut(this.key).expect("flight missing");
                unsafe { flight.get_unchecked_mut() }.waiters -= 1;
                this.done = true;
                return Poll::Ready(FlightOutcome::Failed(LoadError::Overloaded));
            }
            Poll::Pending
        })
    }
}

impl Drop for FlightWait<'_, '_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        unsafe { Pin::new_unchecked(&self.waiter) }.unregister();
        self.state.with_flights(|flights| {
            let Some(flight) = flights.entries.get_mut(self.key) else {
                return;
            };
            let flight = unsafe { flight.get_unchecked_mut() };
            flight.waiters -= 1;
            if flight.waiters == 0 && flight.outcome.is_some() {
                flights.remove(self.key);
            }
        });
    }
}

enum FlightStart<'a, 'd> {
    Leader(FlightLeader<'a>),
    Follower(FlightWait<'a, 'd>),
    Untracked,
    Overloaded,
}

#[derive(Clone)]
struct Config {
    root: PathBuf,
    precompressed_br: bool,
    precompressed_gzip: bool,
    cache_capacity: usize,
    cache_entries: usize,
    flight_capacity: usize,
    cache_valid: Duration,
    max_file_bytes: usize,
    read_budget: usize,
}

struct SharedState {
    config: Config,
    hash_state: hash::State,
    read_budget: Pin<Rc<ByteBudget>>,
    cache: RawCell<Shard>,
    flights: RawCell<Flights>,
}

impl SharedState {
    fn new(config: Config, hash_state: hash::State) -> Self {
        let cache_entries = if config.cache_capacity == 0 {
            0
        } else {
            config.cache_entries
        };
        let flight_capacity = config.flight_capacity;
        Self {
            read_budget: Rc::pin(ByteBudget::new(config.read_budget)),
            config,
            hash_state,
            cache: RawCell::new(Shard::with_capacity(cache_entries)),
            flights: RawCell::new(Flights::with_capacity(flight_capacity)),
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        let mut hasher = self.hash_state.build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    fn finish_flight(&self, key: FlightKey, outcome: FlightOutcome) {
        self.with_flights(|flights| {
            let Some(waiters) = flights.entries.get(key).map(|flight| flight.waiters) else {
                return;
            };
            if waiters == 0 {
                flights.remove(key);
            } else {
                let flight = flights.entries.get_mut(key).expect("flight missing");
                let flight = unsafe { flight.get_unchecked_mut() };
                flight.outcome = Some(outcome);
                flight.wait_queue().wake();
            }
        });
    }

    fn with_cache<R>(&self, f: impl FnOnce(&mut Shard) -> R) -> R {
        unsafe { self.cache.with_mut(f) }
    }

    fn with_flights<R>(&self, f: impl FnOnce(&mut Flights) -> R) -> R {
        unsafe { self.flights.with_mut(f) }
    }
}

#[derive(Clone)]
pub struct ServeDir {
    state: Rc<SharedState>,
}

impl ServeDir {
    pub fn new(root: impl AsRef<Path>, hash_state: hash::State) -> Self {
        Self {
            state: Rc::new(SharedState::new(
                Config {
                    root: root.as_ref().to_path_buf(),
                    precompressed_br: false,
                    precompressed_gzip: false,
                    cache_capacity: DEFAULT_CACHE_CAPACITY,
                    cache_entries: DEFAULT_CACHE_ENTRIES,
                    flight_capacity: DEFAULT_FLIGHT_CAPACITY,
                    cache_valid: DEFAULT_CACHE_VALID,
                    max_file_bytes: DEFAULT_MAX_FILE_BYTES,
                    read_budget: DEFAULT_READ_BUDGET,
                },
                hash_state,
            )),
        }
    }

    fn with_cache<R>(&self, f: impl FnOnce(&mut Shard) -> R) -> R {
        self.state.with_cache(f)
    }

    fn reconfigure(mut self, update: impl FnOnce(&mut Config)) -> Self {
        let mut config = self.state.config.clone();
        update(&mut config);
        self.state = Rc::new(SharedState::new(config, self.state.hash_state));
        self
    }

    pub fn precompressed_br(self) -> Self {
        self.reconfigure(|config| config.precompressed_br = true)
    }

    pub fn precompressed_gzip(self) -> Self {
        self.reconfigure(|config| config.precompressed_gzip = true)
    }

    pub fn cache_capacity(self, bytes: usize) -> Self {
        self.reconfigure(|config| config.cache_capacity = bytes)
    }

    pub fn cache_entries(self, capacity: usize) -> Self {
        self.reconfigure(|config| config.cache_entries = capacity)
    }

    pub fn flight_capacity(self, capacity: usize) -> Self {
        self.reconfigure(|config| config.flight_capacity = capacity)
    }

    pub fn cache_valid(self, window: Duration) -> Self {
        self.reconfigure(|config| config.cache_valid = window)
    }

    pub fn max_file_bytes(self, bytes: usize) -> Self {
        self.reconfigure(|config| config.max_file_bytes = bytes)
    }

    pub fn read_budget(self, bytes: usize) -> Self {
        self.reconfigure(|config| config.read_budget = bytes)
    }

    fn cached(&self, key: &[u8], accept_encoding: &[u8]) -> AsyncCache {
        let now = Instant::now();
        let hash = self.state.hash(key);
        self.with_cache(|shard| {
            let Some(key) = shard.find(hash, key) else {
                return AsyncCache::Miss;
            };
            let entry = shard.entries.get(key).expect("cache key missing");
            if now.duration_since(entry.entry.validated) >= self.state.config.cache_valid {
                return AsyncCache::Stale;
            }
            shard.touch(key);
            let asset = &shard
                .entries
                .get(key)
                .expect("cache key missing")
                .entry
                .asset;
            AsyncCache::Hit(self.respond_asset(asset, accept_encoding))
        })
    }

    fn refresh_cached(&self, key: &[u8], mtime: Option<SystemTime>, size: u64) -> Option<Asset> {
        let hash = self.state.hash(key);
        self.with_cache(|shard| {
            let key = shard.find(hash, key)?;
            let matches = shard.entries.get(key).is_some_and(|slot| {
                slot.entry.asset.mtime == mtime && slot.entry.asset.size == size
            });
            if !matches {
                shard.remove(key);
                return None;
            }
            let entry = shard.entries.get_mut(key).expect("cache key missing");
            entry.entry.validated = Instant::now();
            let asset = entry.entry.asset.clone();
            shard.touch(key);
            Some(asset)
        })
    }

    fn remove_cached(&self, key: &[u8]) {
        let hash = self.state.hash(key);
        self.with_cache(|shard| {
            if let Some(key) = shard.find(hash, key) {
                shard.remove(key);
            }
        });
    }

    fn install(&self, key: &[u8], asset: Asset) {
        let capacity = self.state.config.cache_capacity;
        let hash = self.state.hash(key);
        self.with_cache(|cache| cache.insert(hash, key, asset, capacity));
    }

    fn begin_flight<'d>(&self, key: &[u8]) -> FlightStart<'_, 'd> {
        let hash = self.state.hash(key);
        self.state.with_flights(|flights| {
            if flights.index.is_none() {
                return FlightStart::Untracked;
            }
            if let Some(key) = flights.find(hash, key) {
                let flight = flights.entries.get_mut(key).expect("flight missing");
                let flight = unsafe { flight.get_unchecked_mut() };
                if flight.waiters == flight.waiter_capacity {
                    return FlightStart::Overloaded;
                }
                flight.waiters += 1;
                return FlightStart::Follower(FlightWait {
                    state: &self.state,
                    key,
                    waiter: Waiter::new(),
                    done: false,
                });
            }
            let Some(entry) = flights.entries.vacant_entry() else {
                return FlightStart::Overloaded;
            };
            let flight = Flight {
                hash,
                key: Box::from(key),
                waiters: 0,
                waiter_capacity: flights.waiter_capacity,
                outcome: None,
                wake: WaitQueue::with_capacity(flights.waiter_capacity),
            };
            let key = entry.insert(flight);
            if flights
                .index
                .as_mut()
                .expect("flight index missing")
                .try_insert(hash, FlightIndex { key }, |_| false)
                .is_err()
            {
                flights.entries.remove(key);
                return FlightStart::Overloaded;
            }
            FlightStart::Leader(FlightLeader {
                state: &self.state,
                key,
                done: false,
            })
        })
    }

    fn respond(body: Shared, mime: &'static str, encoding: Option<Encoding>) -> Response {
        let mut response = Response::ok();
        response.content_type(mime);
        if let Some(enc) = encoding {
            response.append_wire_header_static("content-encoding", enc.header());
            response.append_wire_header_static("vary", "accept-encoding");
        }
        response.set_body(body);
        response
    }

    fn respond_asset(&self, asset: &Asset, accept_encoding: &[u8]) -> Response {
        for encoding in self.negotiate_order(accept_encoding) {
            if let Some(variant) = asset.variants[encoding.index()].as_ref() {
                return Self::respond(variant.body.clone(), asset.mime, Some(encoding));
            }
        }
        Self::respond(asset.body.clone(), asset.mime, None)
    }

    fn precompressed(&self) -> impl Iterator<Item = Encoding> {
        [
            self.state.config.precompressed_br.then_some(Encoding::Br),
            self.state
                .config
                .precompressed_gzip
                .then_some(Encoding::Gzip),
        ]
        .into_iter()
        .flatten()
    }

    fn relative(rel: &[u8]) -> Option<&str> {
        if rel.is_empty() || rel.iter().any(|&byte| byte == 0 || byte == b'\\') {
            return None;
        }
        let rel = std::str::from_utf8(rel).ok()?;
        if rel.starts_with('/') {
            return None;
        }
        let mut found = false;
        for segment in rel.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return None;
            }
            found = true;
        }
        found.then_some(rel)
    }

    fn resolve(&self, rel: &str) -> Option<PathBuf> {
        let mut path = self.state.config.root.clone();
        for segment in rel.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            path.push(segment);
        }
        Some(path)
    }

    fn negotiate(&self, accept_encoding: &[u8]) -> [Option<Encoding>; 2] {
        let br = self
            .state
            .config
            .precompressed_br
            .then(|| Self::quality(accept_encoding, Encoding::Br.token()))
            .flatten();
        let gzip = self
            .state
            .config
            .precompressed_gzip
            .then(|| Self::quality(accept_encoding, Encoding::Gzip.token()))
            .flatten();

        match (br, gzip) {
            (Some(b), Some(g)) if g > b => [Some(Encoding::Gzip), Some(Encoding::Br)],
            (Some(_), Some(_)) => [Some(Encoding::Br), Some(Encoding::Gzip)],
            (Some(_), None) => [Some(Encoding::Br), None],
            (None, Some(_)) => [Some(Encoding::Gzip), None],
            (None, None) => [None, None],
        }
    }

    fn negotiate_order(&self, accept_encoding: &[u8]) -> impl Iterator<Item = Encoding> {
        self.negotiate(accept_encoding).into_iter().flatten()
    }

    fn sibling(base: &Path, encoding: Encoding) -> PathBuf {
        let mut name = base.as_os_str().to_owned();
        name.push(encoding.suffix());
        PathBuf::from(name)
    }

    fn quality(accept_encoding: &[u8], token: &[u8]) -> Option<u32> {
        let mut direct: Option<u32> = None;
        let mut star: Option<u32> = None;
        for entry in accept_encoding.split(|&b| b == b',') {
            let entry = entry.trim_ascii();
            let mut parts = entry.split(|&b| b == b';');
            let coding = parts.next().unwrap_or(b"").trim_ascii();
            let mut q = 1000u32;
            for param in parts {
                let param = param.trim_ascii();
                if param.len() >= 2 && param[0].eq_ignore_ascii_case(&b'q') && param[1] == b'=' {
                    q = Self::parse_q(&param[2..]);
                }
            }
            if coding.eq_ignore_ascii_case(token) {
                direct = Some(q);
            } else if coding == b"*" {
                star = Some(q);
            }
        }
        match direct.or(star)? {
            0 => None,
            q => Some(q),
        }
    }

    fn parse_q(value: &[u8]) -> u32 {
        let value = value.trim_ascii();
        let mut parts = value.splitn(2, |&b| b == b'.');
        let integer = parts.next().unwrap_or(b"");
        if integer == b"1" {
            return 1000;
        }
        let mut q = 0u32;
        let mut scale = 100u32;
        for &b in parts.next().unwrap_or(b"").iter().take(3) {
            if !b.is_ascii_digit() {
                break;
            }
            q += (b - b'0') as u32 * scale;
            scale /= 10;
        }
        q
    }

    fn content_type(base: &Path) -> &'static str {
        let Some(ext) = base.extension().and_then(|e| e.to_str()) else {
            return "application/octet-stream";
        };
        match ext.to_ascii_lowercase().as_str() {
            "css" => "text/css",
            "js" => "application/javascript",
            "html" => "text/html; charset=UTF-8",
            "json" => "application/json",
            "svg" => "image/svg+xml",
            "woff2" => "font/woff2",
            "woff" => "font/woff",
            "webp" => "image/webp",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "txt" => "text/plain",
            _ => "application/octet-stream",
        }
    }

    #[dope_fiber::fiber_fn('d)]
    pub async fn serve_async<'d, const ID: u8, const N: usize>(
        &self,
        files: &Files<'d, ID, N>,
        rel_path: &[u8],
        accept_encoding: &[u8],
    ) -> Response {
        let Some(rel) = Self::relative(rel_path) else {
            return Response::not_found();
        };
        let stale = match self.cached(rel_path, accept_encoding) {
            AsyncCache::Hit(response) => return response,
            AsyncCache::Stale => true,
            AsyncCache::Miss => false,
        };
        let leader = match self.begin_flight(rel_path) {
            FlightStart::Leader(leader) => Some(leader),
            FlightStart::Untracked => None,
            FlightStart::Overloaded => return LoadError::Overloaded.response(),
            FlightStart::Follower(wait) => {
                return match wait.await {
                    FlightOutcome::Loaded(asset) => self.respond_asset(&asset, accept_encoding),
                    FlightOutcome::Failed(error) => error.response(),
                };
            }
        };
        let Some(base) = self.resolve(rel) else {
            if let Some(leader) = leader {
                leader.finish(FlightOutcome::Failed(LoadError::NotFound));
            }
            return Response::not_found();
        };
        if stale {
            let Some(metadata) = self.stat_async(files, &base).await else {
                self.remove_cached(rel_path);
                if let Some(leader) = leader {
                    leader.finish(FlightOutcome::Failed(LoadError::NotFound));
                }
                return Response::not_found();
            };
            if let Some(asset) = self.refresh_cached(rel_path, metadata.modified(), metadata.len())
            {
                if let Some(leader) = leader {
                    leader.finish(FlightOutcome::Loaded(asset.clone()));
                }
                return self.respond_asset(&asset, accept_encoding);
            }
        }
        match self.load_async(files, base).await {
            Ok(asset) => {
                self.install(rel_path, asset.clone());
                if let Some(leader) = leader {
                    leader.finish(FlightOutcome::Loaded(asset.clone()));
                }
                self.respond_asset(&asset, accept_encoding)
            }
            Err(error) => {
                if let Some(leader) = leader {
                    leader.finish(FlightOutcome::Failed(error));
                }
                error.response()
            }
        }
    }

    #[dope_fiber::fiber_fn('d)]
    async fn load_async<'d, const ID: u8, const N: usize>(
        &self,
        files: &Files<'d, ID, N>,
        base: PathBuf,
    ) -> Result<Asset, LoadError> {
        let file = self.read_async(files, base.clone()).await?;
        let body = file.body;
        let mtime = file.metadata.modified();
        let size = file.metadata.len();
        let mime = Self::content_type(&base);
        let mut variants = [None, None];
        let mut bytes = body.len();
        for encoding in self.precompressed() {
            let path = Self::sibling(&base, encoding);
            match self.read_async(files, path).await {
                Ok(file) => {
                    bytes = bytes
                        .checked_add(file.body.len())
                        .ok_or(LoadError::Overloaded)?;
                    variants[encoding.index()] = Some(Variant { body: file.body });
                }
                Err(LoadError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Asset {
            body,
            mime,
            variants,
            mtime,
            size,
            bytes,
        })
    }

    fn read_async<'f, 'b, 'd, const ID: u8, const N: usize>(
        &'b self,
        files: &'f Files<'d, ID, N>,
        path: PathBuf,
    ) -> ReadFile<'f, 'b, 'd, ID, N> {
        ReadFile::new(
            files,
            path,
            self.state.read_budget.as_ref().handle(),
            self.state.config.max_file_bytes,
        )
    }

    #[dope_fiber::fiber_fn('d)]
    async fn stat_async<'d, const ID: u8, const N: usize>(
        &self,
        files: &Files<'d, ID, N>,
        path: &Path,
    ) -> Option<Metadata> {
        let path = path.to_str()?;
        let path = OpenPath::new(path).ok()?;
        let metadata = FileStat::path(files, path).await.ok()?;
        metadata.is_file().then_some(metadata)
    }
}
