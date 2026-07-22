//! Asynchronous static-file serving with bounded caching and single-flight loading.

mod cache;
mod encoding;
mod flight;
mod loader;
mod resolver;

use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use cache::{Asset, Cache, Lookup, Variant};
use dope::hash;
use dope::io::file::OpenPath;
use dope::manifold::file::Files;
use dope_fiber::file;
use encoding::Encoding;
use loader::{LoadError, ReadFile};
use o3::buffer::Shared;
use o3::mem::ByteBudget;
use sark_core::http::Response;

const DEFAULT_MAX_FILE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_READ_BUDGET: usize = 256 * 1024 * 1024;
const DEFAULT_CACHE_CAPACITY: usize = 256 * 1024 * 1024;
const DEFAULT_CACHE_ENTRIES: usize = 4096;
const DEFAULT_FLIGHT_CAPACITY: usize = 256;
const DEFAULT_CACHE_VALID: Duration = Duration::from_secs(2);

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
    cache: Cache,
    flights: flight::Hub,
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
            cache: Cache::new(cache_entries),
            flights: flight::Hub::new(flight_capacity),
            config,
            hash_state,
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        let mut hasher = self.hash_state.build_hasher();
        hasher.write(key);
        hasher.finish()
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

    fn cached(&self, key: &[u8]) -> Lookup {
        self.state
            .cache
            .lookup(self.state.hash(key), key, self.state.config.cache_valid)
    }

    fn refresh_cached(
        &self,
        key: &[u8],
        mtime: Option<std::time::SystemTime>,
        size: u64,
    ) -> Option<Asset> {
        self.state
            .cache
            .refresh(self.state.hash(key), key, mtime, size)
    }

    fn remove_cached(&self, key: &[u8]) {
        self.state.cache.remove(self.state.hash(key), key);
    }

    fn install(&self, key: &[u8], asset: Asset) {
        self.state.cache.insert(
            self.state.hash(key),
            key,
            asset,
            self.state.config.cache_capacity,
        );
    }

    fn begin_flight<'d>(&self, key: &[u8]) -> flight::Start<'_, 'd> {
        self.state.flights.begin(self.state.hash(key), key)
    }

    fn respond(body: Shared, mime: &'static str, encoding: Option<Encoding>) -> Response {
        let mut response = Response::ok();
        response.content_type(mime);
        if let Some(encoding) = encoding {
            response.append_wire_header_static("content-encoding", encoding.header());
            response.append_wire_header_static("vary", "accept-encoding");
        }
        response.set_body(body);
        response
    }

    fn respond_asset(&self, asset: &Asset, accept_encoding: &[u8]) -> Response {
        for encoding in Encoding::negotiate(
            accept_encoding,
            self.state.config.precompressed_br,
            self.state.config.precompressed_gzip,
        ) {
            if let Some(variant) = asset.variant(encoding) {
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

    #[dope_fiber::fiber_fn('d)]
    pub async fn serve_async<'d, const ID: u8, const N: usize>(
        &self,
        files: &Files<'d, ID, N>,
        relative_path: &[u8],
        accept_encoding: &[u8],
    ) -> Response {
        let Some(relative) = resolver::Resolver::relative(relative_path) else {
            return Response::not_found();
        };
        let stale = match self.cached(relative_path) {
            Lookup::Hit(asset) => return self.respond_asset(&asset, accept_encoding),
            Lookup::Stale => true,
            Lookup::Miss => false,
        };
        let leader = match self.begin_flight(relative_path) {
            flight::Start::Leader(leader) => Some(leader),
            flight::Start::Untracked => None,
            flight::Start::Overloaded => return LoadError::Overloaded.response(),
            flight::Start::Follower(wait) => {
                return match wait.await {
                    flight::Outcome::Loaded(asset) => self.respond_asset(&asset, accept_encoding),
                    flight::Outcome::Failed(error) => error.response(),
                };
            }
        };
        let base = resolver::Resolver::new(&self.state.config.root).resolve(relative);
        if stale {
            let Some(metadata) = self.stat_async(files, &base).await else {
                self.remove_cached(relative_path);
                if let Some(leader) = leader {
                    leader.finish(flight::Outcome::Failed(LoadError::NotFound));
                }
                return Response::not_found();
            };
            if let Some(asset) =
                self.refresh_cached(relative_path, metadata.modified(), metadata.len())
            {
                if let Some(leader) = leader {
                    leader.finish(flight::Outcome::Loaded(asset.clone()));
                }
                return self.respond_asset(&asset, accept_encoding);
            }
        }
        match self.load_async(files, base).await {
            Ok(asset) => {
                self.install(relative_path, asset.clone());
                if let Some(leader) = leader {
                    leader.finish(flight::Outcome::Loaded(asset.clone()));
                }
                self.respond_asset(&asset, accept_encoding)
            }
            Err(error) => {
                if let Some(leader) = leader {
                    leader.finish(flight::Outcome::Failed(error));
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
        let mime = resolver::Resolver::content_type(&base);
        let mut variants = [None, None];
        let mut bytes = body.len();
        for encoding in self.precompressed() {
            match self
                .read_async(files, resolver::Resolver::sibling(&base, encoding))
                .await
            {
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
        Ok(Asset::new(body, mime, variants, mtime, size, bytes))
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
    ) -> Option<file::Metadata> {
        let path = path.to_str()?;
        let path = OpenPath::new(path).ok()?;
        let metadata = file::Stat::path(files, path).await.ok()?;
        metadata.is_file().then_some(metadata)
    }
}
