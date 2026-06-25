//! Static file handler with a size-bounded, thread-local cache. `serve` reads
//! synchronously; `serve_async` reads via the dope file manifold. With
//! `precompressed_br`/`precompressed_gzip`, sibling `.br`/`.gz` files are served
//! per `Accept-Encoding`. `cache_capacity(0)` disables caching.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use dope::Driver;
use dope::fiber::Holding;
use dope::fiber::file::Source;
use dope::file::{O_CLOEXEC, O_RDONLY, OpenPath};
use dope::manifold::file::Files;
use o3::buffer::{Owned, Shared};
use sark_core::http::Response;

const READ_CHUNK: usize = 256 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Br,
    Gzip,
}

impl Encoding {
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
const DEFAULT_CACHE_VALID: Duration = Duration::from_secs(2);

struct Variant {
    body: Shared,
    encoding: Encoding,
}

struct Entry {
    body: Shared,
    mime: &'static str,
    variants: Vec<Variant>,
    mtime: Option<SystemTime>,
    size: u64,
    bytes: usize,
    stamp: u64,
    validated: Instant,
}

impl Entry {
    fn footprint(&self) -> usize {
        self.bytes
    }
}

struct Shard {
    entries: HashMap<PathBuf, Entry>,
    total: usize,
    clock: u64,
}

impl Shard {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total: 0,
            clock: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn evict_until_fits(&mut self, capacity: usize, incoming: usize) {
        if incoming > capacity {
            self.entries.clear();
            self.total = 0;
            return;
        }
        while self.total + incoming > capacity {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.stamp)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = self.entries.remove(&victim) {
                self.total -= e.footprint();
            }
        }
    }

    fn insert(&mut self, key: PathBuf, mut entry: Entry, capacity: usize) {
        if let Some(old) = self.entries.remove(&key) {
            self.total -= old.footprint();
        }
        let incoming = entry.footprint();
        self.evict_until_fits(capacity, incoming);
        if self.total + incoming > capacity {
            return;
        }
        entry.stamp = self.tick();
        self.total += incoming;
        self.entries.insert(key, entry);
    }
}

thread_local! {
    static CACHE: RefCell<Shard> = RefCell::new(Shard::new());
}

#[derive(Clone)]
pub struct ServeDir {
    root: PathBuf,
    precompressed_br: bool,
    precompressed_gzip: bool,
    cache_capacity: usize,
    cache_valid: Duration,
}

impl ServeDir {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            precompressed_br: false,
            precompressed_gzip: false,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            cache_valid: DEFAULT_CACHE_VALID,
        }
    }

    pub fn precompressed_br(mut self) -> Self {
        self.precompressed_br = true;
        self
    }

    pub fn precompressed_gzip(mut self) -> Self {
        self.precompressed_gzip = true;
        self
    }

    pub fn cache_capacity(mut self, bytes: usize) -> Self {
        self.cache_capacity = bytes;
        self
    }

    pub fn cache_valid(mut self, window: Duration) -> Self {
        self.cache_valid = window;
        self
    }

    pub fn serve(&self, rel_path: &[u8], accept_encoding: &[u8]) -> Response {
        let Some(base) = self.resolve(rel_path) else {
            return Response::not_found();
        };

        if self.cache_capacity > 0 {
            return self.serve_cached(base, accept_encoding);
        }
        self.serve_uncached(&base, accept_encoding)
    }

    fn try_cached(&self, base: &Path, accept_encoding: &[u8]) -> Option<Response> {
        let now = Instant::now();
        CACHE.with(|cell| {
            let mut shard = cell.borrow_mut();
            let needs_revalidate = match shard.entries.get(base) {
                Some(e) => now.duration_since(e.validated) >= self.cache_valid,
                None => return None,
            };
            if needs_revalidate {
                match Self::stat(base) {
                    Some((mtime, size)) => {
                        let entry = shard.entries.get_mut(base).expect("checked present");
                        if entry.mtime == mtime && entry.size == size {
                            entry.validated = now;
                        } else {
                            return None;
                        }
                    }
                    None => {
                        if let Some(e) = shard.entries.remove(base) {
                            shard.total -= e.footprint();
                        }
                        return Some(Response::not_found());
                    }
                }
            }
            let stamp = shard.tick();
            let entry = shard.entries.get_mut(base).expect("checked present");
            entry.stamp = stamp;
            Some(self.respond_entry(entry, accept_encoding))
        })
    }

    fn install(&self, base: PathBuf, entry: Entry, accept_encoding: &[u8]) -> Response {
        let response = self.respond_entry(&entry, accept_encoding);
        CACHE.with(|cell| {
            cell.borrow_mut().insert(base, entry, self.cache_capacity);
        });
        response
    }

    fn serve_cached(&self, base: PathBuf, accept_encoding: &[u8]) -> Response {
        if let Some(response) = self.try_cached(&base, accept_encoding) {
            return response;
        }
        let Some(entry) = self.load(&base) else {
            return Response::not_found();
        };
        self.install(base, entry, accept_encoding)
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

    fn respond_entry(&self, entry: &Entry, accept_encoding: &[u8]) -> Response {
        for encoding in self.negotiate_order(accept_encoding) {
            if let Some(v) = entry.variants.iter().find(|v| v.encoding == encoding) {
                return Self::respond(v.body.clone(), entry.mime, Some(encoding));
            }
        }
        Self::respond(entry.body.clone(), entry.mime, None)
    }

    fn precompressed(&self) -> impl Iterator<Item = Encoding> {
        [
            self.precompressed_br.then_some(Encoding::Br),
            self.precompressed_gzip.then_some(Encoding::Gzip),
        ]
        .into_iter()
        .flatten()
    }

    fn load(&self, base: &Path) -> Option<Entry> {
        let (body, mtime, size) = Self::read_with_meta(base)?;
        let mime = Self::content_type(base);
        let mut variants = Vec::new();
        let mut bytes = body.len();
        for encoding in self.precompressed() {
            if let Some((body, _, _)) = Self::read_with_meta(&Self::sibling(base, encoding)) {
                bytes += body.len();
                variants.push(Variant { body, encoding });
            }
        }
        Some(Entry {
            body,
            mime,
            variants,
            mtime,
            size,
            bytes,
            stamp: 0,
            validated: Instant::now(),
        })
    }

    fn serve_uncached(&self, base: &Path, accept_encoding: &[u8]) -> Response {
        let mime = Self::content_type(base);
        for encoding in self.negotiate_order(accept_encoding) {
            if let Some((body, _, _)) = Self::read_with_meta(&Self::sibling(base, encoding)) {
                return Self::respond(body, mime, Some(encoding));
            }
        }
        match Self::read_with_meta(base) {
            Some((body, _, _)) => Self::respond(body, mime, None),
            None => Response::not_found(),
        }
    }

    fn resolve(&self, rel: &[u8]) -> Option<PathBuf> {
        if rel.is_empty() || rel.iter().any(|&b| b == 0 || b == b'\\') {
            return None;
        }
        let rel = std::str::from_utf8(rel).ok()?;
        if rel.starts_with('/') {
            return None;
        }
        let mut path = self.root.clone();
        for segment in rel.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return None;
            }
            path.push(segment);
        }
        if path == self.root {
            return None;
        }
        Some(path)
    }

    fn negotiate_order(&self, accept_encoding: &[u8]) -> impl Iterator<Item = Encoding> {
        let br = self
            .precompressed_br
            .then(|| Self::quality(accept_encoding, Encoding::Br.token()))
            .flatten();
        let gzip = self
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
        .into_iter()
        .flatten()
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

    fn stat(path: &Path) -> Option<(Option<SystemTime>, u64)> {
        let meta = std::fs::metadata(path).ok()?;
        if !meta.is_file() {
            return None;
        }
        Some((meta.modified().ok(), meta.len()))
    }

    fn read_with_meta(path: &Path) -> Option<(Shared, Option<SystemTime>, u64)> {
        let mut file = File::open(path).ok()?;
        let meta = file.metadata().ok()?;
        if !meta.is_file() {
            return None;
        }
        let len = meta.len() as usize;
        let mut body = Owned::with_capacity(len);
        let spare = body.spare_capacity_mut();
        // SAFETY: spare covers len reserved bytes; read_exact fills them before set_len.
        let slot = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), len) };
        file.read_exact(slot).ok()?;
        // SAFETY: read_exact filled exactly len bytes, so the prefix is initialized.
        unsafe { body.set_len(len) };
        Some((body.freeze(), meta.modified().ok(), meta.len()))
    }

    pub async fn serve_async<const ID: u8, const N: usize>(
        &self,
        files: Holding<'_, Files<ID, N>>,
        driver: &mut Driver,
        rel_path: &[u8],
        accept_encoding: &[u8],
    ) -> Response {
        let Some(base) = self.resolve(rel_path) else {
            return Response::not_found();
        };

        if self.cache_capacity == 0 {
            return self
                .serve_uncached_async(files, driver, &base, accept_encoding)
                .await;
        }

        if let Some(response) = self.try_cached(&base, accept_encoding) {
            return response;
        }

        let Some(entry) = self.load_async(files, driver, &base).await else {
            return Response::not_found();
        };
        self.install(base, entry, accept_encoding)
    }

    async fn serve_uncached_async<const ID: u8, const N: usize>(
        &self,
        files: Holding<'_, Files<ID, N>>,
        driver: &mut Driver,
        base: &Path,
        accept_encoding: &[u8],
    ) -> Response {
        let mime = Self::content_type(base);
        for encoding in self.negotiate_order(accept_encoding) {
            let path = Self::sibling(base, encoding);
            if let Some(body) = Self::read_async(files, driver, &path).await {
                return Self::respond(body, mime, Some(encoding));
            }
        }
        match Self::read_async(files, driver, base).await {
            Some(body) => Self::respond(body, mime, None),
            None => Response::not_found(),
        }
    }

    async fn load_async<const ID: u8, const N: usize>(
        &self,
        files: Holding<'_, Files<ID, N>>,
        driver: &mut Driver,
        base: &Path,
    ) -> Option<Entry> {
        let body = Self::read_async(files, driver, base).await?;
        let (mtime, size) = Self::stat(base).unwrap_or((None, body.len() as u64));
        let mime = Self::content_type(base);
        let mut variants = Vec::new();
        let mut bytes = body.len();
        for encoding in self.precompressed() {
            let path = Self::sibling(base, encoding);
            if let Some(body) = Self::read_async(files, driver, &path).await {
                bytes += body.len();
                variants.push(Variant { body, encoding });
            }
        }
        Some(Entry {
            body,
            mime,
            variants,
            mtime,
            size,
            bytes,
            stamp: 0,
            validated: Instant::now(),
        })
    }

    async fn read_async<const ID: u8, const N: usize>(
        files: Holding<'_, Files<ID, N>>,
        driver: &mut Driver,
        path: &Path,
    ) -> Option<Shared> {
        let cpath = OpenPath::new(path.to_str()?).ok()?;
        let src = Files::open_held(files, driver, &cpath, O_RDONLY | O_CLOEXEC)
            .await
            .ok()?;
        let body = Self::read_to_end(files, driver, &src).await;
        Self::close_source(files, driver, src);
        body
    }

    async fn read_to_end<const ID: u8, const N: usize>(
        files: Holding<'_, Files<ID, N>>,
        driver: &mut Driver,
        src: &Source,
    ) -> Option<Shared> {
        let mut body = Owned::with_capacity(READ_CHUNK);
        let mut buf = vec![0u8; READ_CHUNK];
        let mut offset = 0u64;
        loop {
            let (returned, res) =
                Files::read_held(files, driver, Self::source_dup(src), buf, offset).await;
            buf = returned;
            let n = res.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&buf[..n]);
            offset += n as u64;
        }
        Some(body.freeze())
    }

    fn source_dup(src: &Source) -> Source {
        match *src {
            Source::Fd(fd) => Source::Fd(fd),
            Source::Fixed(slot) => Source::Fixed(slot),
        }
    }

    fn close_source<const ID: u8, const N: usize>(
        _files: Holding<'_, Files<ID, N>>,
        _driver: &mut Driver,
        src: Source,
    ) {
        if let Source::Fd(fd) = src {
            // SAFETY: fd was returned by an io_uring open we own and is not registered/fixed;
            // OwnedFd takes sole ownership and closes it on drop.
            drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) });
        }
    }
}
