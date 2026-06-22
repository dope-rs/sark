use std::io::Read;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use cartel_core::{FatalSlot, Slab};
use dope::WakerSet;
use dope::manifold::connector;
use dope::runtime::token::Token;
use o3::buffer::Owned;
use sark_core::http::Response;
use sark_core::http::codec::{DecodeMode, HeaderLookup, Parse};

use crate::connector::codec::{self, Head};
use crate::connector::error::Error;
use crate::connector::retry::RetryPolicy;

pub(super) type Outcome = Result<Response, Error>;

pub(super) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

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

struct ConnEntry {
    conn_id: Token,
    slab_ix: usize,
    last_activity: Instant,
    keepalive: Option<Duration>,
}

pub struct Shared {
    conns: Vec<ConnEntry>,
    #[allow(clippy::vec_box)]
    slabs: Vec<Box<Slab<Outcome>>>,
    pub active_wakers: WakerSet,
    pub fatal: FatalSlot<Error>,
    pub host: String,
    pub decompression: DecompressionPolicy,
    pub max_redirects: u32,
    pub retry: RetryPolicy,
    pub idle_timeout: Duration,
    pub request_timeout: Duration,
}

impl Shared {
    fn new(host: String) -> Self {
        Self {
            conns: Vec::new(),
            slabs: Vec::new(),
            active_wakers: WakerSet::new(),
            fatal: FatalSlot::default(),
            host,
            decompression: DecompressionPolicy::Strict,
            max_redirects: 10,
            retry: RetryPolicy::default(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    pub fn any_ready(&self) -> bool {
        !self.conns.is_empty()
    }

    pub fn live_conns(&self) -> usize {
        self.conns.len()
    }

    fn alloc_slab(&mut self) -> usize {
        for ix in 0..self.slabs.len() {
            let bound = self.conns.iter().any(|c| c.slab_ix == ix);
            if !bound && self.slabs[ix].is_drained() {
                return ix;
            }
        }
        self.slabs.push(Box::new(Slab::new()));
        self.slabs.len() - 1
    }

    fn note_connect(&mut self, conn_id: Token, now: Instant) {
        if self.conns.iter().any(|c| c.conn_id == conn_id) {
            return;
        }
        let slab_ix = self.alloc_slab();
        self.conns.push(ConnEntry {
            conn_id,
            slab_ix,
            last_activity: now,
            keepalive: None,
        });
        self.active_wakers.drain_wake();
    }

    fn push_response(
        &mut self,
        conn_id: Token,
        outcome: Outcome,
        keepalive: Option<Duration>,
        now: Instant,
    ) {
        let Some(pos) = self.conns.iter().position(|c| c.conn_id == conn_id) else {
            return;
        };
        let slab_ix = {
            let c = &mut self.conns[pos];
            c.last_activity = now;
            if keepalive.is_some() {
                c.keepalive = keepalive;
            }
            c.slab_ix
        };
        self.slabs[slab_ix].push(outcome);
        self.slabs[slab_ix].complete();
        self.active_wakers.drain_wake();
    }

    fn fail_connection(&mut self, conn_id: Token, fatal: Option<String>) {
        if let Some(pos) = self.conns.iter().position(|c| c.conn_id == conn_id) {
            let slab_ix = self.conns[pos].slab_ix;
            self.slabs[slab_ix].fail_all(|| match &fatal {
                Some(m) => Err(Error::Http(m.clone())),
                None => Err(Error::Closed),
            });
            self.conns.remove(pos);
        }
        self.active_wakers.drain_wake();
    }

    pub fn drop_conn(&mut self, conn_id: Token) {
        if let Some(pos) = self.conns.iter().position(|c| c.conn_id == conn_id) {
            let slab_ix = self.conns[pos].slab_ix;
            self.slabs[slab_ix].fail_all(|| Err(Error::Closed));
            self.conns.remove(pos);
        }
    }

    pub fn acquire(&mut self, now: Instant, idle_timeout: Duration) -> (Option<Token>, Vec<Token>) {
        let mut chosen: Option<usize> = None;
        let mut best_depth = usize::MAX;
        let mut recycle_idx: Vec<usize> = Vec::new();
        for (i, c) in self.conns.iter().enumerate() {
            let limit = c
                .keepalive
                .map(|k| k.saturating_sub(KEEPALIVE_MARGIN))
                .unwrap_or(idle_timeout);
            let stale = now.saturating_duration_since(c.last_activity) >= limit;
            let depth = self.slabs[c.slab_ix].depth();
            if stale {
                recycle_idx.push(i);
                continue;
            }
            if depth < best_depth {
                best_depth = depth;
                chosen = Some(i);
            }
        }
        let chosen_tok = chosen.map(|i| self.conns[i].conn_id);
        let mut recycle = Vec::with_capacity(recycle_idx.len());
        recycle_idx.sort_unstable();
        for &i in recycle_idx.iter().rev() {
            recycle.push(self.conns[i].conn_id);
            self.conns.remove(i);
        }
        (chosen_tok, recycle)
    }

    pub fn slab_ptr_for(&mut self, conn_id: Token) -> Option<NonNull<Slab<Outcome>>> {
        let pos = self.conns.iter().position(|c| c.conn_id == conn_id)?;
        let slab_ix = self.conns[pos].slab_ix;
        Some(NonNull::from(&mut *self.slabs[slab_ix]))
    }

    pub fn touch(&mut self, conn_id: Token, now: Instant) {
        if let Some(c) = self.conns.iter_mut().find(|c| c.conn_id == conn_id) {
            c.last_activity = now;
        }
    }
}

pub struct Session {
    codec: codec::Codec,
    pub shared: Shared,
}

impl Session {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            codec: codec::Codec::default(),
            shared: Shared::new(host.into()),
        }
    }

    pub fn with_decompression(host: impl Into<String>, policy: DecompressionPolicy) -> Self {
        let mut session = Self::new(host);
        session.shared.decompression = policy;
        session
    }

    pub fn max_response_body(&mut self, cap: usize) -> &mut Self {
        self.codec.max_response_body = cap;
        self
    }

    pub fn with_max_redirects(mut self, max: u32) -> Self {
        self.shared.max_redirects = max;
        self
    }

    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.shared.retry = retry;
        self
    }

    pub fn with_idle_timeout(mut self, idle: Duration) -> Self {
        self.shared.idle_timeout = idle;
        self
    }

    pub fn with_request_timeout(mut self, dur: Duration) -> Self {
        self.shared.request_timeout = dur;
        self
    }
}

impl connector::Session for Session {
    type Codec = codec::Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &codec::Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, Self>) {
        self.shared.fatal.clear();
        self.shared.note_connect(ctx.conn_id, Instant::now());
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, Self>) {
        if let Some(reason) = head.error {
            self.shared.push_response(
                ctx.conn_id,
                Err(Error::Parse(reason.into())),
                None,
                Instant::now(),
            );
            ctx.state.pending_close = true;
            return;
        }
        let bytes = head.full.as_ref();
        let (outcome, keep_alive, keepalive_timeout) =
            match Parse::response(bytes, DecodeMode::Response) {
                Ok(Some(mut resp)) => {
                    let keep = Self::should_keep_alive(&resp);
                    let timeout = Self::keepalive_timeout(&resp);
                    let outcome = match Self::decompress(
                        &mut resp,
                        self.shared.decompression,
                        self.codec.max_response_body,
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
        self.shared
            .push_response(ctx.conn_id, outcome, keepalive_timeout, Instant::now());
    }

    fn disconnect(&mut self, ctx: &mut connector::Ctx<'_, Self>) {
        let fatal_msg = self.shared.fatal.as_ref().map(|e| e.to_string());
        self.shared.fail_connection(ctx.conn_id, fatal_msg);
        ctx.state.pending_close = false;
    }
}

impl Session {
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
                resp.set_body(Owned::from(&decompressed[..]));
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
