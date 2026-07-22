use std::time::{Duration, Instant, SystemTime};

use o3::buffer::Shared;
use o3::cell::RawCell;
use o3::collections::{FixedHashTable, Slab, SlabKey};

use super::encoding::Encoding;

#[derive(Clone)]
pub(super) struct Variant {
    pub(super) body: Shared,
}

#[derive(Clone)]
pub(super) struct Asset {
    pub(super) body: Shared,
    pub(super) mime: &'static str,
    pub(super) variants: [Option<Variant>; 2],
    pub(super) mtime: Option<SystemTime>,
    pub(super) size: u64,
    bytes: usize,
}

impl Asset {
    pub(super) fn new(
        body: Shared,
        mime: &'static str,
        variants: [Option<Variant>; 2],
        mtime: Option<SystemTime>,
        size: u64,
        bytes: usize,
    ) -> Self {
        Self {
            body,
            mime,
            variants,
            mtime,
            size,
            bytes,
        }
    }

    fn footprint(&self) -> usize {
        self.bytes
    }

    pub(super) fn variant(&self, encoding: Encoding) -> Option<&Variant> {
        self.variants[encoding.index()].as_ref()
    }
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
        if self.tail != Some(key) {
            self.unlink(key);
            self.link_back(key);
        }
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
        let slot = CacheSlot {
            hash,
            key: Box::from(key),
            entry: CacheEntry {
                asset,
                validated: Instant::now(),
            },
            prev: None,
            next: None,
        };
        let Ok(entry_key) = self.entries.insert(slot) else {
            return;
        };
        if self
            .index
            .as_mut()
            .expect("cache index missing")
            .try_insert(hash, CacheIndex { key: entry_key }, |_| false)
            .is_err()
        {
            self.entries.remove(entry_key);
            return;
        }
        self.link_back(entry_key);
        self.total += incoming;
    }
}

pub(super) enum Lookup {
    Hit(Asset),
    Stale,
    Miss,
}

pub(super) struct Cache {
    shard: RawCell<Shard>,
}

impl Cache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            shard: RawCell::new(Shard::with_capacity(capacity)),
        }
    }

    pub(super) fn lookup(&self, hash: u64, key: &[u8], valid_for: Duration) -> Lookup {
        self.with_shard(|shard| {
            let Some(entry_key) = shard.find(hash, key) else {
                return Lookup::Miss;
            };
            let entry = shard.entries.get(entry_key).expect("cache key missing");
            if entry.entry.validated.elapsed() >= valid_for {
                return Lookup::Stale;
            }
            let asset = entry.entry.asset.clone();
            shard.touch(entry_key);
            Lookup::Hit(asset)
        })
    }

    pub(super) fn refresh(
        &self,
        hash: u64,
        key: &[u8],
        mtime: Option<SystemTime>,
        size: u64,
    ) -> Option<Asset> {
        self.with_shard(|shard| {
            let entry_key = shard.find(hash, key)?;
            let matches = shard.entries.get(entry_key).is_some_and(|slot| {
                slot.entry.asset.mtime == mtime && slot.entry.asset.size == size
            });
            if !matches {
                shard.remove(entry_key);
                return None;
            }
            let entry = shard.entries.get_mut(entry_key).expect("cache key missing");
            entry.entry.validated = Instant::now();
            let asset = entry.entry.asset.clone();
            shard.touch(entry_key);
            Some(asset)
        })
    }

    pub(super) fn remove(&self, hash: u64, key: &[u8]) {
        self.with_shard(|shard| {
            if let Some(entry_key) = shard.find(hash, key) {
                shard.remove(entry_key);
            }
        });
    }

    pub(super) fn insert(&self, hash: u64, key: &[u8], asset: Asset, capacity: usize) {
        self.with_shard(|shard| shard.insert(hash, key, asset, capacity));
    }

    fn with_shard<R>(&self, operation: impl FnOnce(&mut Shard) -> R) -> R {
        // SAFETY: ServeDir and its state are thread-local (`Rc`). Every access to
        // the cache is completed synchronously and this method never re-enters.
        unsafe { self.shard.with_mut(operation) }
    }
}
