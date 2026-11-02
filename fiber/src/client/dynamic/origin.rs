use std::{cell, marker, mem, net, num::NonZero};

use dope::core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake::{self, queue},
};
use o3::{
    alloc::{self, BoxSliceExt as _, StrExt as _, array},
    depot::queue::{
        indexed::lanes::runtime::shared,
        resident::{Fixed, fifo},
    },
};
use sark_client::{Error, OriginError, connect};
use sark_http::uri::{self, target};

use crate::client::{
    dynamic,
    exchange::net::{PoolTag, Tagged, waiters::Waiters},
};

const MAX_AUTHORITY_LEN: usize = 1024;

struct Registered<'r, 'q, 'd, ID: Identity> {
    lane: shared::Head<'q, 'd, (), NonZero<u32>, NonZero<u32>>,
    hash: u32,
    wake: fifo::Index<'q>,
    next: Option<Key<'r, 'q, 'd, ID>>,
    origin: Origin,
    /// Admitted before publication and reused by every connection attempt.
    address: net::SocketAddr,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(hidden)]
pub struct Key<'r, 'q, 'd, ID: Identity> {
    entry: &'r Registered<'r, 'q, 'd, ID>,
    driver: marker::PhantomData<fn(&'q ID) -> &'q ID>,
}

impl<'r, 'q, 'd, ID: Identity> Key<'r, 'q, 'd, ID> {
    pub(in crate::client) fn lane(
        self,
    ) -> &'r shared::Head<'q, 'd, (), NonZero<u32>, NonZero<u32>> {
        &self.entry.lane
    }
    pub(super) fn address(self) -> net::SocketAddr {
        self.entry.address
    }
}

impl<'r, 'q, 'd, ID: Identity> PoolTag<'d, ID> for Key<'r, 'q, 'd, ID> {
    type Ready = &'q Tagged<'d, ID>;
    type Lanes = fifo::Scope<'q>;
    type Connections = [queue::Queue<'d, Fixed<1>>; 0];

    fn wake(self, queue: &queue::Queue<'d, Self::Lanes>) {
        queue.wake_one(self.entry.wake);
    }

    fn register<'target>(
        self,
        queue: &'target queue::Queue<'d, Self::Lanes>,
        entry: &mut queue::Registration<'target, 'd, Self::Lanes>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), queue::Error> {
        queue
            .try_register(self.entry.wake, entry, wake)
            .map_err(Into::into)
    }
}

const _: () = {
    assert!(usize::BITS >= u32::BITS);
    assert!(MAX_AUTHORITY_LEN <= u16::MAX as usize);
    assert!(
        mem::size_of::<Key<'static, 'static, 'static, KeyTag<0>>>()
            == mem::size_of::<&Registered<'static, 'static, 'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Option<Key<'static, 'static, 'static, KeyTag<0>>>>()
            == mem::size_of::<Key<'static, 'static, 'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Registered<'static, 'static, 'static, KeyTag<0>>>()
            == mem::size_of::<Origin>()
                + 4 * mem::size_of::<usize>()
                + mem::size_of::<net::SocketAddr>()
    );
    assert!(
        mem::size_of::<cell::OnceCell<Registered<'static, 'static, 'static, KeyTag<0>>>>()
            == mem::size_of::<Registered<'static, 'static, 'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<
            dope::manifold::app::dispatch::identity::Binding<
                dope::manifold::client::conn::Id<'static, KeyTag<0>>,
                Key<'static, 'static, 'static, KeyTag<0>>,
            >,
        >() == mem::size_of::<
            dope::manifold::app::dispatch::identity::Binding<
                dope::manifold::client::conn::Id<'static, KeyTag<0>>,
            >,
        >() + mem::size_of::<Key<'static, 'static, 'static, KeyTag<0>>>()
    );
};

/// Owned authority retaining its admitted host boundary through ASCII normalization.
pub struct Origin {
    authority: Box<str>,
    host_len: u16,
    port: u16,
}

struct Parsed<'a> {
    authority: &'a str,
    host: &'a str,
    host_len: u16,
    port: u16,
}

impl Parsed<'_> {
    fn try_own(self) -> Result<Origin, alloc::Exhausted> {
        let mut value = self.authority.try_to_owned()?;
        value.make_ascii_lowercase();
        Ok(Origin {
            authority: value.into_boxed_str(),
            host_len: self.host_len,
            port: self.port,
        })
    }
}

impl PartialEq for Origin {
    fn eq(&self, other: &Self) -> bool {
        self.host() == other.host() && self.port == other.port
    }
}

impl Eq for Origin {}

impl Origin {
    pub fn from_uri(uri: &target::Target) -> Result<Self, OriginError> {
        Self::parse(uri)?.try_own().map_err(OriginError::Allocation)
    }

    fn parse(uri: &target::Target) -> Result<Parsed<'_>, OriginError> {
        match uri.scheme_str() {
            Some(scheme) if scheme.eq_ignore_ascii_case("http") => {}
            Some(scheme) if scheme.eq_ignore_ascii_case("https") => {
                return Err(OriginError::HttpsRequiresTls);
            }
            Some(_) => {
                return Err(OriginError::UnsupportedScheme);
            }
            None => {
                return Err(OriginError::MissingScheme);
            }
        }
        let auth = uri.authority();
        if auth.len() > MAX_AUTHORITY_LEN {
            return Err(OriginError::AuthorityTooLong);
        }
        let host = uri.host_raw();
        let host_len = host.len() as u16;
        let port = uri
            .parts()
            .try_port_u16()
            .map_err(OriginError::Port)?
            .unwrap_or(80);
        Ok(Parsed {
            authority: auth,
            host,
            host_len,
            port,
        })
    }

    pub fn authority(&self) -> &str {
        &self.authority
    }

    pub fn host(&self) -> &str {
        // SAFETY: parse retains Target::host_raw's prefix boundary in this exact
        // authority. Ownership copies and ASCII normalization preserve its UTF-8
        // boundaries and length; neither field is subsequently mutable.
        unsafe { dynamic::raw::prefix(&self.authority, usize::from(self.host_len)) }
    }

    fn address(&self) -> Result<net::SocketAddr, uri::Host> {
        let ip = uri::Authority::parse_ip(self.host().as_bytes())?;
        Ok(net::SocketAddr::new(ip, self.port))
    }

    pub const fn port(&self) -> u16 {
        self.port
    }
}

enum Probe<'a, 'r, 'q, 'd, ID: Identity> {
    Found(Key<'r, 'q, 'd, ID>),
    Missing(&'a cell::Cell<Option<Key<'r, 'q, 'd, ID>>>),
}

pub(in crate::client) struct Registry<'r, 'q, 'd, ID: Identity> {
    entries: &'r [cell::OnceCell<Registered<'r, 'q, 'd, ID>>],
    buckets: array::Block<cell::Cell<Option<Key<'r, 'q, 'd, ID>>>, NonZero<usize>>,
    base: cell::Cell<Option<Key<'r, 'q, 'd, ID>>>,
    initialized: cell::Cell<usize>,
}

const _: () = assert!(
    mem::size_of::<Registry<'static, 'static, 'static, KeyTag<0>>>() == 6 * mem::size_of::<usize>()
);

#[derive(Clone, Copy, Debug)]
pub enum BuildError {
    BucketCapacity { capacity: NonZero<u32> },
    Allocation(alloc::Error),
}

impl<'r, 'q, 'd, ID: Identity> Registry<'r, 'q, 'd, ID> {
    pub(super) fn try_with<R>(
        capacity: NonZero<u32>,
        run: impl for<'s> FnOnce(Registry<'s, 'q, 'd, ID>) -> R,
    ) -> Result<R, BuildError> {
        let len = NonZero::<usize>::MIN.saturating_add(capacity.get() as usize - 1);
        let entries: Box<[cell::OnceCell<Registered<'_, 'q, 'd, ID>>]> =
            Box::try_init(len.get(), cell::OnceCell::new).map_err(BuildError::Allocation)?;
        let count = len
            .checked_next_power_of_two()
            .ok_or(BuildError::BucketCapacity { capacity })?;
        let buckets = array::Block::try_new(count, ())
            .map_err(BuildError::Allocation)?
            .repeat_with(|| cell::Cell::new(None));
        Ok(run(Registry {
            entries: &entries,
            buckets,
            base: cell::Cell::new(None),
            initialized: cell::Cell::new(0),
        }))
    }

    pub(in crate::client) fn acquire_base<T, E, M>(
        &self,
        uri: &target::Target,
        ready: &'q Tagged<'d, ID>,
        waiters: &Waiters<'d, ID, Key<'r, 'q, 'd, ID>>,
        prepare: impl FnOnce(&Origin) -> Result<T, Error<E, M>>,
    ) -> Result<(Key<'r, 'q, 'd, ID>, T), Error<E, M>> {
        if let Some(key) = self.base.get() {
            return Ok((key, prepare(&key.entry.origin)?));
        }
        let (key, value) = self.acquire(uri, ready, waiters, prepare)?;
        self.base.set(Some(key));
        Ok((key, value))
    }

    pub(in crate::client) fn acquire<T, E, M>(
        &self,
        uri: &target::Target,
        ready: &'q Tagged<'d, ID>,
        waiters: &Waiters<'d, ID, Key<'r, 'q, 'd, ID>>,
        prepare: impl FnOnce(&Origin) -> Result<T, Error<E, M>>,
    ) -> Result<(Key<'r, 'q, 'd, ID>, T), Error<E, M>> {
        let parsed = Origin::parse(uri).map_err(Error::Origin)?;
        let hash = hash(&parsed);
        match self.probe(hash, &parsed) {
            Probe::Found(key) => Ok((key, prepare(&key.entry.origin)?)),
            Probe::Missing(bucket) => {
                let index = self.initialized.get();
                let Some(entry) = self.entries.get(index) else {
                    return Err(Error::Origin(OriginError::Capacity));
                };
                let lane = ready
                    .lane(index)
                    .map_err(OriginError::Lane)
                    .map_err(Error::Origin)?;
                let wake = waiters.try_index(index).map_err(Error::Origin)?;
                let origin = parsed
                    .try_own()
                    .map_err(OriginError::Allocation)
                    .map_err(Error::Origin)?;
                let value = prepare(&origin)?;
                let address = origin
                    .address()
                    .map_err(connect::Error::Resolve)
                    .map_err(Error::Connect)?;
                let entry = entry.get_or_init(|| Registered {
                    lane,
                    hash,
                    wake,
                    next: bucket.get(),
                    origin,
                    address,
                });
                let key = Key {
                    entry,
                    driver: marker::PhantomData,
                };
                bucket.set(Some(key));
                self.initialized.set(index + 1);
                Ok((key, value))
            }
        }
    }

    fn probe<'a>(&'a self, hash: u32, parsed: &Parsed<'_>) -> Probe<'a, 'r, 'q, 'd, ID> {
        let bucket = dynamic::raw::bucket(&self.buckets, hash);
        let mut current = bucket.get();
        while let Some(key) = current {
            if key.entry.hash == hash
                && key.entry.origin.host().eq_ignore_ascii_case(parsed.host)
                && key.entry.origin.port == parsed.port
            {
                return Probe::Found(key);
            }
            current = key.entry.next;
        }
        Probe::Missing(bucket)
    }
}

fn hash(parsed: &Parsed<'_>) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    let bytes = parsed
        .host
        .bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .chain([0])
        .chain(parsed.port.to_be_bytes());
    for byte in bytes {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}
