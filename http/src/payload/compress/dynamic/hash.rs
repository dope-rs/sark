use std::hash;

use dope_core::random;
use dope_runtime::random::{HashSeed, Namespace};

use crate::field::metadata::map::record::Entry;

/// Thread-bound keyed hash seed owned by one dynamic table.
#[derive(Clone)]
pub struct Seed {
    name: HashSeed,
    exact: HashSeed,
}

impl Seed {
    pub fn new(seed: &random::Seed) -> Self {
        const DOMAIN: Namespace = Namespace::new(*b"sarkhttp");
        Self {
            name: HashSeed::from_seed(seed, DOMAIN.instance(0)),
            exact: HashSeed::from_seed(seed, DOMAIN.instance(1)),
        }
    }

    pub fn probe<'a>(&self, field: Entry<'a>) -> Probe<'a> {
        Probe {
            field,
            digest: self.digest(field),
        }
    }

    pub fn needle<'a>(&self, name: &'a [u8]) -> Needle<'a> {
        Needle {
            name,
            hash: self.name(name),
        }
    }

    fn digest(&self, field: Entry<'_>) -> Digest {
        let name = self.name(field.name);
        Digest {
            name,
            exact: self.exact.hash(&Exact(name, field.value)),
        }
    }

    fn name(&self, name: &[u8]) -> u64 {
        self.name.hash_bytes(name)
    }
}

/// Fixed-width name digest followed by the exact value bytes.
struct Exact<'a>(u64, &'a [u8]);

impl hash::Hash for Exact<'_> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.0);
        state.write(self.1);
    }
}

/// Name and exact-field hashes computed by one seed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest {
    name: u64,
    exact: u64,
}

impl Digest {
    pub const fn words(self) -> [u64; 2] {
        [self.name, self.exact]
    }

    pub const fn name(self) -> u64 {
        self.name
    }

    pub const fn exact(self) -> u64 {
        self.exact
    }
}

/// Borrowed field and its table-selected digest.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Probe<'a> {
    field: Entry<'a>,
    digest: Digest,
}

impl<'a> Probe<'a> {
    pub const fn field(self) -> Entry<'a> {
        self.field
    }

    pub const fn digest(self) -> Digest {
        self.digest
    }

    pub(super) const fn needle(self) -> Needle<'a> {
        Needle {
            name: self.field.name,
            hash: self.digest.name,
        }
    }
}

/// Borrowed name and its table-selected hash.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Needle<'a> {
    name: &'a [u8],
    hash: u64,
}

impl<'a> Needle<'a> {
    pub const fn name(self) -> &'a [u8] {
        self.name
    }

    pub const fn hash(self) -> u64 {
        self.hash
    }
}

const _: () = {
    assert!(core::mem::size_of::<Seed>() == 4 * core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Digest>() == 2 * core::mem::size_of::<u64>());
};
