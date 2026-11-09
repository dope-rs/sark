mod snapshot;

pub use snapshot::Snapshot;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Capability(u32);

impl Capability {
    pub const NONE: Self = Self(0);
    pub const REQUEST_BOOK_ENTRIES: Self = Self(1 << 0);
    pub const PRE_ADMISSION_BYTES: Self = Self(1 << 1);
    pub const PRE_ADMISSION_CHUNKS: Self = Self(1 << 2);
    pub const REQUEST_READY_NODES: Self = Self(1 << 3);
    pub const TASK_BUDGET_USED: Self = Self(1 << 4);
    pub const LIVE_TASKS: Self = Self(1 << 5);
    pub const REQUEST_OWNER_BYTES: Self = Self(1 << 6);
    pub const REQUEST_OWNER_PEAK_BYTES: Self = Self(1 << 7);
    pub const REQUEST_OWNERS: Self = Self(1 << 8);
    pub const RECEIVE_STREAMS: Self = Self(1 << 9);
    pub const TRANSMIT_STREAMS: Self = Self(1 << 10);
    pub const CONNECTION_READY_ENTRIES: Self = Self(1 << 11);
    pub const H3_RESPONSE_OWNER_BYTES: Self = Self(1 << 12);
    pub const TRANSPORT_RESPONSE_OWNER_BYTES: Self = Self(1 << 13);
    pub const RESPONSE_STAGING_OWNER_BYTES: Self = Self(1 << 14);
    pub const TASK_COMPLETION_SLOTS: Self = Self(1 << 15);
    pub const ALL: Self = Self((1 << 16) - 1);
    pub const QUIESCENT: Self = Self(Self::ALL.0 & !Self::REQUEST_OWNER_PEAK_BYTES.0);

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn from_bits(bits: u32) -> Option<Self> {
        if bits & !Self::ALL.0 == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Fixed-width, pointer-free projection of one lifecycle audit snapshot.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Projection {
    known: Capability,
    values: [usize; 16],
}

impl Projection {
    pub const fn from_parts(known: Capability, values: [usize; 16]) -> Self {
        Self { known, values }
    }

    pub const fn into_parts(self) -> (Capability, [usize; 16]) {
        (self.known, self.values)
    }

    pub const fn restore(self) -> Snapshot {
        Snapshot {
            known: self.known,
            values: self.values,
        }
    }
}
