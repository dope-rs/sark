use std::num::NonZero;

use crate::conn::config::payload::Error;

/// Connection-lifetime capacities for retained ingress payload owners.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Payload {
    data: NonZero<usize>,
    headers: NonZero<usize>,
    segments: usize,
}

impl Payload {
    /// Uses an additive arena: one maximally fragmented input plus one
    /// segment for every remaining live DATA event.
    pub const fn new(data: usize, headers: usize) -> Result<Self, Error> {
        let dcount = match NonZero::new(data) {
            Some(count) => count,
            None => return Err(Error::ZeroData),
        };
        let hcount = match NonZero::new(headers) {
            Some(count) => count,
            None => return Err(Error::ZeroHeaders),
        };
        let slots = match data.checked_add(headers) {
            Some(slots) => slots,
            None => return Err(Error::SlotOverflow { data, headers }),
        };
        let ingress = match slots.checked_add(1) {
            Some(ingress) => ingress,
            None => return Err(Error::IngressOverflow { slots }),
        };
        let segments = match data.checked_add(ingress - 1) {
            Some(segments) => segments,
            None => return Err(Error::SegmentOverflow { data, ingress }),
        };
        Ok(Self {
            data: dcount,
            headers: hcount,
            segments,
        })
    }

    /// Replaces the arena bound while preserving enough space for one input
    /// fragmented across every ingress slot.
    pub const fn with_arena(self, segments: usize) -> Result<Self, Error> {
        let minimum = self.data.get() + self.headers.get() + 1;
        if segments < minimum {
            return Err(Error::ArenaTooSmall {
                required: minimum,
                actual: segments,
            });
        }
        Ok(Self { segments, ..self })
    }

    /// Adds owners that can outlive the DATA-event permit which admitted them.
    pub const fn extend(self, extra: usize) -> Result<Self, Error> {
        let segments = match self.segments.checked_add(extra) {
            Some(segments) => segments,
            None => {
                return Err(Error::RetentionOverflow {
                    segments: self.segments,
                    extra,
                });
            }
        };
        Ok(Self { segments, ..self })
    }

    /// Maximum number of live DATA events.
    pub const fn data(self) -> usize {
        self.data.get()
    }

    /// Maximum number of live header blocks.
    pub const fn headers(self) -> usize {
        self.headers.get()
    }

    /// Maximum number of retained payload segments.
    pub const fn segments(self) -> usize {
        self.segments
    }
}

impl Default for Payload {
    fn default() -> Self {
        use crate::conn::config::{
            DEFAULT_DATA_EVENTS, DEFAULT_HEADER_EVENTS, DEFAULT_RETAINED_SEGMENTS,
        };

        Self {
            data: DEFAULT_DATA_EVENTS,
            headers: DEFAULT_HEADER_EVENTS,
            segments: DEFAULT_RETAINED_SEGMENTS,
        }
    }
}
