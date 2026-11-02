//! HTTP/3 connection options and their exact validation failures.

use std::{alloc::LayoutError, fmt, num::TryFromIntError};

use o3::depot::{capacity, map};
use sark_http::field::metadata::block;

mod error;
pub mod plan;
pub use error::Error;

const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_STREAM_CAPACITY: usize = 1024;
const DEFAULT_EVENT_CAPACITY: usize = 1024;
const DEFAULT_WRITE_CAPACITY: usize = 1024;
const DEFAULT_FIELD_SECTION_BYTES: usize = 64 * 1024;
const DEFAULT_QPACK_SECTION_CAPACITY: usize = 1024;
const DEFAULT_QPACK_ENCODER_TABLE_BYTES: usize = 4096;
const DEFAULT_INBOUND_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_INBOUND_SEGMENTS: usize = 16 * 1024;
const DEFAULT_OUTBOUND_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_OUTBOUND_RESIDENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct Protocol {
    pub max_frame_bytes: usize,
    pub extended_connect: block::Connect,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            extended_connect: block::Connect::Disabled,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Streams {
    /// Maximum concurrent receive streams and lifetime push IDs: sent promises on
    /// servers or promised/received IDs on clients. New IDs beyond this limit fail
    /// with `push::Error::Capacity` before promise publication or response decoding.
    pub capacity: usize,
}

impl Default for Streams {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_STREAM_CAPACITY,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Ingress {
    pub event_capacity: usize,
    pub retained_bytes: usize,
    pub retained_segments: usize,
    pub field_section_bytes: usize,
}

impl Default for Ingress {
    fn default() -> Self {
        Self {
            event_capacity: DEFAULT_EVENT_CAPACITY,
            retained_bytes: DEFAULT_INBOUND_BYTES,
            retained_segments: DEFAULT_INBOUND_SEGMENTS,
            field_section_bytes: DEFAULT_FIELD_SECTION_BYTES,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Egress {
    pub write_capacity: usize,
    pub wire_bytes: usize,
    pub resident_bytes: usize,
}

impl Default for Egress {
    fn default() -> Self {
        Self {
            write_capacity: DEFAULT_WRITE_CAPACITY,
            wire_bytes: DEFAULT_OUTBOUND_BYTES,
            resident_bytes: DEFAULT_OUTBOUND_RESIDENT_BYTES,
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Decode {
    pub table_bytes: usize,
    pub blocked_streams: usize,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Encode {
    pub table_bytes: usize,
    pub section_capacity: usize,
}

impl Default for Encode {
    fn default() -> Self {
        Self {
            table_bytes: DEFAULT_QPACK_ENCODER_TABLE_BYTES,
            section_capacity: DEFAULT_QPACK_SECTION_CAPACITY,
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Qpack {
    pub decoder: Decode,
    pub encoder: Encode,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub protocol: Protocol,
    pub streams: Streams,
    pub ingress: Ingress,
    pub egress: Egress,
    pub qpack: Qpack,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Resource {
    Frame,
    Stream,
    Event,
    Write,
    FieldSection,
    QpackSection,
    QpackDecoderTable,
    QpackEncoderTable,
    BlockedStream,
    ReadyStream,
    ReceiveCredit,
    InboundByte,
    InboundSegment,
    OutboundByte,
    OutboundResidentByte,
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Arithmetic {
    Add,
    Mul,
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Debug)]
pub enum Bound {
    Wire,
    Depot(capacity::Error),
    Map(map::Capacity),
    Tagged { requested: usize },
    Layout(LayoutError),
    Integer(TryFromIntError),
}
