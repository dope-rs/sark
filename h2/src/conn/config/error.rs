use std::num;

use o3::{
    buffer::{bytes::window::reserve, hold::limits, pool},
    depot::capacity,
};
use sark_http::payload::compress::dynamic::plan;

use crate::{
    conn::{
        self,
        config::{Field, payload},
    },
    frame::numbers,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    ZeroCapacity(Field),
    StreamCapacity(capacity::Error),
    EventCapacity(capacity::Error),
    HpackPlan(plan::Error),
    IngressCount(capacity::Error),
    RetentionCount(capacity::Error),
    PayloadCapacity(payload::Error),
    IngressCapacityOverflow {
        permits: usize,
        fields: usize,
    },
    IngressBudgetOverflow(limits::Error),
    InitialWindowOverflow {
        actual: u32,
        max: u32,
    },
    ReceiveWindowOverflow {
        actual: u32,
        max: u32,
    },
    HpackBlockOverflow {
        list: usize,
    },
    HpackWireOverflow {
        list: usize,
    },
    OutboundCapacityOverflow {
        logical: num::NonZero<usize>,
        source: reserve::Error,
    },
    InboundHeadroom {
        buffer: num::NonZero<u32>,
        frame: numbers::Limit,
    },
    OutboundHeadroom {
        base: usize,
        extra: usize,
    },
    OutboundTooSmall {
        required: usize,
        actual: usize,
    },
    Pool(pool::LayoutError),
    Build(conn::Build),
}

impl From<pool::LayoutError> for Error {
    fn from(error: pool::LayoutError) -> Self {
        Self::Pool(error)
    }
}
