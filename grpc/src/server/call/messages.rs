//! Buffered decoded messages owned by one admitted call lane.

use std::num;

use o3::{
    alloc, buffer,
    depot::{capacity, queue::pool},
};

use crate::{details, message};

pub(in crate::server) struct Pool<C: buffer::raw::Segment> {
    lanes: pool::Lanes<message::Frame<C>>,
    capacity: capacity::Count,
}

impl<C: buffer::raw::Segment> Pool<C> {
    pub(in crate::server) fn try_new(
        cap: capacity::Count,
        calls: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            lanes: pool::Lanes::try_new(pool::Plan::from_counts(cap, calls))?,
            capacity: cap,
        })
    }

    pub(in crate::server) fn try_bind(
        &mut self,
        index: usize,
    ) -> Result<pool::Lane<'_, message::Frame<C>>, pool::Missing> {
        self.lanes.try_bind_mut(index)
    }

    pub(in crate::server) fn is_empty(&self) -> bool {
        self.lanes.available() == self.capacity.get()
    }
}

pub(in crate::server) fn try_push<C: buffer::raw::Segment>(
    mut lane: pool::Lane<'_, message::Frame<C>>,
    message: message::Frame<C>,
    limit: usize,
) -> Result<(), details::Buffered> {
    let count = num::NonZero::<usize>::MIN.saturating_add(lane.count().get());
    if count.get() > limit {
        return Err(details::Buffered::Limit {
            actual: count,
            limit,
        });
    }
    let vacant = lane
        .vacant()
        .ok_or(details::Buffered::Full(capacity::Shortage {
            requested: num::NonZero::<usize>::MIN,
            available: 0,
        }))?;
    vacant.append(message);
    Ok(())
}
