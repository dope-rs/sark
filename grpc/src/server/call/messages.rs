//! Buffered decoded messages owned by one admitted call lane.

use std::num;

use o3::{
    buffer,
    depot::{capacity, queue::pool},
};

use crate::{details, message};

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
