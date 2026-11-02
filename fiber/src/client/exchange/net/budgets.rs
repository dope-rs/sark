//! Per-lane HTTP response budgets and checked arena totals.

use std::num::NonZero;

use dope_fiber::reply::{layout, usage};
use o3::depot::capacity;
use sark_client::protocol::codec::output;

use crate::client::config;

#[derive(Clone, Copy)]
pub(in crate::client) struct Plan {
    pub(super) report_capacity: capacity::Count,
    pub(super) arena: layout::Layout,
}

impl Plan {
    pub(in crate::client) fn new(
        capacity: usize,
        entries: usize,
        bytes: usize,
        output: output::Bound,
        reports: capacity::Count,
    ) -> Result<Self, config::Error> {
        let lanes = layout::Lanes::new(capacity);
        let item = |value: Option<usize>| value.ok_or(config::Error::ResponseItems);
        let fixed = output::INFORMATIONAL_LIMIT + 3;
        let heads = item(entries.checked_mul(fixed))?;
        let data = output::Bound::new(bytes).data_items();
        let items = item(heads.checked_add(data))?;
        let bound = item(output.data_items().checked_add(fixed))?;
        let lane = item(data.checked_add(fixed))?.min(bound);
        let limit = layout::Limit::new(
            layout::Data::new(lane),
            usage::Bytes::new(bytes),
            usage::Values::new(lane),
        );
        let scale = |value, error| lanes.get().checked_mul(value).ok_or(error);
        let entries = scale(entries, config::Error::ResponseEntries)?;
        let items = scale(items, config::Error::ResponseItems)?;
        let bytes = scale(bytes, config::Error::ResponseBytes)?;
        let arena = layout::Layout::new(
            layout::Capacity::new(
                lanes,
                layout::Entries::new(entries),
                layout::Data::new(items),
                usage::Bytes::new(bytes),
                usage::Values::new(items),
            ),
            limit,
        )
        .map_err(config::Error::ResponseLayout)?;
        Ok(Self {
            report_capacity: reports,
            arena,
        })
    }

    pub(super) const fn capacity(&self) -> NonZero<u32> {
        self.arena.lane_count()
    }
}
