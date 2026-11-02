use std::num;

use dope::{manifold::client::port, net::link::egress};

use crate::{
    client::{self, outbound},
    frame,
};

pub(super) struct Layout {
    entries: usize,
    resident: usize,
}

struct Bound {
    entries: num::NonZero<u32>,
    resident: u32,
}

impl Layout {
    pub(super) fn new(config: &client::Config) -> Result<Self, client::Error> {
        let message = config.max_message_size.get();
        let frame = config.max_outbound_frame_payload.get();
        let frames = message.div_ceil(frame);
        let overhead =
            frames
                .checked_mul(outbound::MAX_FRAME_OVERHEAD)
                .ok_or(client::Error::Layout(client::Layout::Overhead {
                    frames,
                    bytes: outbound::MAX_FRAME_OVERHEAD,
                }))?;
        let resident = message
            .checked_add(overhead)
            .map(|bytes| bytes.max(frame::MAX_CONTROL_PAYLOAD + outbound::MAX_FRAME_OVERHEAD))
            .ok_or(client::Error::Layout(client::Layout::Resident {
                base: message,
                added: overhead,
            }))?;
        Ok(Self {
            entries: frames,
            resident,
        })
    }

    pub(super) fn config(
        &self,
        lanes: usize,
        entry: usize,
        bytes: usize,
    ) -> Result<egress::Config, client::Error> {
        let bound = self.bound(lanes, entry, bytes)?;
        Ok(egress::Config::shared(egress::Credit::new(
            egress::Entries::new(bound.entries.get()),
            egress::Resident::new(bound.resident),
        )))
    }

    pub(super) fn stage(&self, lanes: usize) -> Result<port::Plan<()>, client::Error> {
        let lanes = num::NonZero::<usize>::new(lanes).ok_or(client::Error::Layout(
            client::Layout::EntryZero {
                lanes,
                each: self.entries,
            },
        ))?;
        let bound = self.bound(lanes.get(), 0, 0)?;
        port::Plan::from_shared(lanes, bound.entries, bound.resident).map_err(client::Error::Lanes)
    }

    fn bound(&self, lanes: usize, entry: usize, bytes: usize) -> Result<Bound, client::Error> {
        let each = self
            .entries
            .checked_add(entry)
            .ok_or(client::Error::Layout(client::Layout::EntryOverlap {
                base: self.entries,
                added: entry,
            }))?;
        let total =
            lanes
                .checked_mul(each)
                .ok_or(client::Error::Layout(client::Layout::EntryTotal {
                    lanes,
                    each,
                }))?;
        let Some(total) = num::NonZero::new(total) else {
            return Err(client::Error::Layout(client::Layout::EntryZero {
                lanes,
                each,
            }));
        };
        let Ok(entries) = num::NonZero::<u32>::try_from(total) else {
            return Err(client::Error::Layout(client::Layout::EntryRange {
                actual: total.get(),
            }));
        };
        let each = self
            .resident
            .checked_add(bytes)
            .ok_or(client::Error::Layout(client::Layout::ResidentOverlap {
                base: self.resident,
                added: bytes,
            }))?;
        let total = lanes.checked_mul(each).ok_or(client::Error::Layout(
            client::Layout::ResidentTotal { lanes, each },
        ))?;
        let Ok(resident) = u32::try_from(total) else {
            return Err(client::Error::Layout(client::Layout::ResidentRange {
                actual: total,
            }));
        };
        Ok(Bound { entries, resident })
    }
}
