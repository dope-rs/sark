use std::ops;

use dope_quic::stream::send;
use o3::{
    buffer::{hold, resident},
    depot::capacity,
};
use output::{Payload as _, WritePayload, WritePrefix};
use sark_core::manifold::stream::Boundary;

use crate::{conn::output, stream};

/// A frozen frame whose wire and retention sums were proved by construction.
#[repr(transparent)]
pub(in crate::conn) struct Record<'d> {
    pub(super) write: output::Write<'d>,
}

impl<'d> Record<'d> {
    pub(in crate::conn::io::write) fn single(
        target: stream::Target,
        prefix: output::WritePrefix,
        boundary: Boundary,
    ) -> Self {
        Self {
            write: output::Write::new(target, prefix, None, boundary),
        }
    }

    pub(in crate::conn::io::write) const fn get(&self) -> &output::Write<'d> {
        &self.write
    }

    pub(in crate::conn::io::write) fn into_output(self) -> output::Write<'d> {
        self.write
    }

    pub(in crate::conn::io::write) fn charge(&self) -> output::WriteCharge {
        let prefix = self.write.prefix.charge();
        let body = self.write.payload.as_ref().map_or(
            output::WriteCharge::default(),
            output::WritePayload::measure,
        );
        output::WriteCharge::new(
            prefix.wire_bytes + body.wire_bytes,
            prefix.resident_bytes + body.resident_bytes,
        )
    }

    pub(in crate::conn) fn retained(&self) -> hold::Charge {
        output::retention(&self.write.prefix, self.write.payload.as_ref())
    }

    pub(in crate::conn::io::write) fn demand(&self) -> Result<send::Demand, send::Overflow> {
        let prefix = u32::from(!self.write.prefix.is_empty());
        let payload = self.write.payload.as_ref().filter(|body| !body.is_empty());
        let stored = u32::from(payload.is_some());
        let body = match payload {
            Some(WritePayload::Segmented(body)) => body.as_chain().count().into_raw() as usize,
            _ => stored as usize,
        };
        let segments = (prefix as usize)
            .checked_add(body)
            .ok_or(send::Overflow::Segments {
                left: prefix as usize,
                right: body,
            })?;
        let head = if prefix == 0 {
            output::WriteCharge::default()
        } else {
            self.write.prefix.charge()
        };
        let tail = payload.map_or(output::WriteCharge::default(), WritePayload::measure);
        Ok(send::Demand::new(
            head.wire_bytes + tail.wire_bytes,
            head.resident_bytes + tail.resident_bytes,
            segments,
            capacity::Count::new(prefix + stored),
        ))
    }

    pub(in crate::conn::io::write) fn into_parts(self) -> send::Parts<resident::Shared<'d>> {
        let boundary = match self.write.boundary() {
            Boundary::More => send::Boundary::More,
            Boundary::End => send::Boundary::End,
        };
        let first = match self.write.prefix {
            WritePrefix::Inline(bytes) => send::Buffer::Inline(bytes),
            WritePrefix::Owned(bytes) => send::Buffer::Owned(bytes),
        };
        match self.write.payload {
            None => send::Parts::new(first, None, boundary),
            Some(WritePayload::Owned(bytes)) => {
                send::Parts::new(first, Some(send::Buffer::Owned(bytes)), boundary)
            }
            Some(WritePayload::Shared(owner)) => send::Parts::new(
                first,
                Some(send::Buffer::Retained(resident::Shared::from(owner))),
                boundary,
            ),
            Some(WritePayload::Retained(owner)) => {
                send::Parts::new(first, Some(send::Buffer::Retained(owner)), boundary)
            }
            Some(WritePayload::Segmented(chain)) => send::Parts::from_chain(first, chain, boundary),
        }
    }
}

impl<'d> ops::Deref for Record<'d> {
    type Target = output::Write<'d>;
    fn deref(&self) -> &Self::Target {
        &self.write
    }
}

const _: () = assert!(size_of::<Record<'static>>() == size_of::<output::Write<'static>>());
