use o3::mem;
mod record;
pub(in crate::conn) use record::Record;
use sark_core::manifold::stream::Boundary;

use crate::{conn::output, stream};

pub(crate) struct Plan<T> {
    prefix: output::WritePrefix,
    payload: T,
    charge: output::WriteCharge,
}

impl Plan<Vec<u8>> {
    pub(in crate::conn::io) fn new(prefix: output::WritePrefix, payload: Vec<u8>) -> Self {
        let head = prefix.charge();
        // Both byte owners fit isize::MAX, so each pair sum fits usize.
        let charge = output::WriteCharge::new(
            head.wire_bytes + payload.len(),
            head.resident_bytes + payload.capacity(),
        );
        Self {
            prefix,
            payload,
            charge,
        }
    }
}

impl<'d, T: output::Payload<'d>> Plan<T> {
    pub(in crate::conn::io) fn try_measured(
        prefix: output::WritePrefix,
        payload: T,
        charge: output::WriteCharge,
    ) -> Result<Self, mem::Rejected<T, output::Overflow>> {
        let charge = match prefix.charge().try_add(charge) {
            Ok(charge) => charge,
            Err(error) => {
                return Err(mem::Rejected::new(payload, error));
            }
        };
        // Sealed payloads bound retained logical bytes by wire bytes, and both
        // retained resident bytes and owner counts by resident bytes.
        Ok(Self {
            prefix,
            payload,
            charge,
        })
    }

    pub(in crate::conn::io) fn freeze(self) -> Plan<Option<output::WritePayload<'d>>> {
        Plan {
            prefix: self.prefix,
            payload: Some(self.payload.into_payload()),
            charge: self.charge,
        }
    }
}

impl<T> Plan<T> {
    pub(in crate::conn::io) const fn charge(&self) -> output::WriteCharge {
        self.charge
    }

    pub(in crate::conn::io) fn into_payload(self) -> T {
        self.payload
    }
}

impl<'d> Plan<Option<output::WritePayload<'d>>> {
    pub(in crate::conn::io) fn single(prefix: output::WritePrefix) -> Self {
        let charge = prefix.charge();
        Self {
            prefix,
            payload: None,
            charge,
        }
    }

    pub(in crate::conn::io) fn retained(&self) -> o3::buffer::hold::Charge {
        output::retention(&self.prefix, self.payload.as_ref())
    }

    pub(in crate::conn::io) fn into_write(
        self,
        target: stream::Target,
        boundary: Boundary,
    ) -> Record<'d> {
        Record {
            write: output::Write::new(target, self.prefix, self.payload, boundary),
        }
    }
}
