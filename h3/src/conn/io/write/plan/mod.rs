use o3::{buffer::hold, mem};
mod record;
pub(in crate::conn) use record::Record;
use sark_core::manifold::stream::Boundary;

use crate::{conn::output, stream};

pub(in crate::conn::io) struct Plan<T> {
    prefix: output::WritePrefix,
    payload: T,
    charge: output::WriteCharge,
}

impl<'d, T: output::Payload<'d>> Plan<T> {
    pub(in crate::conn::io) fn try_new(
        prefix: output::WritePrefix,
        payload: T,
    ) -> Result<(Self, hold::Charge), mem::Rejected<T, output::Error>> {
        let right = payload.measure();
        Self::try_measured(prefix, payload, right)
    }

    pub(in crate::conn::io) fn try_measured(
        prefix: output::WritePrefix,
        payload: T,
        charge: output::WriteCharge,
    ) -> Result<(Self, hold::Charge), mem::Rejected<T, output::Error>> {
        let charge = match prefix.charge().try_add(charge) {
            Ok(charge) => charge,
            Err(error) => {
                return Err(mem::Rejected::new(payload, output::Error::Charge(error)));
            }
        };
        let retained = match prefix.retention().try_add(payload.retention()) {
            Ok(retained) => retained,
            Err(error) => {
                return Err(mem::Rejected::new(payload, output::Error::Retention(error)));
            }
        };
        Ok((
            Self {
                prefix,
                payload,
                charge,
            },
            retained,
        ))
    }

    pub(in crate::conn::io) const fn charge(&self) -> output::WriteCharge {
        self.charge
    }

    pub(in crate::conn::io) fn into_payload(self) -> T {
        self.payload
    }

    pub(in crate::conn::io) fn into_write(
        self,
        target: stream::Target,
        boundary: Boundary,
    ) -> Record<'d> {
        Record {
            write: output::Write::segmented(
                target,
                self.prefix,
                self.payload.into_payload(),
                boundary,
            ),
        }
    }
}
