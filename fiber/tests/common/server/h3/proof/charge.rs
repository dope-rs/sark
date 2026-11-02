use o3::buffer::hold;

use crate::common::server::h3::proof::{Metric, ledger::Error};

pub(super) fn remove(balance: &mut hold::Charge, charge: hold::Charge) -> Result<(), Error> {
    for (name, value, removed) in [
        (
            Metric::LogicalBytes,
            &mut balance.logical_bytes,
            charge.logical_bytes,
        ),
        (Metric::Owners, &mut balance.owners, charge.owners),
        (
            Metric::ResidentBytes,
            &mut balance.resident_bytes,
            charge.resident_bytes,
        ),
    ] {
        *value = value
            .checked_sub(removed)
            .ok_or(Error::Remove(name, *value, removed))?;
    }
    Ok(())
}
