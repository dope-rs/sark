use o3::depot::array::Index;

use crate::{conn, conn::output, qpack};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteClass {
    Regular,
    Critical,
}

impl WriteClass {
    pub(in crate::conn) const fn lane(self) -> Index<2> {
        match self {
            Self::Regular => Index::at::<0>(),
            Self::Critical => Index::at::<1>(),
        }
    }

    pub(in crate::conn) fn try_add(
        self,
        left: output::WriteCharge,
        right: output::WriteCharge,
    ) -> Result<output::WriteCharge, conn::Error> {
        left.try_add(right).map_err(|error| match (self, error) {
            (Self::Regular, output::Overflow::Wire { left, right }) => {
                conn::Error::BatchRegularWire { left, right }
            }
            (Self::Regular, output::Overflow::Resident { left, right }) => {
                conn::Error::BatchRegularResident { left, right }
            }
            (Self::Critical, output::Overflow::Wire { left, right }) => {
                conn::Error::BatchCriticalWire { left, right }
            }
            (Self::Critical, output::Overflow::Resident { left, right }) => {
                conn::Error::BatchCriticalResident { left, right }
            }
        })
    }
}

pub(super) struct Change<'a> {
    budget: &'a mut WriteBudget,
    retained: output::WriteCharge,
    regular: output::WriteCharge,
}

#[must_use = "a checked feedback allowance must be committed after decoder ingestion"]
pub(super) struct Allowance<'a> {
    change: Change<'a>,
    room: usize,
}

impl Change<'_> {
    pub(super) fn commit(self) {
        self.budget.retained = self.retained;
        self.budget.regular = self.regular;
    }
}

impl<'a> Allowance<'a> {
    pub(super) fn try_new(
        budget: &'a mut WriteBudget,
        before: usize,
        limit: usize,
    ) -> Result<Self, conn::Error> {
        let room = limit.saturating_sub(before);
        let change = budget.try_retain(room)?;
        Ok(Self { change, room })
    }

    pub(super) fn commit(mut self, growth: qpack::Growth) {
        self.change.retained.wire_bytes -= self.room - growth.bytes();
        self.change.commit();
    }
}

pub(in crate::conn) struct WriteBudget {
    retained: output::WriteCharge,
    regular: output::WriteCharge,
    limit: output::WriteCharge,
    regular_limit: output::WriteCharge,
}

impl WriteBudget {
    const MAX_CRITICAL_RESERVE: usize = 64 * 1024;

    pub(in crate::conn) const fn new(wire: usize, resident: usize) -> Self {
        Self {
            retained: output::WriteCharge::new(0, 0),
            regular: output::WriteCharge::new(0, 0),
            limit: output::WriteCharge::new(wire, resident),
            regular_limit: output::WriteCharge::new(
                super::regular_limit(wire, Self::MAX_CRITICAL_RESERVE),
                super::regular_limit(resident, Self::MAX_CRITICAL_RESERVE),
            ),
        }
    }

    /// Refused owners remain outside the queued-write budget. Both dimensions
    /// must fit independently before a later admission can make progress.
    pub(in crate::conn) fn check_pending(
        &self,
        charge: output::WriteCharge,
        class: WriteClass,
    ) -> Result<(), conn::Error> {
        let limit = match class {
            WriteClass::Regular => self.regular_limit,
            WriteClass::Critical => self.limit,
        };
        if charge.wire_bytes > limit.wire_bytes {
            return Err(match class {
                WriteClass::Regular => conn::Error::BudgetRegularWire {
                    retained: 0,
                    additional: charge.wire_bytes,
                    limit: limit.wire_bytes,
                },
                WriteClass::Critical => conn::Error::BudgetWire {
                    retained: 0,
                    additional: charge.wire_bytes,
                    limit: limit.wire_bytes,
                },
            });
        }
        if charge.resident_bytes > limit.resident_bytes {
            return Err(match class {
                WriteClass::Regular => conn::Error::BudgetRegularResident {
                    retained: 0,
                    additional: charge.resident_bytes,
                    limit: limit.resident_bytes,
                },
                WriteClass::Critical => conn::Error::BudgetResident {
                    retained: 0,
                    additional: charge.resident_bytes,
                    limit: limit.resident_bytes,
                },
            });
        }
        Ok(())
    }

    pub(super) fn try_reserve(
        &mut self,
        regular: output::WriteCharge,
        critical: output::WriteCharge,
    ) -> Result<Change<'_>, conn::Error> {
        let retained = output::WriteCharge::new(
            {
                let used = self
                    .retained
                    .wire_bytes
                    .checked_add(regular.wire_bytes)
                    .ok_or(conn::Error::BudgetWire {
                        retained: self.retained.wire_bytes,
                        additional: regular.wire_bytes,
                        limit: self.limit.wire_bytes,
                    })?;
                let used =
                    used.checked_add(critical.wire_bytes)
                        .ok_or(conn::Error::BudgetWire {
                            retained: used,
                            additional: critical.wire_bytes,
                            limit: self.limit.wire_bytes,
                        })?;
                if used > self.limit.wire_bytes {
                    return Err(conn::Error::BudgetWire {
                        retained: self.retained.wire_bytes,
                        additional: used - self.retained.wire_bytes,
                        limit: self.limit.wire_bytes,
                    });
                }
                used
            },
            {
                let used = self
                    .retained
                    .resident_bytes
                    .checked_add(regular.resident_bytes)
                    .ok_or(conn::Error::BudgetResident {
                        retained: self.retained.resident_bytes,
                        additional: regular.resident_bytes,
                        limit: self.limit.resident_bytes,
                    })?;
                let used = used.checked_add(critical.resident_bytes).ok_or(
                    conn::Error::BudgetResident {
                        retained: used,
                        additional: critical.resident_bytes,
                        limit: self.limit.resident_bytes,
                    },
                )?;
                if used > self.limit.resident_bytes {
                    return Err(conn::Error::BudgetResident {
                        retained: self.retained.resident_bytes,
                        additional: used - self.retained.resident_bytes,
                        limit: self.limit.resident_bytes,
                    });
                }
                used
            },
        );
        let next = output::WriteCharge::new(
            match self.regular.wire_bytes.checked_add(regular.wire_bytes) {
                Some(used) if used <= self.regular_limit.wire_bytes => used,
                _ => {
                    return Err(conn::Error::BudgetRegularWire {
                        retained: self.regular.wire_bytes,
                        additional: regular.wire_bytes,
                        limit: self.regular_limit.wire_bytes,
                    });
                }
            },
            match self
                .regular
                .resident_bytes
                .checked_add(regular.resident_bytes)
            {
                Some(used) if used <= self.regular_limit.resident_bytes => used,
                _ => {
                    return Err(conn::Error::BudgetRegularResident {
                        retained: self.regular.resident_bytes,
                        additional: regular.resident_bytes,
                        limit: self.regular_limit.resident_bytes,
                    });
                }
            },
        );
        Ok(Change {
            budget: self,
            retained,
            regular: next,
        })
    }

    pub(super) fn try_retain(&mut self, bytes: usize) -> Result<Change<'_>, conn::Error> {
        let wire = match self.retained.wire_bytes.checked_add(bytes) {
            Some(wire) if wire <= self.limit.wire_bytes => wire,
            _ => {
                return Err(conn::Error::BudgetWire {
                    retained: self.retained.wire_bytes,
                    additional: bytes,
                    limit: self.limit.wire_bytes,
                });
            }
        };
        Ok(Change {
            retained: output::WriteCharge::new(wire, self.retained.resident_bytes),
            regular: self.regular,
            budget: self,
        })
    }

    pub(super) fn release(&mut self, class: WriteClass, charge: output::WriteCharge) {
        self.retained.wire_bytes -= charge.wire_bytes;
        self.retained.resident_bytes -= charge.resident_bytes;
        if class == WriteClass::Regular {
            self.regular.wire_bytes -= charge.wire_bytes;
            self.regular.resident_bytes -= charge.resident_bytes;
        }
    }

    pub(in crate::conn) const fn wire_len(&self) -> usize {
        self.retained.wire_bytes
    }

    pub(super) const fn wire_limit(&self) -> usize {
        self.limit.wire_bytes
    }

    pub(in crate::conn) const fn resident_len(&self) -> usize {
        self.retained.resident_bytes
    }

    pub(super) fn wire_left(&self) -> usize {
        self.limit.wire_bytes - self.retained.wire_bytes
    }
}

const _: () = {
    assert!(core::mem::size_of::<WriteBudget>() == 8 * core::mem::size_of::<usize>());
    assert!(
        core::mem::size_of::<Allowance<'static>>()
            == core::mem::size_of::<Change<'static>>() + core::mem::size_of::<usize>()
    );
};
