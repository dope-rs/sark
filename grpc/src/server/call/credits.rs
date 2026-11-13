use o3::{
    buffer::storage,
    mem::{raw, slot},
};

use crate::details::{Account, Credit};

#[derive(Default)]
pub(in crate::server) struct Charge {
    bytes: usize,
}

impl Charge {
    pub(in crate::server) fn amount(&self) -> usize {
        self.bytes
    }
}

#[derive(Default)]
pub(in crate::server) struct Ledger {
    used: usize,
}

impl Ledger {
    pub(in crate::server) fn used(&self) -> usize {
        self.used
    }

    pub(in crate::server) fn try_grow<'a>(
        &'a mut self,
        charge: &'a mut Charge,
        added: usize,
        local: usize,
        total: usize,
    ) -> Result<Update<'a>, Credit> {
        let budget = Budget::new(self, charge);
        let amounts = budget.plan(added, local, total)?;
        Ok(Update { budget, amounts })
    }

    pub(in crate::server) fn shrink<'a>(
        &'a mut self,
        charge: &'a mut Charge,
        removed: usize,
    ) -> Result<Update<'a>, Credit> {
        let left = charge.bytes.checked_sub(removed).ok_or(Credit::Underflow {
            account: Account::Charge,
            used: charge.bytes,
            removed,
        })?;
        let total = self.used.checked_sub(removed).ok_or(Credit::Underflow {
            account: Account::Ledger,
            used: self.used,
            removed,
        })?;
        Ok(Update {
            budget: Budget::new(self, charge),
            amounts: (left, total),
        })
    }
}

/// Exclusive credit accounts retained through admission, commit, or cancellation.
struct Budget<'a> {
    ledger: &'a mut Ledger,
    charge: &'a mut Charge,
}

#[must_use]
pub(in crate::server) struct Update<'a> {
    budget: Budget<'a>,
    amounts: (usize, usize),
}

pub(in crate::server) struct Staged<'a> {
    budget: Budget<'a>,
    state: slot::Storage<(storage::Shared, (usize, usize))>,
}

pub(in crate::server) struct Vacant<'a, 'b> {
    state: raw::Vacant<'a, (storage::Shared, (usize, usize))>,
    budget: &'a Budget<'b>,
}

pub(in crate::server) struct Admission<'a> {
    state: raw::Vacant<'a, (storage::Shared, (usize, usize))>,
    amounts: (usize, usize),
}

impl<'a> Budget<'a> {
    fn new(ledger: &'a mut Ledger, charge: &'a mut Charge) -> Self {
        Self { ledger, charge }
    }

    fn plan(&self, added: usize, local: usize, total: usize) -> Result<(usize, usize), Credit> {
        let charge = self
            .charge
            .bytes
            .checked_add(added)
            .ok_or(Credit::Overflow {
                account: Account::Charge,
                used: self.charge.bytes,
                added,
            })?;
        let used = self
            .ledger
            .used
            .checked_add(added)
            .ok_or(Credit::Overflow {
                account: Account::Ledger,
                used: self.ledger.used,
                added,
            })?;
        if charge > local {
            return Err(Credit::Limit {
                account: Account::Charge,
                required: charge,
                capacity: local,
            });
        }
        if used > total {
            return Err(Credit::Limit {
                account: Account::Ledger,
                required: used,
                capacity: total,
            });
        }
        Ok((charge, used))
    }
}

impl<'a> Update<'a> {
    pub(in crate::server) fn commit(self) {
        let (charge, total) = self.amounts;
        self.budget.charge.bytes = charge;
        self.budget.ledger.used = total;
    }
}

impl<'a> Staged<'a> {
    pub(in crate::server) fn new(ledger: &'a mut Ledger, charge: &'a mut Charge) -> Self {
        Self {
            budget: Budget::new(ledger, charge),
            state: slot::Storage::new(()),
        }
    }

    pub(in crate::server) fn is_empty(&self) -> bool {
        self.state.get().is_none()
    }

    pub(in crate::server) fn clear(&mut self) {
        if let Some(state) = self.state.occupied() {
            state.clear();
        }
    }

    pub(in crate::server) fn check(
        &self,
        added: usize,
        local: usize,
        total: usize,
    ) -> Result<(usize, usize), Credit> {
        self.budget.plan(added, local, total)
    }

    pub(in crate::server) fn vacant(&mut self) -> Option<Vacant<'_, 'a>> {
        Some(Vacant {
            state: self.state.vacant()?,
            budget: &self.budget,
        })
    }

    pub(in crate::server) fn finish(mut self) -> Option<(storage::Shared, Update<'a>)> {
        let (value, amounts) = self.state.occupied()?.take().0;
        Some((
            value,
            Update {
                budget: self.budget,
                amounts,
            },
        ))
    }
}

impl<'a> Vacant<'a, '_> {
    pub(in crate::server) fn try_grow(
        self,
        added: usize,
        local: usize,
        total: usize,
    ) -> Result<Admission<'a>, Credit> {
        let amounts = self.budget.plan(added, local, total)?;
        Ok(Admission {
            state: self.state,
            amounts,
        })
    }
}

impl Admission<'_> {
    pub(in crate::server) fn write(self, value: storage::Shared) {
        self.state.insert((value, self.amounts));
    }
}

const _: () = {
    assert!(std::mem::size_of::<Credit>() == 3 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Budget<'static>>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Update<'static>>() == 4 * std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<Option<Update<'static>>>() == std::mem::size_of::<Update<'static>>()
    );
};
