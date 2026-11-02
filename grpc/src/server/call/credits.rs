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
        let (charge, total) = budget.plan(added, local, total)?;
        Ok(Update {
            budget,
            charge,
            total,
        })
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
            charge: left,
            total,
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
    charge: usize,
    total: usize,
}

pub(in crate::server) struct Staged<'a, T> {
    budget: Budget<'a>,
    state: State<T>,
}

enum State<T> {
    Empty,
    Message {
        value: T,
        charge: usize,
        total: usize,
    },
}

pub(in crate::server) struct Admission<'a, T> {
    state: &'a mut State<T>,
    charge: usize,
    total: usize,
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
        self.budget.charge.bytes = self.charge;
        self.budget.ledger.used = self.total;
    }
}

impl<'a, T> Staged<'a, T> {
    pub(in crate::server) fn new(ledger: &'a mut Ledger, charge: &'a mut Charge) -> Self {
        Self {
            budget: Budget::new(ledger, charge),
            state: State::Empty,
        }
    }

    pub(in crate::server) fn is_empty(&self) -> bool {
        matches!(self.state, State::Empty)
    }

    pub(in crate::server) fn clear(&mut self) {
        self.state = State::Empty;
    }

    pub(in crate::server) fn check(
        &self,
        added: usize,
        local: usize,
        total: usize,
    ) -> Result<(usize, usize), Credit> {
        self.budget.plan(added, local, total)
    }

    pub(in crate::server) fn try_grow(
        &mut self,
        added: usize,
        local: usize,
        total: usize,
    ) -> Result<Admission<'_, T>, Credit> {
        let (charge, total) = self.budget.plan(added, local, total)?;
        Ok(Admission {
            state: &mut self.state,
            charge,
            total,
        })
    }

    pub(in crate::server) fn finish(self) -> Option<(T, Update<'a>)> {
        match self.state {
            State::Empty => None,
            State::Message {
                value,
                charge,
                total,
            } => Some((
                value,
                Update {
                    budget: self.budget,
                    charge,
                    total,
                },
            )),
        }
    }
}

impl<T> Admission<'_, T> {
    pub(in crate::server) fn write(self, value: T) {
        *self.state = State::Message {
            value,
            charge: self.charge,
            total: self.total,
        };
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
