use crate::body::{admit, buffered, credit, ingress, input};

use crate::body::buffered::receive::Receive;

/// An exact carrier's retained buffer lent under its account's current identity.
pub struct Loan<'a, S, O: credit::Origin> {
    pub(in crate::body) state: &'a mut buffered::State<S>,
    pub(in crate::body) _origin: O,
}

impl<S, O: credit::Origin> Loan<'_, S, O> {
    pub const fn max_body(&self) -> usize {
        self.state.max_body()
    }

    pub const fn retention(&self) -> admit::Retention {
        self.state.retention()
    }

    pub const fn stream(&self) -> admit::Stream {
        self.state.stream()
    }

    pub const fn len(&self) -> usize {
        self.state.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub fn try_push<D, E, F, R>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
        input: input::Input<D>,
        rmax: credit::Limits,
        cmax: credit::Limits,
        retain: F,
    ) -> buffered::Push<D, R>
    where
        E: credit::Evidence,
        F: FnOnce(Option<S>, D) -> buffered::Retain<S, D, R>,
    {
        let turn = ingress::Turn::from_account(&mut self.state.state, ledger.amount(), event);
        Receive {
            turn,
            storage: &mut self.state.storage,
        }
        .try_push(input, rmax, cmax, retain)
    }
}
