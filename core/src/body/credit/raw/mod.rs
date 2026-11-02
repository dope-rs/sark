use o3::{
    base::{cell::region, marker::ThreadBound},
    buffer::segments,
};

use crate::body::{
    buffered,
    credit::{Amount, Origin},
};

mod bind;

/// Persisted credit whose issuing account remains in its exclusive carrier owner.
#[derive(Clone, Copy, Debug)]
pub struct Stored {
    _private: (),
}

impl Stored {
    pub(in crate::body) const fn new() -> Self {
        Self { _private: () }
    }
}

/// One carrier's persistent account, kept beside all of its charged body states.
pub struct Account {
    retained: Amount,
    _thread: ThreadBound,
}

impl Account {
    pub const fn new() -> Self {
        Self {
            retained: Amount::new(0, segments::Count::EMPTY),
            _thread: ThreadBound::NEW,
        }
    }

    pub const fn retained(&self) -> Amount {
        self.retained
    }

    /// Lends this exact account under an identity that cannot escape this operation.
    pub fn scope<R>(
        &mut self,
        run: impl for<'id> FnOnce(Ledger<'_, region::Proof<region::Identity<'id>>>) -> R,
    ) -> R {
        region::Token::scope(|token| {
            run(Ledger {
                retained: &mut self.retained,
                origin: token.proof(),
            })
        })
    }
}

impl Default for Account {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Ledger<'a, O: Origin> {
    retained: &'a mut Amount,
    origin: O,
}

impl<O: Origin> Ledger<'_, O> {
    pub const fn retained(&self) -> Amount {
        *self.retained
    }

    /// Restores the exact carrier's persisted owner under this account's identity.
    /// # Safety
    /// The value must remain in this exact account carrier since acquisition.
    pub unsafe fn bind<T: Bind<Origin = Stored>>(&self, value: T) -> T::Bound<O> {
        // SAFETY: the carrier proves exact persistent custody of this account and value.
        unsafe { value.bind(self.origin) }
    }

    /// Lends a persisted buffer without moving its storage or generated carrier.
    /// # Safety
    /// This exact account carrier must own the buffer throughout its charged lifetime.
    pub unsafe fn bind_mut<'a, S>(
        &self,
        state: &'a mut buffered::State<S>,
    ) -> buffered::Loan<'a, S, O> {
        buffered::Loan {
            state,
            _origin: self.origin,
        }
    }

    /// Returns an operation-bound owner to its exact persistent carrier.
    /// # Safety
    /// The result must remain in this exact account carrier until it is rebound.
    pub unsafe fn store<T: Bind<Origin = O>>(&self, value: T) -> T::Bound<Stored> {
        // SAFETY: the caller preserves this account's exclusive persistent custody.
        unsafe { value.bind(Stored::new()) }
    }

    pub(in crate::body) fn origin(&self) -> O {
        self.origin
    }

    pub(in crate::body) fn amount(&mut self) -> &mut Amount {
        self.retained
    }
}

/// An exclusively owned body state whose origin can be rebound by its carrier.
/// # Safety
/// Preserve all owners and obligations; replace only the invariant zero-sized proof.
pub unsafe trait Bind {
    type Origin: Origin;
    type Bound<O: Origin>;

    /// # Safety
    /// The new origin names this value's exact issuing account. Persisted values
    /// may only return to the same carrier that owns that account.
    unsafe fn bind<O: Origin>(self, origin: O) -> Self::Bound<O>;
}

const _: () = {
    assert!(size_of::<Stored>() == 0);
    assert!(size_of::<Account>() == size_of::<Amount>());
    assert!(
        size_of::<Ledger<'static, region::Proof<region::Identity<'static>>>>()
            == size_of::<&mut Amount>()
    );
    assert!(
        size_of::<buffered::Loan<'static, (), region::Proof<region::Identity<'static>>>>()
            == size_of::<&mut buffered::State<()>>()
    );
};
