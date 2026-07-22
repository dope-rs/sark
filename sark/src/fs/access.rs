use o3::cell::RawCell;

#[cfg(debug_assertions)]
use std::cell::Cell;

/// Interior state confined to one `ServeDir` owner and one runtime thread.
///
/// Every call site is private to `fs`, completes synchronously, and cannot call
/// back into the same cell. `RawCell` supplies thread confinement; the debug
/// guard turns a future violation of the non-reentrancy proof into a panic.
/// Release builds retain exactly the unchecked cell access and no borrow flag.
pub(super) struct ProofCell<T> {
    inner: RawCell<T>,
    #[cfg(debug_assertions)]
    active: Cell<bool>,
}

#[cfg(not(debug_assertions))]
const _: () =
    assert!(std::mem::size_of::<ProofCell<usize>>() == std::mem::size_of::<RawCell<usize>>());

impl<T> ProofCell<T> {
    pub(super) const fn new(value: T) -> Self {
        Self {
            inner: RawCell::new(value),
            #[cfg(debug_assertions)]
            active: Cell::new(false),
        }
    }

    #[inline]
    pub(super) fn with_mut<R>(&self, operation: impl FnOnce(&mut T) -> R) -> R {
        #[cfg(debug_assertions)]
        let _access = Access::enter(&self.active);

        // SAFETY: ProofCell is thread-confined by RawCell. Its fs-only callers
        // are synchronous and non-reentrant; the debug guard continuously
        // checks the latter invariant in tests and development builds.
        unsafe { self.inner.with_mut(operation) }
    }
}

#[cfg(debug_assertions)]
struct Access<'a>(&'a Cell<bool>);

#[cfg(debug_assertions)]
impl<'a> Access<'a> {
    fn enter(active: &'a Cell<bool>) -> Self {
        assert!(!active.replace(true), "reentrant fs state access");
        Self(active)
    }
}

#[cfg(debug_assertions)]
impl Drop for Access<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::ProofCell;

    #[test]
    #[should_panic(expected = "reentrant fs state access")]
    fn proof_cell_rejects_reentrant_mutation() {
        let cell = ProofCell::new(0);
        cell.with_mut(|_| cell.with_mut(|_| {}));
    }
}
