use std::{cell::Cell, ops::ControlFlow, task::Poll};

use dope::{
    core::driver::schedule::ready::wake,
    manifold::network::listener::AcceptError,
    runtime::{
        executor::{self, root, session},
        shutdown,
    },
};

use super::{ExecutionError, Output};

/// First failure retained by the enclosing listener-storage scope.
pub struct Watch<'d> {
    error: Cell<Option<AcceptError>>,
    wake: wake::store::Slot<'d>,
}

/// Keeps the first failure observable through dependent and provider retirement.
#[doc(hidden)]
#[must_use = "resolve the server result after its session has retired"]
pub struct Pending<'a, 'd, T> {
    watch: &'a Watch<'d>,
    output: Output<T>,
}

/// Sole terminal-failure waiter issued with one installed listener.
///
/// Compose listeners by polling their individual waiters. After the enclosing
/// session retires, inspect each [`Watch::error`] for failures recorded during
/// retirement, including listeners whose waiter did not finish first.
#[must_use = "observe terminal listener failure while driving its application"]
pub struct Wait<'a, 'd>(&'a Watch<'d>);

impl<'d> Watch<'d> {
    pub(super) const fn new() -> Self {
        Self {
            error: Cell::new(None),
            wake: wake::store::Slot::empty(),
        }
    }

    pub(super) fn record(&self, cause: AcceptError) {
        self.error.set(self.error.get().or(Some(cause)));
        self.wake.wake_once();
    }

    /// Returns the first failure. Read after session retirement to include late failures.
    pub fn error(&self) -> Option<AcceptError> {
        self.error.get()
    }
}

impl<'a, 'd> Wait<'a, 'd> {
    pub(super) const fn new(watch: &'a Watch<'d>) -> Self {
        Self(watch)
    }

    /// Observes terminal failure and registers this sole waiter's wake target.
    pub fn poll(&mut self, wake: wake::Waker<'d>) -> Poll<AcceptError> {
        match self.0.error() {
            Some(cause) => Poll::Ready(cause),
            None => {
                self.0.wake.set(wake);
                Poll::Pending
            }
        }
    }

    /// Drives one server and retains failures through session retirement.
    pub fn run<D, Q>(
        self,
        app: session::App<'_, '_, 'd, D, Q>,
    ) -> Pending<'a, 'd, shutdown::Requested<Q>>
    where
        D: executor::raw::App<'d>,
    {
        let watch = self.0;
        let output = match app.drive_until(self) {
            Ok(ControlFlow::Break(cause)) => Err(ExecutionError::Accept { cause, drive: None }),
            Ok(ControlFlow::Continue(requested)) => Ok(requested),
            Err(error) => Err(ExecutionError::Drive(error)),
        };
        Pending { watch, output }
    }
}

impl<T> Pending<'_, '_, T> {
    pub(super) fn finish(self) -> Output<T> {
        match (self.watch.error(), self.output) {
            (None, output) => output,
            (Some(cause), Ok(_)) => Err(ExecutionError::Accept { cause, drive: None }),
            (Some(cause), Err(ExecutionError::Drive(drive))) => Err(ExecutionError::Accept {
                cause,
                drive: Some(drive),
            }),
            (Some(cause), Err(ExecutionError::Accept { drive, .. })) => {
                Err(ExecutionError::Accept { cause, drive })
            }
        }
    }
}

impl<'d> root::Root<'d> for Wait<'_, 'd> {
    type Output = AcceptError;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, target, _, _) = context.into_parts();
        root.get_mut().poll(wake::Waker::from(target))
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        self.0.wake.clear();
    }
}
