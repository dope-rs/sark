use std::ops::ControlFlow;

use dope::runtime::{
    executor::{self, session},
    shutdown,
};

use super::{ExecutionError, Output, Wait, Watch};

/// Keeps the first failure observable through dependent and provider retirement.
#[doc(hidden)]
#[must_use = "resolve the server result after its session has retired"]
pub struct Pending<'a, 'd, T> {
    watch: &'a Watch<'d>,
    output: Output<T>,
}

/// Drives one server and retains failures through session retirement.
pub fn run<'a, 'd, D, Q>(
    wait: Wait<'a, 'd>,
    app: session::App<'_, '_, 'd, D, Q>,
) -> Pending<'a, 'd, shutdown::Requested<Q>>
where
    D: executor::raw::App<'d>,
    Q: executor::Startup,
{
    let watch = wait.watch();
    let output = match app.drive_until(wait) {
        Ok(ControlFlow::Break(cause)) => Err(ExecutionError::Accept { cause, drive: None }),
        Ok(ControlFlow::Continue(requested)) => Ok(requested),
        Err(error) => Err(ExecutionError::Drive(error)),
    };
    Pending { watch, output }
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
