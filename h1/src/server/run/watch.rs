use dope::runtime::{
    executor::{self, session},
    shutdown,
    topology::lending,
};

use super::{Failure, Output, Wait, Watch};

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
    let output = wait.run(app);
    Pending { watch, output }
}

/// Resolves the listener result after the session has retired its owners.
pub fn session<'report, I, T>(
    result: session::Result<'report, Pending<'_, '_, T>>,
) -> Result<T, Failure<'report, I, T>> {
    match result {
        Ok(Pending { watch, output }) => watch.finish(Ok(output)),
        Err(session::Error::Quiesce {
            output: Pending { watch, output },
            drive,
            disarm,
        }) => watch.finish(Err(session::Error::Quiesce {
            output,
            drive,
            disarm,
        })),
        Err(session::Error::Drive(error)) => Err(Failure::Run(session::Error::Drive(error))),
        Err(session::Error::Install { cause, disarm }) => {
            Err(Failure::Run(session::Error::Install { cause, disarm }))
        }
    }
}

/// Resolves a dependent server only after its provider has also quiesced.
pub fn lending<'report, I, T>(
    result: lending::Result<'report, I, Pending<'_, '_, T>>,
) -> Result<T, Failure<'report, I, T>> {
    match result {
        Ok(output) => session(Ok(output)),
        Err(lending::Error::Build(error)) => Err(Failure::Install(error)),
        Err(lending::Error::Run(error)) => session(Err(error)),
    }
}
