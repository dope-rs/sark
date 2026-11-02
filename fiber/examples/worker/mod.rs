use std::{cell::Cell, convert::Infallible, fmt, io};

use dope::{
    core::{BranchError, driver::life::recovery, io::Reclaimed},
    runtime::topology,
};

use crate::error;

/// Native failure with its first output error, or output failure after a successful run.
pub(super) type Error = BranchError<Option<io::Error>, io::Error>;

pub(super) fn finish_run<S: fmt::Debug, I: fmt::Debug, O, T, Q>(
    result: Result<T, topology::RunError<'_, S, I, O, T, Q>>,
    output: &Cell<Option<io::Error>>,
) -> Result<T, Error> {
    match result {
        Ok(value) => match output.take() {
            Some(error) => Err(BranchError::Second(error)),
            None => Ok(value),
        },
        Err(error) => {
            report(output, format_args!("worker topology: {:?}", error.cause()));
            Err(BranchError::First(output.take()))
        }
    }
}

pub(super) fn observe(
    output: &Cell<Option<io::Error>>,
) -> recovery::Policy<impl FnMut(Reclaimed) + '_, impl recovery::Recover + '_> {
    recovery::Policy::new(
        move |completion| report(output, format_args!("reclaimed completion: {completion:?}")),
        move |failure: recovery::Recovery<'_, '_>| recover(output, failure),
    )
}

fn report(output: &Cell<Option<io::Error>>, message: fmt::Arguments<'_>) {
    if let Err(error) = error::write(message) {
        output.set(output.take().or(Some(error)));
    }
}

fn recover(output: &Cell<Option<io::Error>>, failure: recovery::Recovery<'_, '_>) -> Infallible {
    report(
        output,
        format_args!("worker native recovery failed: {:?}", failure.cause()),
    );
    failure.abort()
}
