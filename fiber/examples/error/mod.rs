use std::{
    convert, fmt,
    fs::File,
    io::{self, Write as _},
    os::fd::AsFd as _,
    process::{ExitCode, Termination},
};

use ::dope::core::platform::affinity;
use dope::runtime::process::failure::{create, execute};

use crate::input;

pub(super) enum Error<'signal, C, W> {
    Config(C),
    Input(input::Error),
    Output(io::Error),
    Affinity(affinity::Error),
    Create(create::Error),
    Run(execute::Error<'signal, convert::Infallible, W>),
}

impl<C, W> From<C> for Error<'_, C, W> {
    fn from(error: C) -> Self {
        Self::Config(error)
    }
}

impl<C: fmt::Debug, W: fmt::Debug> Error<'_, C, W> {
    fn try_report(self) -> io::Result<ExitCode> {
        match self {
            Self::Config(error) => write(format_args!("configuration: {error:?}"))?,
            Self::Input(error) => {
                error.print()?;
                return Ok(ExitCode::from(error.exit_code() as u8));
            }
            Self::Output(error) => write(format_args!("output: {error}"))?,
            Self::Affinity(error) => write(format_args!("CPU affinity: {error:?}"))?,
            Self::Create(error) => write(format_args!("process creation: {error:?}"))?,
            Self::Run(error) => write(format_args!("process: {error:?}"))?,
        }
        Ok(ExitCode::FAILURE)
    }
}

impl<C: fmt::Debug, W: fmt::Debug> Termination for Error<'_, C, W> {
    fn report(self) -> ExitCode {
        self.try_report().unwrap_or(ExitCode::FAILURE)
    }
}

pub(super) fn write(message: fmt::Arguments<'_>) -> io::Result<()> {
    let mut stderr = File::from(io::stderr().as_fd().try_clone_to_owned()?);
    writeln!(stderr, "{message}")
}
