use std::{convert::Infallible, pin};

use dope::{
    core::{driver::route::Identity, io::socket},
    manifold::{
        app::receive,
        network::listener::{
            conn,
            handler::{Application, failures},
        },
    },
    net::{
        link::pool::recv::input::Execution,
        wire,
        wire::{open, terminal},
    },
};
use o3::base::Call;
use sark_core::manifold::server::app::{self, Spec};

use crate::{
    frame::numbers,
    server::runtime::{
        app::{App, Failure},
        output::{self, track::Progress},
    },
};

/// Owns the final consumer of failures from the selected wire.
#[repr(transparent)]
pub struct Report<F>(F);

impl<F> Report<F> {
    pub const fn new(report: F) -> Self {
        Self(report)
    }
}

impl<'w, 'd: 'w, A, W, F, ID: Identity> failures::Policy<'w, 'd, App<A, W, Self>, ID> for Report<F>
where
    A: Spec,
    App<A, W, Self>: Application<'d, ID, Wire = W>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    F: FnMut(Failure<'w, 'd, W, ID, A::Failure, <App<A, W, Self> as Application<'d, ID>>::Build>),
{
    fn open_failed(app: pin::Pin<&mut App<A, W, Self>>, cause: open::Error<'w, W>) {
        (app.get_mut().fault.0)(Failure::Open(cause));
    }

    /// A peer address that failed validation is a platform fault raised before
    /// this report's connection classes exist, so the accepted socket closes
    /// without an application-visible cause.
    fn peer() -> Option<impl for<'a> Call<(pin::Pin<&'a mut App<A, W, Self>>, socket::Error)>> {
        None::<Infallible>
    }

    fn wire_failed<'e>(
        app: pin::Pin<&mut App<A, W, Self>>,
        conn: conn::Ctx<
            'e,
            '_,
            '_,
            '_,
            'w,
            'd,
            ID,
            W,
            <App<A, W, Self> as Application<'d, ID>>::Conn,
            <App<A, W, Self> as Application<'d, ID>>::Egress,
        >,
        cause: terminal::Error<'w, W>,
    ) {
        (app.get_mut().fault.0)(Failure::Wire {
            id: conn.id(),
            cause,
        });
    }
}

pub(in crate::server::runtime) trait Fault<'d, W: wire::Spec, E, ID: Identity, B> {
    fn construction(&mut self, cause: B);

    fn output(
        &mut self,
        id: conn::Id<'d, ID>,
        result: Result<Progress, (output::Failure, Progress)>,
    ) -> Progress;

    fn diagnostic(&mut self, id: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>);

    fn failed(
        &mut self,
        id: conn::Id<'d, ID>,
        cause: app::Error<crate::conn::Error, E>,
    ) -> numbers::ErrorCode;
}

impl<'d, W: wire::Spec, E, ID: Identity, B> Fault<'d, W, E, ID, B> for failures::Close {
    fn construction(&mut self, cause: B) {
        drop(cause);
    }

    fn output(
        &mut self,
        _: conn::Id<'d, ID>,
        result: Result<Progress, (output::Failure, Progress)>,
    ) -> Progress {
        match result {
            Ok(progress) => progress,
            Err((_, progress)) => progress,
        }
    }

    fn diagnostic(&mut self, _: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>) {
        drop(cause);
    }

    fn failed(
        &mut self,
        _: conn::Id<'d, ID>,
        cause: app::Error<crate::conn::Error, E>,
    ) -> numbers::ErrorCode {
        match cause {
            app::Error::Connection(cause) => cause.code(),
            app::Error::Application(_) => numbers::ErrorCode::InternalError,
        }
    }
}

impl<'d, W, E, F, ID: Identity, B> Fault<'d, W, E, ID, B> for Report<F>
where
    W: wire::Spec + 'd,
    F: FnMut(Failure<'d, 'd, W, ID, E, B>),
{
    fn construction(&mut self, cause: B) {
        (self.0)(Failure::Construction(cause));
    }

    fn output(
        &mut self,
        id: conn::Id<'d, ID>,
        result: Result<Progress, (output::Failure, Progress)>,
    ) -> Progress {
        match result {
            Ok(progress) => progress,
            Err((cause, progress)) => {
                (self.0)(Failure::Output { id, cause });
                progress
            }
        }
    }

    fn diagnostic(&mut self, id: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>) {
        (self.0)(Failure::Diagnostic { id, cause });
    }

    fn failed(
        &mut self,
        id: conn::Id<'d, ID>,
        cause: app::Error<crate::conn::Error, E>,
    ) -> numbers::ErrorCode {
        let code = match &cause {
            app::Error::Connection(cause) => numbers::ErrorCode::from(cause),
            app::Error::Application(_) => numbers::ErrorCode::InternalError,
        };
        (self.0)(Failure::Drive { id, cause });
        code
    }
}
