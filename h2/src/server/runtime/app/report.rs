use std::{convert::Infallible, fmt, pin};

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
use sark_core::{
    decode::request,
    life,
    manifold::{
        server::app::{self, Spec},
        task::app::Runtime,
    },
};

use crate::{
    frame::numbers,
    server::{
        runtime::{
            app::{App, Failure},
            output,
        },
        task,
    },
};

pub(in crate::server) fn response<C: Copy, M, E: fmt::Debug>(
    mode: &mut M,
    exchange: life::exchange::Id<C, crate::stream::Key>,
    cause: &E,
    report: &mut impl FnMut(C, crate::stream::Key, &dyn fmt::Debug),
) where
    M: life::Mode<C, crate::stream::Key>,
{
    if M::ENABLED {
        mode.diagnostic(cause);
    } else {
        report(exchange.connection(), exchange.stream(), cause);
    }
}

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
    F: for<'a> FnMut(
        Failure<'a, 'w, 'd, W, ID, A::Failure, <App<A, W, Self> as Application<'d, ID>>::Build>,
    ),
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

impl<'w, 'd: 'w, R, W, M, D, F, A, ID: Identity + 'd>
    failures::Policy<'w, 'd, task::App<'d, ID, R, W, M, D, Self, A>, ID> for Report<F>
where
    R: Runtime<'d>,
    <R::Head as request::Head>::Start: task::works::Family,
    W: Execution + 'd,
    A: task::service::Select<R>
        + app::Timed<'d, task::service::Connection<'d, A>, conn::Id<'d, ID>>,
    task::App<'d, ID, R, W, M, D, Self, A>: Application<'d, ID, Wire = W>,
    F: for<'a> FnMut(Failure<'a, 'w, 'd, W, ID, A::Failure, task::Build<A::Build>>),
{
    fn open_failed(
        app: pin::Pin<&mut task::App<'d, ID, R, W, M, D, Self, A>>,
        cause: open::Error<'w, W>,
    ) {
        (app.project().fault.0)(Failure::Open(cause));
    }

    fn peer() -> Option<
        impl for<'a> Call<(
            pin::Pin<&'a mut task::App<'d, ID, R, W, M, D, Self, A>>,
            socket::Error,
        )>,
    > {
        None::<Infallible>
    }

    fn wire_failed<'e>(
        app: pin::Pin<&mut task::App<'d, ID, R, W, M, D, Self, A>>,
        conn: conn::Ctx<
            'e,
            '_,
            '_,
            '_,
            'w,
            'd,
            ID,
            W,
            <task::App<'d, ID, R, W, M, D, Self, A> as Application<'d, ID>>::Conn,
            <task::App<'d, ID, R, W, M, D, Self, A> as Application<'d, ID>>::Egress,
        >,
        cause: terminal::Error<'w, W>,
    ) {
        (app.project().fault.0)(Failure::Wire {
            id: conn.id(),
            cause,
        });
    }
}

pub(in crate::server) trait Fault<'d, W: wire::Spec, E, ID: Identity, B> {
    fn construction(&mut self, cause: B);

    fn output(&mut self, id: conn::Id<'d, ID>, cause: output::Failure);

    fn diagnostic(&mut self, id: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>);

    fn response(&mut self) -> impl FnMut(conn::Id<'d, ID>, crate::stream::Key, &dyn fmt::Debug);

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

    fn output(&mut self, _: conn::Id<'d, ID>, _: output::Failure) {}

    fn diagnostic(&mut self, _: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>) {
        drop(cause);
    }

    fn response(&mut self) -> impl FnMut(conn::Id<'d, ID>, crate::stream::Key, &dyn fmt::Debug) {
        |_, _, _| {}
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
    F: for<'a> FnMut(Failure<'a, 'd, 'd, W, ID, E, B>),
{
    fn construction(&mut self, cause: B) {
        (self.0)(Failure::Construction(cause));
    }

    fn output(&mut self, id: conn::Id<'d, ID>, cause: output::Failure) {
        (self.0)(Failure::Output { id, cause });
    }

    fn diagnostic(&mut self, id: conn::Id<'d, ID>, cause: app::Error<crate::conn::Error, E>) {
        (self.0)(Failure::Diagnostic { id, cause });
    }

    fn response(&mut self) -> impl FnMut(conn::Id<'d, ID>, crate::stream::Key, &dyn fmt::Debug) {
        move |id, stream, cause| (self.0)(Failure::Response { id, stream, cause })
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
