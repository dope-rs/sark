use core::{convert::Infallible, ops::ControlFlow};

use crate::decode::{self, Encode, dispatch, request, state};

mod stop;
pub use stop::Stop;

/// Consumes the selected route's admission contract without erasing its guard capability.
pub trait Start<R: decode::Decode>: Sized {
    type Stop<T, E>: Stop<T, E>;

    fn admit<'req, 'd: 'req, E: Encode<'req, 'd>, T, F>(
        self,
        router: &R,
        encode: impl FnOnce() -> E,
        stop: impl FnOnce(Encoded<'req, 'd, E>) -> Result<T, F>,
    ) -> ControlFlow<Self::Stop<T, F>, Admitted<R::Prepared>>;
}

impl<R, U> Start<R> for request::Plain<U>
where
    R: decode::Decode,
    U: state::Admission<R::Prepared, state::Routed, state::Admitted,
        Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<state::Admitted>>,
{
    type Stop<T, E> = Infallible;

    fn admit<'req, 'd: 'req, E: Encode<'req, 'd>, T, F>(
        self,
        _: &R,
        _: impl FnOnce() -> E,
        _: impl FnOnce(Encoded<'req, 'd, E>) -> Result<T, F>,
    ) -> ControlFlow<Infallible,
        <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<state::Admitted>,
    > {
        state::Admission::admit(self.into_state(), &mut state::Pass)
    }
}

impl<R: decode::Guarded> Start<R>
    for request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>
{
    type Stop<T, E> = Result<T, E>;

    fn admit<'req, 'd: 'req, E: Encode<'req, 'd>, T, F>(
        self,
        router: &R,
        encode: impl FnOnce() -> E,
        stop: impl FnOnce(Encoded<'req, 'd, E>) -> Result<T, F>,
    ) -> ControlFlow<Result<T, F>, Admitted<R::Prepared>> {
        decode::Guarded::try_admit(router, self, encode(), &mut state::Pass).map_break(stop)
    }
}

impl<R, U> Start<R>
    for request::Start<U, <R::Routes as state::Family<R::Prepared>>::State<state::Routed>>
where
    R: decode::Guarded,
    U: state::Admission<R::Prepared, state::Routed, state::Admitted,
        Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<state::Admitted>>,
{
    type Stop<T, E> = Result<T, E>;

    fn admit<'req, 'd: 'req, E: Encode<'req, 'd>, T, F>(
        self,
        router: &R,
        encode: impl FnOnce() -> E,
        stop: impl FnOnce(Encoded<'req, 'd, E>) -> Result<T, F>,
    ) -> ControlFlow<
        Result<T, F>,
        <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<state::Admitted>,
    > {
        match self {
            Self::Plain(request) => match request.admit(router, encode, stop) {
                ControlFlow::Continue(request) => ControlFlow::Continue(request),
                ControlFlow::Break(never) => match never {},
            },
            Self::Guarded(request) => request.admit(router, encode, stop),
        }
    }
}

type Admitted<P> = <<P as request::Prepared>::Family as state::Family<P>>::State<state::Admitted>;

type Encoded<'req, 'd, E> = Result<
    <E as Encode<'req, 'd>>::Prepared,
    (
        dispatch::Error<<E as Encode<'req, 'd>>::Error, Infallible, Infallible>,
        <E as decode::Cancel>::Target,
    ),
>;
