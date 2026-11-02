use o3::buffer::{resident, segments};
use sark_core::{
    body::{Error, credit},
    decode::{
        request,
        state::{self, body},
    },
};

use crate::{
    conn::event,
    server::{
        self, payload,
        payload::Buffer,
        state::exchange::{self, Input, Pending, Rejected},
    },
};

/// An exact carrier's routed request and the exclusive account it charges.
pub(crate) struct Route<'a, 'c, 'd, P, F, O>
where
    P: request::Prepared,
    F: state::Family<P>,
    O: credit::Origin,
{
    route: &'a mut exchange::Routed<'d, P, F>,
    ledger: &'a mut credit::raw::Ledger<'c, O>,
}

impl<'a, 'c, 'd, P, F, O> Route<'a, 'c, 'd, P, F, O>
where
    P: request::Prepared,
    F: state::Family<P>,
    O: credit::Origin,
{
    /// # Safety
    /// The selected Book must exclusively own this request and this ledger's
    /// account throughout every contained buffer's charged lifetime.
    pub(crate) unsafe fn new(
        route: &'a mut exchange::Routed<'d, P, F>,
        ledger: &'a mut credit::raw::Ledger<'c, O>,
    ) -> Self {
        Self { route, ledger }
    }

    pub(crate) fn try_push<E: credit::Evidence>(
        self,
        evidence: E,
        data: event::DataPayload<resident::Shared<'d>>,
        limits: server::Config,
    ) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        state::Flow::each(
            self.route.request.state_mut(),
            Push {
                ledger: self.ledger,
                evidence,
                data,
                limits,
            },
        )
    }
}

/// One selected request kept with its original account until its body is closed.
pub(crate) struct Closing<'a, 'c, T, O: credit::Origin> {
    request: T,
    ledger: &'a mut credit::raw::Ledger<'c, O>,
}

impl<'a, 'c, T, O: credit::Origin> Closing<'a, 'c, T, O> {
    /// # Safety
    /// The exclusive carrier must have retained this request and this account
    /// together throughout the contained body's charged lifetime.
    pub(crate) unsafe fn new(request: T, ledger: &'a mut credit::raw::Ledger<'c, O>) -> Self {
        Self { request, ledger }
    }

    pub(crate) fn discard<'d, F, E>(self, evidence: E) -> Result<(), payload::Error>
    where
        F: Input<'d>,
        T: body::Flow<F>,
        E: credit::Evidence,
    {
        body::Flow::fold(
            self.request,
            Close {
                ledger: self.ledger,
                evidence,
            },
        )
    }
}

/// A routed request consumed under its selected Book's original account.
pub(crate) struct Routed<'a, 'c, 'd, P, F, O>
where
    P: request::Prepared,
    F: state::Family<P>,
    O: credit::Origin,
{
    route: exchange::Routed<'d, P, F>,
    ledger: &'a mut credit::raw::Ledger<'c, O>,
}

impl<'a, 'c, 'd, P, F, O> Routed<'a, 'c, 'd, P, F, O>
where
    P: request::Prepared,
    F: state::Family<P>,
    O: credit::Origin,
{
    /// # Safety
    /// The selected Book must have exclusively retained this request and this
    /// ledger's account throughout every contained buffer's charged lifetime.
    pub(crate) unsafe fn new(
        route: exchange::Routed<'d, P, F>,
        ledger: &'a mut credit::raw::Ledger<'c, O>,
    ) -> Self {
        Self { route, ledger }
    }

    pub(crate) fn discard<E: credit::Evidence>(self, evidence: E) -> Result<(), payload::Error> {
        self.route.into_state().fold(|request| {
            Closing {
                request,
                ledger: self.ledger,
            }
            .discard::<Pending<'d>, _>(evidence)
        })
    }
}

struct Close<'a, 'c, E, O: credit::Origin> {
    ledger: &'a mut credit::raw::Ledger<'c, O>,
    evidence: E,
}

impl<'d, F, E, O> body::Fold<F> for Close<'_, '_, E, O>
where
    F: Input<'d>,
    E: credit::Evidence,
    O: credit::Origin,
{
    type Output = Result<(), payload::Error>;

    fn apply(self, value: body::Value<F>) -> Self::Output {
        let Self { ledger, evidence } = self;
        let body = match value {
            body::Value::SyncOpen(body) => F::sync(body),
            body::Value::SyncFinished(body) => F::sync_done(body),
            body::Value::TaskOpen(body) => F::task(body),
            body::Value::TaskFinished(body) => F::task_done(body),
            body::Value::UnsupportedOpen(body) => F::other(body),
            body::Value::UnsupportedFinished(body) => F::other_done(body),
            body::Value::DuplexOpen(body) => {
                F::close_duplex(body);
                return Ok(());
            }
            body::Value::DuplexFinished(body) => {
                F::end_duplex(body);
                return Ok(());
            }
        };
        // SAFETY: Closing's selected-carrier invariant covers this sole body.
        unsafe { ledger.bind(body) }.discard(ledger, evidence)
    }
}

struct Push<'a, 'c, 'd, E, O: credit::Origin> {
    ledger: &'a mut credit::raw::Ledger<'c, O>,
    evidence: E,
    data: event::DataPayload<resident::Shared<'d>>,
    limits: server::Config,
}

impl<'d, E: credit::Evidence, O: credit::Origin> Push<'_, '_, 'd, E, O> {
    fn body(
        self,
        body: &mut Buffer<'d>,
    ) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        let Self {
            ledger,
            evidence,
            data,
            limits,
        } = self;
        // SAFETY: Route retains this exact request and its selected Book account;
        // Each lends the existing body without moving or rebuilding the Family.
        let mut body = unsafe { payload::raw::Loan::new(body, ledger) };
        let request = limits.request(body.max_body());
        body.try_push(ledger, evidence, data, request, limits.connection())
    }

    fn duplex(
        self,
        body: &mut Option<segments::Chain<resident::Shared<'d>>>,
    ) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        drop(self.evidence);
        match self.data.try_append(body) {
            Ok(()) => payload::Push::Accepted,
            Err(rejected) => {
                let (input, error) = rejected.into_parts();
                payload::Push::Rejected {
                    input,
                    error: payload::Error::Segments(error),
                }
            }
        }
    }

    fn reject(self) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        drop(self.evidence);
        payload::Push::Rejected {
            input: self.data,
            error: payload::Error::Body(Error::InputFinished),
        }
    }
}

impl<'d, P, E, O> state::Each<P, Pending<'d>> for Push<'_, '_, 'd, E, O>
where
    E: credit::Evidence,
    O: credit::Origin,
{
    type Output<'a> = payload::Push<event::DataPayload<resident::Shared<'d>>>;

    fn sync_open<'a, S: state::Slot<P>>(self, _: &S, body: &'a mut Buffer<'d>) -> Self::Output<'a> {
        self.body(body)
    }

    fn sync_done<'a, S: state::Slot<P>>(self, _: &S, _: &'a mut Buffer<'d>) -> Self::Output<'a> {
        self.reject()
    }

    fn task_open<'a, S: state::Slot<P>>(self, _: &S, body: &'a mut Buffer<'d>) -> Self::Output<'a> {
        self.body(body)
    }

    fn task_done<'a, S: state::Slot<P>>(self, _: &S, _: &'a mut Buffer<'d>) -> Self::Output<'a> {
        self.reject()
    }

    fn duplex_open<'a, S: state::Slot<P>>(
        self,
        _: &S,
        body: &'a mut Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Self::Output<'a> {
        self.duplex(body)
    }

    fn duplex_done<'a, S: state::Slot<P>>(
        self,
        _: &S,
        _: &'a mut Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Self::Output<'a> {
        self.reject()
    }

    fn reject_open<'a, S: state::Slot<P>>(
        self,
        _: &S,
        body: &'a mut Buffer<'d>,
    ) -> Self::Output<'a> {
        self.body(body)
    }

    fn reject_done<'a, S: state::Slot<P>>(self, _: &S, _: &'a mut Buffer<'d>) -> Self::Output<'a> {
        self.reject()
    }
}

// SAFETY: the rejection keeps exactly one unchanged Buffer or an uncharged duplex
// marker. Delegating the body binding preserves storage and every obligation.
unsafe impl<'d, O: credit::Origin> credit::raw::Bind for Rejected<'d, O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = Rejected<'d, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        match self {
            Self::Body(body) => {
                // SAFETY: the selected carrier establishes this body's account.
                Rejected::Body(unsafe { credit::raw::Bind::bind(body, origin) })
            }
            Self::Duplex => Rejected::Duplex,
        }
    }
}
