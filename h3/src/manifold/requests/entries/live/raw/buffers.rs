use o3::buffer::{segments, segments::assembly};
use sark_core::{
    body::{buffered, credit, input},
    decode::{
        request,
        state::{self, Slot, open},
    },
};
use sark_http::payload::{owned, response::Body as _};

use crate::{
    Payload,
    manifold::requests::{
        self,
        entries::buffered::{Buffered, Finished, Receiving},
        exchange,
    },
};

/// # Safety
/// `body` is the state held by the exact slot whose account supplied `origin`.
pub(super) unsafe fn try_finish<
    'd,
    P: request::Prepared,
    E: credit::Evidence,
    O: credit::Origin,
>(
    body: Buffered<'d, P>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    evidence: E,
    origin: O,
) -> Result<Finished<'d, P>, exchange::Error> {
    state::Flow::fold_open(body.request, Finish::new(ledger, evidence, origin))
}

/// # Safety
/// `body` remains under the exact selected slot and its issuing account loan.
pub(super) unsafe fn try_push<'d, P: request::Prepared, E: credit::Evidence, O: credit::Origin>(
    body: &mut Buffered<'d, P>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    evidence: E,
    input: Payload<'d>,
    request: credit::Limits,
    conn: credit::Limits,
) -> Result<(), requests::Error> {
    let bytes = input.len();
    let chunks = input.count();
    let pushed = state::Flow::each_open(
        &mut body.request,
        Feed {
            ledger,
            evidence,
            input,
            bytes,
            chunks,
            request,
            conn,
        },
    );
    match pushed {
        buffered::Push::Accepted | buffered::Push::Discarded(_) => Ok(()),
        buffered::Push::Rejected { error, .. } => Err(requests::Error::Payload(error)),
    }
}

/// # Safety
/// `body` is removed from the exact slot whose account supplied `origin`.
pub(super) unsafe fn try_discard<
    'd,
    P: request::Prepared,
    E: credit::Evidence,
    O: credit::Origin,
>(
    body: Buffered<'d, P>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    mut evidence: E,
    origin: O,
) -> Result<(), requests::Error> {
    state::Flow::consume(body.request, |body| {
        // SAFETY: the caller retains this body and its exact issuing account.
        let body = unsafe { credit::raw::Bind::bind(body, origin) };
        body.discard(ledger, &mut evidence)
            .abandon(evidence)
            .map_err(requests::Error::from)
    })
}

struct Feed<'a, 'account, 'd, E, O: credit::Origin> {
    ledger: &'a mut credit::raw::Ledger<'account, O>,
    evidence: E,
    input: Payload<'d>,
    bytes: usize,
    chunks: segments::Count,
    request: credit::Limits,
    conn: credit::Limits,
}

impl<'d, E: credit::Evidence, O: credit::Origin> open::Each<Receiving<'d>>
    for Feed<'_, '_, 'd, E, O>
{
    type Output<'a> = buffered::Push<Payload<'d>, assembly::Error>;

    fn apply<'a>(self, body: &'a mut buffered::State<owned::Segments<'d>>) -> Self::Output<'a> {
        let Self {
            ledger,
            evidence,
            input,
            bytes,
            chunks,
            request,
            conn,
        } = self;
        // SAFETY: Feed is created only by the buffered operation whose caller
        // holds the exact selected slot and its issuing account proof.
        let mut body = unsafe { ledger.bind_mut(body) };
        body.try_push(
            ledger,
            evidence,
            input::Input::new(input, bytes, chunks),
            request,
            conn,
            |current, input| {
                let incoming = input.into_body();
                match current {
                    Some(current) => match current.try_extend(incoming) {
                        Ok(body) => buffered::Retain::Accepted {
                            storage: Some(body),
                        },
                        Err(rejected) => {
                            let ((body, incoming), error) = rejected.into_parts();
                            buffered::Retain::Rejected {
                                storage: Some(body),
                                input: Payload::from_body(incoming),
                                error,
                            }
                        }
                    },
                    None => buffered::Retain::Accepted {
                        storage: Some(incoming),
                    },
                }
            },
        )
    }
}

struct Finish<'a, 'account, E, O: credit::Origin> {
    ledger: &'a mut credit::raw::Ledger<'account, O>,
    evidence: E,
    origin: O,
}

impl<'a, 'account, E, O: credit::Origin> Finish<'a, 'account, E, O> {
    const fn new(ledger: &'a mut credit::raw::Ledger<'account, O>, evidence: E, origin: O) -> Self {
        Self {
            ledger,
            evidence,
            origin,
        }
    }

    fn try_finish<'d, K, P, S>(
        mut self,
        slot: S,
        body: buffered::State<owned::Segments<'d>>,
    ) -> Result<request::Ready<K, P, owned::Segments<'d>>, exchange::Error>
    where
        E: credit::Evidence,
        S: Slot<P>,
    {
        // SAFETY: Finish is created only inside the selected slot's exact
        // account scope, and consumes that slot's buffered body.
        let body = unsafe { credit::raw::Bind::bind(body, self.origin) };
        match slot
            .admit::<K, _>(body)
            .try_map_body(|body| body.try_finish(self.ledger, &mut self.evidence))
        {
            Ok(request) => Ok(request.ready()),
            Err(failure) => {
                let error = *failure.error().error();
                match failure
                    .discard()
                    .try_release(self.ledger, &mut self.evidence)
                {
                    Ok(_) => Err(exchange::Error::Request(error.into())),
                    Err(rejected) => Err(exchange::Error::Release {
                        body: error,
                        release: *rejected.error(),
                    }),
                }
            }
        }
    }
}

impl<'d, P, E: credit::Evidence, O: credit::Origin> open::Fold<P, Receiving<'d>>
    for Finish<'_, '_, E, O>
{
    type Output = Result<Finished<'d, P>, exchange::Error>;

    fn sync<S: Slot<P>>(self, slot: S, body: buffered::State<owned::Segments<'d>>) -> Self::Output {
        self.try_finish(slot, body).map(Finished::Sync)
    }

    fn task<S: Slot<P>>(self, slot: S, body: buffered::State<owned::Segments<'d>>) -> Self::Output {
        self.try_finish(slot, body).map(Finished::Task)
    }
}
