use std::num::NonZero;

use o3::{buffer::resident, depot::queue::indexed::keyed};
use sark_core::{
    body::{self, credit},
    decode::{request, state},
};

use crate::{
    conn::{Error, event},
    server::{
        self, payload,
        state::exchange,
        task::{
            Built,
            ingress::{
                Buffered, Data, Occupied, Receiving, Turn,
                entry::{self, Entry},
                errors,
            },
        },
    },
    stream::Key,
};

/// Admits the selected Book's buffered request without erasing its source contract.
/// # Safety
/// `request` must have remained in this Data owner's Book since credit acquisition.
pub(in crate::server::task) unsafe fn try_buffer<'d, P, F, B, E>(
    data: Data<'_, 'd, P, F, B>,
    stream: Key,
    request: Buffered<'d, P>,
    evidence: E,
) -> Result<(), Error>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
    E: credit::Evidence,
{
    let vacant = match data.entries.try_vacant(stream) {
        Ok(vacant) => vacant,
        Err(error) => {
            let release = data.credit.scope(|mut ledger| {
                // SAFETY: the caller retains this request in its exact Book.
                unsafe { exchange::raw::Closing::new(request.request, &mut ledger) }
                    .discard::<Receiving<'d>, _>(evidence)
            });
            return Err(errors::reject(error, release));
        }
    };
    vacant.insert(Entry::Buffered(request));
    Ok(())
}

/// Drives the native selected entry with the roots that admitted it.
/// # Safety
/// The entry, build FIFO, and account must come from the same exclusive Book.
pub(in crate::server::task) unsafe fn drive_entry<'a, 'd, P, F, B, E>(
    occupied: Occupied<'a, 'd, P, F, B>,
    queue: &'a mut keyed::Fifo<Key, entry::Build>,
    credit: &mut credit::raw::Account,
    budget: NonZero<usize>,
    evidence: E,
) -> Built<'d, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
    E: credit::Evidence,
{
    let Some(member) = Turn::try_new(occupied, queue) else {
        return body::BuildOutcome::Idle;
    };
    member.drive(credit, budget, evidence)
}

/// One selected buffered request and its issuing account for this operation.
pub(super) struct Buffering<'a, 'ledger, 'd, P: request::Prepared, O: credit::Origin> {
    buffered: &'a mut Buffered<'d, P>,
    ledger: &'a mut credit::raw::Ledger<'ledger, O>,
}

impl<'a, 'ledger, 'd, P: request::Prepared, O: credit::Origin> Buffering<'a, 'ledger, 'd, P, O> {
    /// # Safety
    /// The selected request and ledger belong to the same exclusive Book, which
    /// has retained custody of every charged body since its creation.
    pub(super) unsafe fn new(
        buffered: &'a mut Buffered<'d, P>,
        ledger: &'a mut credit::raw::Ledger<'ledger, O>,
    ) -> Self {
        Self { buffered, ledger }
    }

    pub(super) fn try_push<E: credit::Evidence>(
        self,
        evidence: E,
        data: event::DataPayload<resident::Shared<'d>>,
        limits: server::Config,
    ) -> payload::Push<event::DataPayload<resident::Shared<'d>>> {
        let body = state::Flow::open_mut(&mut self.buffered.request);
        // SAFETY: Buffering holds the selected request and its issuing ledger.
        let mut body = unsafe { payload::raw::Loan::new(body, self.ledger) };
        let request = limits.request(body.max_body());
        body.try_push(self.ledger, evidence, data, request, limits.connection())
    }
}
