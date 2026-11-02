use o3::buffer::hold;
use sark_core::life::session;

use crate::{
    conn::{event, outputs, outputs::payloads},
    flow, stream,
};

pub(crate) struct Retained<'a, C: session::Storage> {
    outbound: &'a mut outputs::Outbound<C>,
}

impl<'a, C: session::Storage> Retained<'a, C> {
    pub(super) fn new(outbound: &'a mut outputs::Outbound<C>) -> Self {
        Self { outbound }
    }
    pub(crate) fn clear(&mut self, mut release: impl FnMut(event::Staging)) {
        self.outbound.payloads.clear(|payload| match payload {
            payloads::Shared::Wire(_) => {}
            payloads::Shared::Data(data) => {
                let body_len = data.body.as_ref().map_or(0, |body| body.as_ref().len());
                let resident = data.body.as_ref().map_or(0, |body| body.resident());
                drop(data.body);
                release(event::Staging {
                    charge: hold::Charge::from_owner(body_len, resident),
                    credit: data.credit,
                });
            }
        });
    }

    /// Drains a subset of the ledger's admitted byte totals and bounded record count.
    pub(crate) fn discard(
        &mut self,
        id: stream::Id,
        mut cancel: impl FnMut(flow::Reserved),
    ) -> Option<event::Retirement> {
        let mut retired: Option<event::Retirement> = None;
        self.drain(id, |data| {
            let body_len = data.body.as_ref().map_or(0, |body| body.as_ref().len());
            let resident = data.body.as_ref().map_or(0, |body| body.resident());
            let released = hold::Charge::from_owner(body_len, resident);
            let charge = match &retired {
                Some(event::Retirement { charge: total, .. }) => hold::Charge::from_commit(
                    total.logical_bytes.wrapping_add(released.logical_bytes),
                    total.resident_bytes.wrapping_add(released.resident_bytes),
                    total.owners.wrapping_add(released.owners),
                ),
                None => released,
            };
            retired = Some(event::Retirement {
                stream_id: data.credit.stream(),
                charge,
            });
            cancel(data.credit);
        });
        retired
    }

    pub(crate) fn cancel(&mut self, id: stream::Id, mut cancel: impl FnMut(flow::Reserved)) {
        self.drain(id, |data| cancel(data.credit))
    }

    fn drain(&mut self, id: stream::Id, mut release: impl FnMut(payloads::Data<Option<C>>)) {
        self.outbound.payloads.drain(id, |payload| {
            let payloads::Shared::Data(data) = payload else {
                return;
            };
            release(data);
        });
    }
}
