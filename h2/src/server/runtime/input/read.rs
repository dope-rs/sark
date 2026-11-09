use dope::net::wire;
use o3::buffer::resident;
use sark_core::manifold::server::drain::Progress;
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    server::runtime::input::{Fault, Turn},
};

pub(in crate::server) struct Read<'a, T, P, D> {
    context: &'a mut T,
    protocol: P,
    events: D,
}

impl<'a, 'd, T, P, D, X, F, E> Read<'a, T, P, D>
where
    F: rules::Policy,
    E: event::Evidence,
    P: for<'step> FnMut(
        &'step mut T,
    ) -> &'step mut machine::Connection<
        crate::ServerRole,
        F,
        resident::Shared<'d>,
        rules::RejectPush,
        E,
    >,
    D: FnMut(&mut T, bool) -> Result<Progress, X>,
    X: Fault,
{
    pub(in crate::server) fn new(context: &'a mut T, protocol: P, events: D) -> Self {
        Self {
            context,
            protocol,
            events,
        }
    }

    pub(in crate::server) fn read<'w, W>(
        &mut self,
        source: &mut Turn<'_, 'w, 'd, W>,
    ) -> Result<Progress, X>
    where
        'd: 'w,
        W: wire::Spec,
    {
        let work = {
            let conn = (self.protocol)(self.context);
            if conn.status().is_closing() {
                source.discard();
                return Ok(Progress::default());
            }
            if let Err(error) = conn.ingress().try_bind(source.budget()) {
                source.discard();
                return Err(error.into());
            }
            conn.ingress().has_event() || conn.ingress().is_blocked()
        };
        let result = if work {
            self.resume_inner()
        } else {
            Ok(Progress::default())
        };
        let result = match result {
            Ok(total) if total.is_exhausted() => return Ok(total),
            Ok(total) => self.drive(total, |context, protocol| source.borrow(protocol(context))),
            Err(error) => Err(error),
        };
        if let Err(error) = &result
            && (!error.connection().is_some_and(conn::Error::is_pressure)
                || !(self.protocol)(self.context).ingress().is_blocked())
        {
            source.discard();
        }
        result
    }

    pub(in crate::server) fn resume(&mut self) -> Result<Progress, X> {
        if (self.protocol)(self.context).status().is_closing() {
            return Ok(Progress::default());
        }
        self.resume_inner()
    }

    fn resume_inner(&mut self) -> Result<Progress, X> {
        let total = (self.events)(self.context, false)?;
        if total.is_exhausted() || !(self.protocol)(self.context).ingress().is_blocked() {
            return Ok(total);
        }
        self.drive(total, |context, protocol| {
            protocol(context).ingress().resume()
        })
    }

    fn drive<N>(&mut self, mut total: Progress, mut next: N) -> Result<Progress, X>
    where
        N: FnMut(&mut T, &mut P) -> Result<(), conn::Error>,
    {
        loop {
            let error = match next(self.context, &mut self.protocol) {
                Err(error) if error.is_pressure() => error,
                result => {
                    result?;
                    total.merge((self.events)(self.context, false)?);
                    return Ok(total);
                }
            };
            let drained = (self.events)(self.context, true)?;
            total.merge(drained);
            if drained.is_exhausted() && (self.protocol)(self.context).ingress().is_blocked() {
                return Ok(total);
            }
            if drained.is_exhausted() || !drained.has_drained() {
                return Err(error.into());
            }
        }
    }
}
