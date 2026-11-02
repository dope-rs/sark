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

impl<'a, T, P, D, X> Read<'a, T, P, D>
where
    D: FnMut(&mut T) -> Result<Progress, X>,
    X: Fault,
{
    pub(in crate::server) fn new(context: &'a mut T, protocol: P, events: D) -> Self {
        Self {
            context,
            protocol,
            events,
        }
    }

    pub(in crate::server) fn read<'w, 'd: 'w, F, E, W>(
        &mut self,
        source: &mut Turn<'_, 'w, 'd, W>,
    ) -> Result<Progress, X>
    where
        F: rules::Policy,
        E: event::Evidence,
        W: wire::Spec,
        P: for<'step> FnMut(
            &'step mut T,
        ) -> &'step mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
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
        let total = if work {
            match self.resume_inner() {
                Ok(total) => total,
                Err(error) => {
                    if !error.connection().is_some_and(conn::Error::is_pressure)
                        || !(self.protocol)(self.context).ingress().is_blocked()
                    {
                        source.discard();
                    }
                    return Err(error);
                }
            }
        } else {
            Progress::default()
        };
        if total.is_exhausted() {
            return Ok(total);
        }
        let result = source.borrow((self.protocol)(self.context));
        let result = self.drive(result, total, |context, protocol| {
            source.borrow(protocol(context))
        });
        if let Err(error) = &result
            && (!error.connection().is_some_and(conn::Error::is_pressure)
                || !(self.protocol)(self.context).ingress().is_blocked())
        {
            source.discard();
        }
        result
    }

    pub(in crate::server) fn resume<'d, F, E>(&mut self) -> Result<Progress, X>
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
    {
        if (self.protocol)(self.context).status().is_closing() {
            return Ok(Progress::default());
        }
        self.resume_inner()
    }

    fn resume_inner<'d, F, E>(&mut self) -> Result<Progress, X>
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
    {
        let total = (self.events)(self.context)?;
        if total.is_exhausted() || !(self.protocol)(self.context).ingress().is_blocked() {
            return Ok(total);
        }
        let result = (self.protocol)(self.context).ingress().resume();
        self.drive(result, total, |context, protocol| {
            protocol(context).ingress().resume()
        })
    }

    fn drive<'d, F, E, N>(
        &mut self,
        mut result: Result<(), conn::Error>,
        mut total: Progress,
        mut next: N,
    ) -> Result<Progress, X>
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
        N: FnMut(&mut T, &mut P) -> Result<(), conn::Error>,
    {
        loop {
            let error = match result {
                Err(error) if error.is_pressure() => error,
                result => {
                    result?;
                    total.merge((self.events)(self.context)?);
                    return Ok(total);
                }
            };
            let drained = (self.events)(self.context)?;
            total.merge(drained);
            if drained.is_exhausted() && (self.protocol)(self.context).ingress().is_blocked() {
                return Ok(total);
            }
            if drained.is_exhausted() || !drained.has_drained() {
                return Err(error.into());
            }
            result = next(self.context, &mut self.protocol);
        }
    }
}
