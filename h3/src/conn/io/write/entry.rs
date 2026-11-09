use o3::buffer::hold;
use sark_core::manifold::stream::Boundary;

use crate::{
    conn::{
        io::write::{budget, plan},
        output,
    },
    stream,
};

pub(in crate::conn) struct Entry<'d> {
    class: budget::WriteClass,
    boundary: Boundary,
    plan: plan::Plan<Option<output::WritePayload<'d>>>,
}

pub(in crate::conn) trait Stage<'d>: Sized {
    fn class(&self) -> budget::WriteClass;
    fn charge(&self) -> output::WriteCharge;
    fn into_write(self, target: stream::Target) -> plan::Record<'d>;
}

impl<'d> Entry<'d> {
    pub(in crate::conn) fn single(
        class: budget::WriteClass,
        prefix: output::WritePrefix,
        boundary: Boundary,
    ) -> Self {
        Self {
            class,
            boundary,
            plan: plan::Plan::single(prefix),
        }
    }

    pub(in crate::conn) fn frame(
        class: budget::WriteClass,
        prefix: output::WritePrefix,
        payload: Vec<u8>,
        boundary: Boundary,
    ) -> Self {
        Self::planned(class, plan::Plan::new(prefix, payload), boundary)
    }

    pub(in crate::conn::io) fn planned<T: output::Payload<'d>>(
        class: budget::WriteClass,
        plan: plan::Plan<T>,
        boundary: Boundary,
    ) -> Self {
        Self {
            class,
            boundary,
            plan: plan.freeze(),
        }
    }

    pub(in crate::conn) fn finish(&mut self) {
        self.boundary = Boundary::End;
    }

    pub(in crate::conn) fn retained(&self) -> hold::Charge {
        self.plan.retained()
    }
}

impl<'d> Stage<'d> for Entry<'d> {
    fn class(&self) -> budget::WriteClass {
        self.class
    }

    fn charge(&self) -> output::WriteCharge {
        self.plan.charge()
    }

    fn into_write(self, target: stream::Target) -> plan::Record<'d> {
        self.plan.into_write(target, self.boundary)
    }
}
