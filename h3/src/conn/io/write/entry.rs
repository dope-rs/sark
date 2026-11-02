use o3::{buffer::hold, mem};
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
    charge: output::WriteCharge,
    write: plan::Record<'d>,
}

pub(in crate::conn) trait Stage<'d>: Sized {
    fn class(&self) -> budget::WriteClass;
    fn charge(&self) -> output::WriteCharge;
    fn target(&self) -> stream::Target;
    fn into_write(self) -> plan::Record<'d>;
}

impl<'d> Entry<'d> {
    pub(in crate::conn) fn single(
        class: budget::WriteClass,
        target: stream::Target,
        prefix: output::WritePrefix,
        boundary: Boundary,
    ) -> Self {
        let charge = prefix.charge();
        Self {
            class,
            charge,
            write: plan::Record::single(target, prefix, boundary),
        }
    }

    pub(in crate::conn) fn try_frame<T: output::Payload<'d>>(
        class: budget::WriteClass,
        target: stream::Target,
        prefix: output::WritePrefix,
        payload: T,
        boundary: Boundary,
    ) -> Result<Self, mem::Rejected<T, output::Error>> {
        let (plan, _) = plan::Plan::try_new(prefix, payload)?;
        Ok(Self::planned(class, target, plan, boundary))
    }

    pub(in crate::conn::io) fn planned<T: output::Payload<'d>>(
        class: budget::WriteClass,
        target: stream::Target,
        plan: plan::Plan<T>,
        boundary: Boundary,
    ) -> Self {
        Self {
            class,
            charge: plan.charge(),
            write: plan.into_write(target, boundary),
        }
    }

    pub(in crate::conn) fn finish(mut self) -> Self {
        self.write = self.write.finish();
        self
    }

    pub(in crate::conn) fn target(&self) -> stream::Target {
        self.write.target()
    }

    pub(in crate::conn) fn retained(&self) -> hold::Charge {
        self.write.retained()
    }
}

impl<'d> Stage<'d> for Entry<'d> {
    fn target(&self) -> stream::Target {
        self.write.target()
    }

    fn class(&self) -> budget::WriteClass {
        self.class
    }

    fn charge(&self) -> output::WriteCharge {
        self.charge
    }

    fn into_write(self) -> plan::Record<'d> {
        self.write
    }
}
