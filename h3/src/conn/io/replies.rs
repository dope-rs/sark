use std::num::NonZero;

use dope_quic::wire::varint;
use o3::buffer::hold;
use output::Payload as _;
use sark_core::manifold::stream::Boundary;
use sark_http::{
    self, field,
    field::metadata::section::{rules, state::Selected},
};

use crate::{
    conn::{
        self,
        io::{
            outbound,
            write::{self, Entry, budget, plan},
        },
        output,
    },
    frame, stream,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    Length(varint::Overflow),
    Output(output::Error),
}

/// A nonempty DATA frame with validated wire and retention charges.
#[must_use = "validated DATA is invisible until committed"]
pub struct Data<'d> {
    plan: plan::Plan<output::WritePayload<'d>>,
}

impl<'d> Data<'d> {
    pub fn try_new(
        body: output::WritePayload<'d>,
    ) -> Result<Self, (Error, output::WritePayload<'d>)> {
        let charge = body.measure();
        let Some(length) = NonZero::new(charge.wire_bytes) else {
            return Err((Error::Empty, body));
        };
        let length = match varint::VarInt::try_from(length) {
            Ok(length) => length,
            Err(error) => return Err((Error::Length(error), body)),
        };
        let prefix = conn::frame_prefix(frame::TYPE_DATA, length);
        let (plan, _) = match plan::Plan::try_measured(prefix, body, charge) {
            Ok(plan) => plan,
            Err(rejected) => {
                let (body, error) = rejected.into_parts();
                return Err((Error::Output(error), body));
            }
        };
        Ok(Self { plan })
    }
}

#[must_use = "a prepared response is invisible until committed"]
pub struct Prepared<'a, 'd, P: rules::Policy = rules::RetainRaw> {
    fields: outbound::Pending<'a, 'd, Selected<P>, P::Block>,
}

#[must_use = "a response batch is invisible until committed"]
pub struct Batch<'a, 'd, P: rules::Policy = rules::RetainRaw>(
    outbound::Batch<'a, 'd, Selected<P>, P::Block>,
);

impl<'a, 'd, P: rules::Policy> Prepared<'a, 'd, P> {
    pub fn open(self) -> Batch<'a, 'd, P> {
        Batch(self.fields.into_batch(None))
    }

    pub fn finish(self) -> Batch<'a, 'd, P> {
        Batch(self.fields.finish().into_batch(None))
    }

    pub fn complete(self, body: Data<'d>) -> Batch<'a, 'd, P> {
        let target = self.fields.target();
        let entry = Entry::planned(
            budget::WriteClass::Regular,
            target,
            body.plan,
            Boundary::End,
        );
        Batch(self.fields.into_batch(Some(entry)))
    }

    pub(crate) fn discard(self) {
        self.fields.discard();
    }
}

impl<P: rules::Policy> Batch<'_, '_, P> {
    pub fn commit(self) -> Result<hold::Charge, conn::Error> {
        self.0.commit()
    }
}

struct Body<T> {
    target: stream::Target,
    plan: plan::Plan<T>,
    boundary: Boundary,
}

impl<'d, T: output::Payload<'d>> Body<T> {
    fn into_data(self) -> T {
        self.plan.into_payload()
    }
}

impl<'d, T: output::Payload<'d>> write::Stage<'d> for Body<T> {
    fn target(&self) -> stream::Target {
        self.target
    }

    fn class(&self) -> budget::WriteClass {
        budget::WriteClass::Regular
    }

    fn charge(&self) -> output::WriteCharge {
        self.plan.charge()
    }

    fn into_write(self) -> plan::Record<'d> {
        self.plan.into_write(self.target, self.boundary)
    }
}

/// Exclusive capability for one response admission transaction.
#[repr(transparent)]
pub struct Reply<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush> {
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Reply<'a, 'd, P, A> {
    pub(in crate::conn) const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }

    pub fn finish(self, target: impl Into<stream::Target>) -> Result<hold::Charge, conn::Error> {
        let conn = self.conn;
        let target = target.into();
        let prefix = conn::frame_prefix(frame::TYPE_DATA, varint::VarInt::ZERO);
        let retained = prefix.retention();
        let entry = Entry::single(budget::WriteClass::Regular, target, prefix, Boundary::End);
        conn.queues.enqueue(entry)?;
        Ok(retained)
    }

    /// Admits one data chunk, returning the same owners when refused. The payload
    /// type carries its own extent proof: a nonempty `segments::Nonempty` is
    /// consumed exactly like any other payload.
    pub fn send<T: output::Payload<'d>>(
        self,
        target: impl Into<stream::Target>,
        data: T,
        boundary: Boundary,
    ) -> Result<hold::Charge, (conn::Error, T)> {
        let conn = self.conn;
        let target = target.into();
        let charge = data.measure();
        let length = match varint::VarInt::try_from(charge.wire_bytes) {
            Ok(length) => length,
            Err(error) => return Err((conn::Error::Length(error), data)),
        };
        let prefix = conn::frame_prefix(frame::TYPE_DATA, length);
        let (plan, retained) = match plan::Plan::try_measured(prefix, data, charge) {
            Ok(plan) => plan,
            Err(rejected) => {
                let (data, error) = rejected.into_parts();
                return Err((conn::Error::Output(error), data));
            }
        };
        let body = Body {
            target,
            plan,
            boundary,
        };
        use write::Single;
        match Single::new(&mut conn.queues, body) {
            Ok(permit) => {
                permit.commit();
                Ok(retained)
            }
            Err(rejected) => {
                let ([body], error) = rejected.into_parts();
                Err((error, body.into_data()))
            }
        }
    }

    pub fn prepare<S: field::Source>(
        self,
        target: impl Into<stream::Target>,
        fields: S,
    ) -> Result<Prepared<'a, 'd, P>, conn::Error> {
        let conn = self.conn;
        let target = target.into();
        let fields = conn.stage(target, frame::TYPE_HEADERS, None, fields, Boundary::More)?;
        Ok(Prepared { fields })
    }
}

const _: () =
    assert!(core::mem::size_of::<Reply<'static, 'static>>() == core::mem::size_of::<usize>());
