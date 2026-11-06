use std::{num::NonZero, ops::ControlFlow};

use dope_quic::{stream::release, wire::varint};
use o3::buffer::hold;
use output::Payload as _;
pub(crate) use plan::Plan;
use sark_core::manifold::stream::Boundary;
use sark_http::{
    self, field,
    field::metadata::section::{rules, state::Selected},
    payload::response::dispatch::chunk::Chunk,
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
    Output(output::Overflow),
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
        let plan = match plan::Plan::try_measured(prefix, body, charge) {
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

/// Ordinary response owners whose initial QPACK references remain unpublished.
pub(crate) struct Pending<'d> {
    fields: Entry<'d>,
    trailing: Option<Entry<'d>>,
}

impl<'d> Pending<'d> {
    pub(in crate::conn) const fn new(fields: Entry<'d>, trailing: Option<Entry<'d>>) -> Self {
        Self { fields, trailing }
    }

    pub(crate) fn discard<P: rules::Policy, A: rules::Push>(
        self,
        reply: Reply<'_, 'd, P, A>,
        target: stream::Target,
    ) {
        reply
            .conn
            .qpack
            .qpack_encoder
            .discard_initial(target.stream_id().get());
    }

    pub(crate) fn send<M: release::Mode, P: rules::Policy, A: rules::Push>(
        self,
        reply: Reply<'_, 'd, P, A>,
        target: stream::Target,
    ) -> Result<ControlFlow<Self, M::Projection>, conn::Error> {
        let conn = reply.conn;
        let stream = target.stream_id().get();
        let published = conn.qpack.published();
        let initial = conn.qpack.qpack_encoder.initial(stream);
        if initial
            .as_ref()
            .is_some_and(|head| !head.is_ready(published))
        {
            return Ok(ControlFlow::Break(self));
        }
        let attempt = match self.trailing {
            None => match write::Single::new(&mut conn.queues, target, self.fields) {
                Ok(permit) => Ok(permit.commit::<M>()),
                Err(rejected) => {
                    let ([fields], error) = rejected.into_parts();
                    Err((Self::new(fields, None), error))
                }
            },
            Some(trailing) => {
                let attempt = match write::Permit::new(
                    &mut conn.queues,
                    [(target, self.fields), (target, trailing)],
                ) {
                    Ok(permit) => permit
                        .commit::<M, _>((), |()| ())
                        .map_err(|(rejected, ())| rejected),
                    Err(rejected) => Err(rejected),
                };
                match attempt {
                    Ok(retained) => Ok(retained),
                    Err(rejected) => {
                        let ([(_, fields), (_, trailing)], error) = rejected.into_parts();
                        Err((Self::new(fields, Some(trailing)), error))
                    }
                }
            }
        };
        match attempt {
            Ok(retained) => {
                if let Some(initial) = initial {
                    initial.publish_initial();
                }
                Ok(ControlFlow::Continue(retained))
            }
            Err((owner, error)) if conn.queues.is_pressure(&error) => Ok(ControlFlow::Break(owner)),
            Err((owner, error)) => {
                drop(initial);
                owner.discard(Reply::new(conn), target);
                Err(error)
            }
        }
    }
}

impl<'a, 'd, P: rules::Policy> Prepared<'a, 'd, P> {
    pub fn open(self) -> Batch<'a, 'd, P> {
        Batch(self.fields.into_batch(None))
    }

    pub fn finish(self) -> Batch<'a, 'd, P> {
        Batch(self.fields.finish().into_batch(None))
    }

    pub fn complete(self, body: Data<'d>) -> Batch<'a, 'd, P> {
        let entry = Entry::planned(budget::WriteClass::Regular, body.plan, Boundary::End);
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

impl<'d, P: rules::Policy> Batch<'_, 'd, P> {
    pub(crate) fn defer(self) -> Result<Pending<'d>, conn::Error> {
        self.0.defer()
    }
}

pub(crate) fn try_data<'d, T: output::Payload<'d>>(data: T) -> Result<Plan<T>, (conn::Error, T)> {
    let charge = data.measure();
    let length = match varint::VarInt::try_from(charge.wire_bytes) {
        Ok(length) => length,
        Err(error) => return Err((conn::Error::Length(error), data)),
    };
    let prefix = conn::frame_prefix(frame::TYPE_DATA, length);
    Plan::try_measured(prefix, data, charge).map_err(|rejected| {
        let (data, error) = rejected.into_parts();
        (conn::Error::Output(error), data)
    })
}

impl<'d, T: output::Payload<'d>> write::Stage<'d> for (Plan<T>, Boundary) {
    fn class(&self) -> budget::WriteClass {
        budget::WriteClass::Regular
    }

    fn charge(&self) -> output::WriteCharge {
        self.0.charge()
    }

    fn into_write(self, target: stream::Target) -> plan::Record<'d> {
        self.0.freeze().into_write(target, self.1)
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
        let prefix = output::WritePrefix::Inline(o3::buffer::storage::inline::Bytes::new());
        let retained = prefix.retention();
        let entry = Entry::single(budget::WriteClass::Regular, prefix, Boundary::End);
        conn.queues.enqueue(target, entry)?;
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
        self.send_with(target.into(), try_data(data)?, boundary)
            .map_err(|(error, plan)| (error, plan.into_payload()))
    }

    pub(crate) fn send_chunk(
        self,
        target: stream::Target,
        plan: Plan<Chunk<'d>>,
    ) -> Result<hold::Charge, (conn::Error, Plan<Chunk<'d>>)> {
        self.send_with(target, plan, Boundary::More)
    }

    fn send_with<T: output::Payload<'d>>(
        self,
        target: stream::Target,
        plan: Plan<T>,
        boundary: Boundary,
    ) -> Result<hold::Charge, (conn::Error, Plan<T>)> {
        let conn = self.conn;
        use write::Single;
        let ([(plan, _)], error) = match Single::new(&mut conn.queues, target, (plan, boundary)) {
            Ok(permit) => return Ok(permit.commit::<hold::evidence::On>()),
            Err(rejected) => rejected.into_parts(),
        };
        let error = if conn.queues.is_pressure(&error) {
            conn.queues
                .write_budget
                .check_pending(plan.charge(), budget::WriteClass::Regular)
                .err()
                .unwrap_or(error)
        } else {
            error
        };
        Err((error, plan))
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
