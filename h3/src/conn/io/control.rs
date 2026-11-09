use dope_quic::wire::stream::Direction;
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn,
    conn::{
        io::write::{Entry, Single, budget, feedback::FeedbackMut},
        output,
    },
    frame, stream,
};

/// Exclusive capability for mutating one connection's critical H3 streams.
#[repr(transparent)]
pub struct Control<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush>
{
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Control<'a, 'd, P, A> {
    pub(in crate::conn) const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }

    pub fn open(&mut self, target: impl Into<stream::Target>) -> Result<(), conn::Error> {
        let conn = &mut *self.conn;
        let target = target.into();
        let id = target.stream_id();
        if matches!(id.direction(), Direction::Bidi) {
            return Err(conn::Error::StreamKind {
                stream_id: id.get(),
            });
        }
        if let Some(existing) = conn.session.control {
            return Err(conn::Error::ControlExists {
                existing: existing.stream_id().get(),
                attempted: id.get(),
            });
        }
        let prefix = {
            let stream = frame::Stream::control(&conn.settings.local_settings)
                .map_err(conn::Error::Parse)?;
            output::WritePrefix::try_encode(&stream)?
        };
        let entry = Entry::single(budget::WriteClass::Critical, prefix, Boundary::More);
        conn.queues.enqueue(target, entry)?;
        conn.session.control = Some(target);
        Ok(())
    }

    pub fn open_encoder(&mut self, target: impl Into<stream::Target>) -> Result<(), conn::Error> {
        let conn = &mut *self.conn;
        let target = target.into();
        if let Some(existing) = conn.session.qpack_encoder {
            return Err(conn::Error::EncoderExists {
                existing: existing.stream_id().get(),
                attempted: target.stream_id().get(),
            });
        }
        conn::start_uni_stream(conn, target, frame::Stream::QpackEncoder)?;
        conn.session.qpack_encoder = Some(target);
        Ok(())
    }

    pub fn open_decoder(&mut self, target: impl Into<stream::Target>) -> Result<(), conn::Error> {
        let conn = &mut *self.conn;
        let target = target.into();
        let id = target.stream_id();
        if matches!(id.direction(), Direction::Bidi) {
            return Err(conn::Error::StreamKind {
                stream_id: id.get(),
            });
        }
        if let Some(existing) = conn.session.qpack_decoder {
            return Err(conn::Error::DecoderExists {
                existing: existing.stream_id().get(),
                attempted: id.get(),
            });
        }
        let prefix = frame::STREAM_TYPE_QPACK_DECODER.get() as u8;
        FeedbackMut::new(&mut conn.qpack.feedback, &mut conn.queues.write_budget)
            .try_prepend(prefix)?;
        conn.session.qpack_decoder = Some(target);
        Ok(())
    }

    pub(crate) fn has_encoder(&self) -> bool {
        self.conn.session.qpack_encoder.is_some()
    }

    pub fn set_capacity(&mut self, capacity: usize) -> Result<(), conn::Error> {
        let conn = &mut *self.conn;
        let target = conn
            .session
            .qpack_encoder
            .ok_or(conn::Error::EncoderMissing)?;
        let limit = conn
            .qpack
            .qpack_encoder
            .plan_limit(capacity)
            .map_err(conn::Error::Encode)?;
        let prefix = output::WritePrefix::from(&limit);
        let entry = Entry::single(budget::WriteClass::Critical, prefix, Boundary::More);
        let permit = match Single::new(&mut conn.queues, target, entry) {
            Ok(permit) => permit,
            Err(rejected) => return Err(rejected.discard()),
        };
        permit.commit::<o3::buffer::hold::evidence::Off>();
        limit.commit();
        Ok(())
    }
}

const _: () =
    assert!(core::mem::size_of::<Control<'static, 'static>>() == core::mem::size_of::<usize>());
