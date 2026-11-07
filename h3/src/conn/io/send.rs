use dope_quic::wire::{
    stream::{Id, Kind},
    varint,
};
use o3::buffer::{hold, storage};
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field, field::metadata::section::rules, payload::response::Method};

use crate::{
    conn,
    conn::{
        event,
        io::{
            Stream,
            outbound::{Prepared, Section},
            write::{Entry, budget},
        },
        output, push,
    },
    frame,
    qpack::{encoder, identity::Identity},
    stream,
};

/// Exclusive capability for queueing one H3 output transaction.
#[repr(transparent)]
pub struct Send<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush> {
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Send<'a, 'd, P, A> {
    pub const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }

    /// Checks one owned stream and retains its connection throughout sending.
    pub fn bind(self, target: stream::Owned) -> Result<Stream<'a, 'd, P, A>, conn::Error> {
        let target = target
            .try_target(self.conn.origin())
            .map_err(conn::Error::Target)?;
        Ok(Stream {
            conn: self.conn,
            target,
        })
    }

    pub fn headers<S: field::Source>(
        self,
        target: impl Into<stream::Target>,
        fields: S,
        boundary: Boundary,
    ) -> Result<hold::Charge, conn::Error> {
        use crate::frame::TYPE_HEADERS;

        let conn = self.conn;
        let target = target.into();
        let pending = conn.stage(target, TYPE_HEADERS, None, fields, boundary)?;
        pending.open().commit()
    }

    pub fn data(
        self,
        target: impl Into<stream::Target>,
        data: &[u8],
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let conn = self.conn;
        let target = target.into();
        let data = frame::Data::new(data).map_err(conn::Error::Parse)?;
        let prefix = output::WritePrefix::try_encode(&data)?;
        conn.queues.enqueue(
            target,
            Entry::single(budget::WriteClass::Regular, prefix, boundary),
        )
    }

    pub fn owned(
        self,
        target: impl Into<stream::Target>,
        data: Vec<u8>,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let conn = self.conn;
        let target = target.into();
        conn.reply()
            .send(target, data, boundary)
            .map_err(|(error, _)| error)?;
        Ok(())
    }

    pub fn shared(
        self,
        target: impl Into<stream::Target>,
        data: storage::Shared,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let conn = self.conn;
        let target = target.into();
        conn.reply()
            .send(target, output::WritePayload::Shared(data), boundary)
            .map_err(|(error, _)| error)?;
        Ok(())
    }

    pub fn promise<S: field::Source>(
        self,
        target: impl Into<stream::Target>,
        push_id: u64,
        fields: S,
    ) -> Result<hold::Charge, conn::Error> {
        use crate::frame::TYPE_PUSH_PROMISE;

        let conn = self.conn;
        let target = target.into();
        if conn.settings.role == event::Role::Client {
            return Err(conn::Error::FrameUnexpected(
                event::Violation::PromiseFromClient {
                    stream_id: target.stream_id().get(),
                    push_id,
                },
            ));
        }
        let push_id =
            varint::VarInt::new(push_id).ok_or(conn::Error::VarInt { actual: push_id })?;
        push::check(push_id.get(), conn.session.max_push_id).map_err(conn::Error::Push)?;
        if let Some(cutoff) = conn.session.goaway_id
            && (push_id.get() >= cutoff || !conn.session.pushes.contains(push_id.get()))
        {
            return Err(conn::Error::Push(push::Error::GoAway {
                push_id: push_id.get(),
                cutoff,
            }));
        }
        let encoder_capacity =
            o3::depot::capacity::Count::new((conn.qpack.qpack_encoder.maximum() / 32) as u32);
        let prepared = encoder::Prepared::prepare(
            &mut conn.qpack.qpack_encoder,
            target.stream_id().get(),
            fields,
            encoder::Peer::new(
                conn.settings.peer(),
                (Identity::new(), conn.session.pushes.limit()),
            ),
        )
        .map_err(conn::Error::Encode)?;
        let (fields, peer) = Section::prepared(
            target,
            TYPE_PUSH_PROMISE,
            Some(push_id),
            prepared,
            Boundary::More,
        )?;
        let ((identity, _), method) = peer.into_parts();
        let promise =
            match conn
                .session
                .pushes
                .prepare(push_id, identity, method.unwrap_or(Method::Other))
            {
                Ok(promise) => promise,
                Err(error) => {
                    fields.discard();
                    return Err(conn::Error::Push(error));
                }
            };
        let pending = Prepared::new(
            fields,
            &mut conn.queues,
            conn.session.qpack_encoder,
            None,
            &mut conn.qpack.pending_encoder,
            encoder_capacity,
        );
        let retained = pending.open().commit()?;
        promise.commit();
        Ok(retained)
    }

    pub fn cancel(self, push_id: u64) -> Result<(), conn::Error> {
        let push_id =
            varint::VarInt::new(push_id).ok_or(conn::Error::VarInt { actual: push_id })?;
        self.conn
            .session
            .check_cancel(push_id.get(), self.conn.settings.role)?;
        self.control(frame::Control::cancel_push(push_id))
    }

    pub fn goaway(self, id: u64) -> Result<(), conn::Error> {
        let id = varint::VarInt::new(id).ok_or(conn::Error::VarInt { actual: id })?;
        if self.conn.settings.role == event::Role::Server
            && !Id::new(id.get()).is_some_and(|stream| stream.kind() == Kind::ClientBidi)
        {
            return Err(conn::Error::GoAwayId(id.get()));
        }
        let conn = self.conn;
        if let Some(previous) = conn.session.sent_goaway_id
            && id.get() > previous
        {
            return Err(conn::Error::GoAwayOrder {
                previous,
                actual: id.get(),
            });
        }
        Send::new(conn).control(frame::Control::goaway(id))?;
        conn.session.sent_goaway_id = Some(id.get());
        Ok(())
    }

    pub fn max_push(self, push_id: u64) -> Result<(), conn::Error> {
        if self.conn.settings.role != event::Role::Client {
            return Err(conn::Error::FrameUnexpected(
                event::Violation::MaxPushFromServer { push_id },
            ));
        }
        let push_id =
            varint::VarInt::new(push_id).ok_or(conn::Error::VarInt { actual: push_id })?;
        let conn = self.conn;
        if let Some(previous) = conn.session.max_push_id
            && push_id.get() < previous
        {
            return Err(conn::Error::PushIdOrder {
                previous,
                actual: push_id.get(),
            });
        }
        Send::new(conn).control(frame::Control::max_push_id(push_id))?;
        conn.session.max_push_id = Some(push_id.get());
        Ok(())
    }

    fn control(self, control: frame::Control) -> Result<(), conn::Error> {
        let conn = self.conn;
        let stream = conn.session.control.ok_or(conn::Error::ControlMissing)?;
        let prefix = output::WritePrefix::try_encode(&control)?;
        conn.queues.enqueue(
            stream,
            Entry::single(budget::WriteClass::Critical, prefix, Boundary::More),
        )
    }
}

const _: () =
    assert!(core::mem::size_of::<Send<'static, 'static>>() == core::mem::size_of::<usize>());
