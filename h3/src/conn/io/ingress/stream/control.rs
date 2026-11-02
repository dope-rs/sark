use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn,
    conn::{
        event,
        io::ingress::{Parts, receive::Recv},
    },
    frame,
};

pub(super) struct Control<'a, 'parts, 'd, P: rules::Policy, A: rules::Push> {
    parts: &'a mut Parts<'parts, 'd, P, A>,
}

impl<'a, 'parts, 'd, P: rules::Policy, A: rules::Push> Control<'a, 'parts, 'd, P, A> {
    pub(super) const fn new(parts: &'a mut Parts<'parts, 'd, P, A>) -> Self {
        Self { parts }
    }

    fn observe(&self, id: Id, kind: u64) -> event::Frame {
        event::Frame {
            stream_id: id.get(),
            kind,
            role: self.parts.settings.role,
            site: event::Site::Control,
        }
    }

    pub(super) fn ingest(
        &mut self,
        id: Id,
        recv: &mut Recv<'d>,
        boundary: Boundary,
        released: &mut usize,
    ) -> Result<(), conn::Error> {
        if !recv.buf.is_empty() || recv.decoder.is_pending() {
            if self.parts.queues.events.is_full()
                || recv.has_credit() && self.parts.queues.receive_credits.is_full()
            {
                self.parts.park(id, recv)?;
                return Ok(());
            }
            let Some(frame::Read {
                frame,
                consumed: n,
                next,
            }) = recv.decode_frame(id, self.parts.settings.max_frame_size)?
            else {
                return Ok(());
            };
            if self.parts.settings.peer_settings.is_none()
                && !matches!(&frame, frame::Decoded::Settings(_))
            {
                return Err(conn::Error::MissingSettings(self.observe(id, frame.kind())));
            }
            if self.parts.settings.role == event::Role::Client
                && matches!(&frame, frame::Decoded::MaxPushId { .. })
            {
                return Err(conn::Error::FrameUnexpected(event::Violation::Receive(
                    self.observe(id, frame.kind()),
                )));
            }
            match frame {
                frame::Decoded::Settings(settings) => {
                    if self.parts.settings.peer_settings.is_some() {
                        return Err(conn::Error::DuplicateSettings(
                            self.observe(id, frame::TYPE_SETTINGS.get()),
                        ));
                    }
                    let vacancy = self.parts.queues.try_event()?;
                    self.parts.qpack.qpack_encoder.allow(settings.table().get());
                    self.parts.settings.peer_settings = Some(settings.clone());
                    self.parts
                        .qpack
                        .qpack_encoder
                        .set_blocked(settings.blocked().get());
                    vacancy.push_back(event::Event::Settings(settings));
                }
                frame::Decoded::CancelPush { push_id } => {
                    self.parts
                        .session
                        .check_cancel(push_id.get(), self.parts.settings.role)?;
                    self.parts
                        .queues
                        .try_event()?
                        .push_back(event::Event::CancelPush {
                            push_id: push_id.get(),
                        });
                }
                frame::Decoded::GoAway { id } => {
                    let id = id.get();
                    self.parts.check_goaway(id)?;
                    let vacancy = self.parts.queues.try_event()?;
                    self.parts.session.goaway_id = Some(id);
                    vacancy.push_back(event::Event::GoAway { id });
                }
                frame::Decoded::MaxPushId { push_id } => {
                    let push_id = push_id.get();
                    if let Some(previous) = self.parts.session.max_push_id
                        && push_id < previous
                    {
                        return Err(conn::Error::PushIdOrder {
                            previous,
                            actual: push_id,
                        });
                    }
                    self.parts.session.max_push_id = Some(push_id);
                }
                frame::Decoded::Unknown { .. } => {}
                frame => {
                    return Err(conn::Error::FrameUnexpected(event::Violation::Receive(
                        self.observe(id, frame.kind()),
                    )));
                }
            }
            recv.consume(n, released, &mut self.parts.ingress.budget)?;
            recv.decoder = next;
        }
        if !recv.buf.is_empty() {
            self.parts.park(id, recv)?;
            return Ok(());
        }
        if matches!(boundary, Boundary::End) {
            return Err(conn::Error::ClosedCriticalStream(id.get()));
        }
        Ok(())
    }
}
