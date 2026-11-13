use dope_quic::wire::stream::Id;
use sark_http::{
    field::metadata::section::{self, rules, state::Selected},
    payload::response::Body as _,
};

use crate::{
    conn,
    conn::{
        event,
        io::{Resume, Step, ingress::schedule},
        output,
    },
};

type Event<'d, P> = event::Event<'d, Selected<P>, <P as section::Policy>::Block, event::Token>;

#[repr(transparent)]
pub struct Events<'a, 'd, P: rules::Policy, A: rules::Push> {
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Events<'a, 'd, P, A> {
    pub(in crate::conn) const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }
}

impl<'d, P, A> Events<'_, 'd, P, A>
where
    P: rules::Policy,
    A: rules::Push,
{
    pub fn poll_token(self) -> Result<Step<Event<'d, P>>, conn::Error> {
        let conn = self.conn;
        if let Some(event) = Events::new(conn).pop()? {
            return Ok(Step::Item(event));
        }
        let progress = schedule::Schedule::new(conn).step()?;
        if let Some(event) = Events::new(conn).pop()? {
            Ok(Step::Item(event))
        } else if matches!(progress, schedule::Progress::Moved)
            || schedule::Status::new(conn).has_work()
        {
            Ok(Step::More)
        } else {
            Ok(Step::Idle)
        }
    }

    fn pop(self) -> Result<Option<Event<'d, P>>, conn::Error> {
        let conn = self.conn;
        let Some(front) = conn.queues.events.front_entry() else {
            return Ok(None);
        };
        if matches!(conn.ingress.data_credit, conn::CreditMode::Open) {
            let bytes = match front.get() {
                event::Event::Data { data, .. } | event::Event::PushData { data, .. } => data.len(),
                _ => 0,
            };
            conn.ingress.budget.release(bytes)?;
        }
        let event = front.remove();
        if matches!(event, event::Event::QpackCancelled { .. }) {
            // Every queued cancellation contributes one count until this removal.
            conn.queues.cancellations -= 1;
        }
        Ok(Some(event))
    }

    pub fn poll_write(self) -> Result<Option<output::Write<'d>>, conn::Error> {
        let conn = self.conn;
        if let Err(error) = conn.publish_encoder()
            && !conn.queues.is_pressure(&error)
        {
            return Err(error);
        }
        let feedback = conn.session.qpack_decoder;
        let Some(taken) = conn.queues.take(&mut conn.qpack.feedback, feedback) else {
            return Ok(None);
        };
        let (write, done) = taken.with(|write, prepared| prepared.finish(write));
        if done.is_feedback() {
            Self::new(conn).resume()?;
        }
        Ok(Some(write))
    }

    pub fn poll_credit(self) -> Result<Option<(Id, usize, Resume)>, conn::Error> {
        let conn = self.conn;
        Events::new(conn).resume()?;
        Ok(conn
            .queues
            .receive_credits
            .pop_front()
            .map(|(credit, bytes)| (credit.id(), bytes, credit.resume())))
    }

    pub(crate) fn resume(self) -> Result<(), conn::Error> {
        schedule::Schedule::new(self.conn).step()?;
        Ok(())
    }
}

const _: () = assert!(
    size_of::<Events<'static, 'static, rules::RetainRaw, rules::RejectPush>>()
        == size_of::<usize>()
);
