use sark_h3::{
    conn,
    conn::{event, io},
};
use sark_http::field::metadata::section::{self, rules, state::Selected};

type Event<'d, P> = event::Event<'d, Selected<P>, <P as section::Policy>::Block, event::Token>;

/// Exhaustive event adapter for deterministic tests.
#[repr(transparent)]
pub struct Events<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush> {
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Events<'a, 'd, P, A> {
    pub fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }
}

impl<'d, P, A> Events<'_, 'd, P, A>
where
    P: rules::Policy,
    A: rules::Push,
{
    pub fn poll_token(self) -> Result<Option<Event<'d, P>>, conn::Error> {
        loop {
            match self.conn.events().poll_token()? {
                io::Step::Item(event) => return Ok(Some(event)),
                io::Step::More => {}
                io::Step::Idle => return Ok(None),
            }
        }
    }
}
