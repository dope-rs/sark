use o3::depot::queue::ring;
use sark_core::manifold::stream::Boundary;
use sark_http::payload::response::Body as _;

use crate::conn::{
    self,
    event::{Event, Message, Origin, Token},
};

pub struct Discard<'a, 'd, S, B> {
    events: &'a mut ring::Fifo<Event<'d, S, B, Token>>,
}

impl<'a, 'd, S, B> Discard<'a, 'd, S, B> {
    pub fn new(events: &'a mut ring::Fifo<Event<'d, S, B, Token>>) -> Self {
        Self { events }
    }

    /// Returns discarded DATA bytes and parser-consumed receive credit, respectively.
    pub fn try_for(self, token: Token) -> Result<(usize, usize), conn::Error> {
        self.discard::<false>(token, Origin::Transport)
    }

    pub(in crate::conn) fn try_input(
        self,
        token: Token,
        origin: Origin,
    ) -> Result<(usize, usize), conn::Error> {
        self.discard::<true>(token, origin)
    }

    fn discard<const INPUT: bool>(
        self,
        token: Token,
        origin: Origin,
    ) -> Result<(usize, usize), conn::Error> {
        let mut held = Ok((0usize, 0usize));
        for _ in 0..self.events.len() {
            let Some(front) = self.events.front_entry() else {
                break;
            };
            let (mut event, vacant) = front.take();
            if event.stream_id() != Some(token)
                || INPUT
                    && (matches!(event, Event::Stopped { .. })
                        || event.is_detached() != (origin == Origin::Wire))
            {
                vacant.push_back(event);
                continue;
            }
            if INPUT && let Event::PushPromise { message, .. } = &mut event {
                // Preserve the independent push; RESET now owns the parent's end.
                *message = Message::new(token, Boundary::More);
                vacant.push_back(event);
                continue;
            }
            held = match (held, event) {
                (
                    Ok((queued, released)),
                    Event::Data { data, .. } | Event::PushData { data, .. },
                ) => conn::try_add_credit(queued, data.len()).map(|queued| (queued, released)),
                (
                    Ok((queued, released)),
                    Event::Reset {
                        released: bytes, ..
                    },
                ) => conn::try_add_credit(released, bytes).map(|released| (queued, released)),
                (result, _) => result,
            };
        }
        held
    }
}
