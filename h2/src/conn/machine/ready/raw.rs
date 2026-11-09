use o3::depot::raw::RingEdit;
use sark_core::{
    life::session,
    manifold::{server::drain, stream::Boundary},
};
use sark_http::field::metadata::section::{rules, state::Selected};

use crate::conn::{event, machine::ready::Incoming, registry::store::Notice};

impl<R, F, C, A, E> AsRef<event::Event<Selected<F>, F::Block, C, E>> for Incoming<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    fn as_ref(&self) -> &event::Event<Selected<F>, F::Block, C, E> {
        let queue = &self.0.ingress.events.queue;
        // SAFETY: construction admitted the nonempty queue; this exclusive
        // connection loan prevents any mutation of its initialized front.
        unsafe { queue.as_slices().0.get_unchecked(0) }
    }
}

impl<R, F, C, A, E> drain::Ready for Incoming<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    type Item = event::Event<Selected<F>, F::Block, C, E>;

    fn take(self) -> Self::Item {
        // SAFETY: this consumes the same exclusive nonempty connection loan
        // admitted by Incoming::new; no operation can invalidate its front.
        let event = unsafe { self.0.ingress.events.queue.front_raw() }.remove();
        if self.0.state.lifecycle.keeps_events() {
            let terminal = match &event {
                event::Event::Headers {
                    stream_id,
                    boundary: Boundary::End,
                    ..
                }
                | event::Event::Data {
                    stream_id,
                    boundary: Boundary::End,
                    ..
                } => Some((*stream_id, Notice::End)),
                event::Event::StreamReset { stream_id, .. } => Some((*stream_id, Notice::Reset)),
                _ => None,
            };
            if let Some((key, notice)) = terminal {
                self.0.state.active_streams.store.release(key, notice);
            }
        }
        event
    }
}
