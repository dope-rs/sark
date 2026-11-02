use o3::buffer::storage;
use sark_core::{life::session, manifold::schedule};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{
        self, event, machine,
        machine::ops::{Drain, Shutdown},
    },
    frame::{self, numbers, payload},
};

#[repr(transparent)]
pub struct Control<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Control<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Control<'static, crate::ClientRole>>() == align_of::<usize>());
};

impl<R, F, C, A, E> Control<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn ping(&mut self, opaque: [u8; 8]) -> Result<(), conn::Error> {
        let frame = payload::Ping {
            ack: frame::Ack::Required,
            opaque,
        };
        let wire = frame.wire_bytes();
        self.conn.egress.outbound.try_append([&wire])?.commit();
        Ok(())
    }

    pub fn goaway(&mut self, error: numbers::ErrorCode) {
        self.write(error, Shutdown::Immediate)
    }

    fn write(&mut self, error: numbers::ErrorCode, shutdown: Shutdown) {
        if self.conn.state.lifecycle.is_goaway() {
            self.conn.state.lifecycle.mark_goaway(shutdown);
            return;
        }
        let wire = payload::GoAway::terminal(self.conn.state.stream_history.last_peer_id(), error);
        self.conn.egress.outbound.bytes.finish(&wire);
        self.conn.state.lifecycle.mark_goaway(shutdown);
    }

    /// Stops admitting new streams while accepted owners retire.
    pub fn drain(&mut self, activity: schedule::Quiescence) -> Drain {
        self.write(numbers::ErrorCode::NoError, Shutdown::Drain);
        self.finish(activity)
    }

    pub fn finish(&mut self, activity: schedule::Quiescence) -> Drain {
        match activity {
            schedule::Quiescence::Quiet => self.conn.state.lifecycle.finish_drain(),
            schedule::Quiescence::Busy => Drain::Pending,
        }
    }
}
