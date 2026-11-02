use core::marker;

use dope_quic::wire::stream::{Direction, Id};
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

mod control;
mod decoder;
mod push;
pub(in crate::conn::io::ingress) mod state;
pub(in crate::conn) mod table;
mod uni;

use crate::{
    conn,
    conn::io::{Resume, ingress::Parts},
    link::Status,
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::conn) enum Barrier {
    #[default]
    Open,
    Queued,
    Held,
    Rejected,
    Discarding,
}

pub(super) fn drive<'d, P: rules::Policy, A: rules::Push>(
    conn: &mut conn::Connection<'d, P, A>,
    id: Id,
    resume: Resume,
) -> Result<Status, conn::Error> {
    let result = if matches!(id.direction(), Direction::Uni) {
        uni::resume(conn, id, resume)
    } else {
        request(conn, id, resume)
    };
    match result {
        Err(conn::Error::Content { error, .. }) => {
            let vacancy = conn.queues.try_event()?;
            let token = conn.streams.reject(id)?;
            vacancy.push_front(crate::conn::event::Event::Rejected {
                stream_id: token,
                error,
            });
            Ok(Status::Suspended)
        }
        result => result,
    }
}

fn request<'d, P: rules::Policy, A: rules::Push>(
    conn: &mut conn::Connection<'d, P, A>,
    id: Id,
    resume: Resume,
) -> Result<Status, conn::Error> {
    let conn::Connection {
        settings,
        qpack,
        session,
        streams,
        queues,
        ingress,
        ..
    } = conn;
    let mut parts: Parts<'_, 'd, P, A> = Parts {
        settings,
        qpack,
        session,
        queues,
        ingress,
        push: marker::PhantomData,
    };
    let (released, held, boundary, wake) = {
        let (recv, state) = streams.request_mut(id).ok_or(conn::Error::StreamMissing {
            stream_id: id.get(),
        })?;
        let boundary = recv.boundary;
        let mut released = 0;
        let wake = match state::ingest::<false, _, _>(
            &mut parts,
            id,
            recv,
            state,
            0,
            boundary,
            &mut released,
        ) {
            Ok(wake) => wake,
            Err(error) => {
                parts.release(id, released, Resume::Idle)?;
                return Err(error);
            }
        };
        let held = matches!(
            state.data_barrier,
            Barrier::Queued | Barrier::Held | Barrier::Rejected
        ) || parts.qpack.blocked.contains(id)
            || recv.wait.is_some();
        (released, held, boundary, wake)
    };
    if let Some(ready) = wake
        && let Some(recv) = streams.recv_mut(ready)
    {
        parts.park(ready, recv)?;
    }
    let resume = if matches!(resume, Resume::Ready) && !held && matches!(boundary, Boundary::More) {
        Resume::Ready
    } else {
        Resume::Idle
    };
    parts.release(id, released, resume)?;
    if !held && matches!(boundary, Boundary::End) {
        streams.close(id)?;
    }
    Ok(if held {
        Status::Suspended
    } else {
        Status::Open
    })
}
