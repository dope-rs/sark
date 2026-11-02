use dope_quic::{conn::id, stream, wire::stream::Id};
use requests::book::claim;
use sark_core::{
    deadline::timeline,
    decode::{self, request},
    life::{self, exchange},
    manifold::{
        stream::{Boundary, Section},
        task::app::Runtime,
    },
};
use sark_http::field::metadata::{
    block,
    section::{
        self,
        state::{RetainedHead, Selected},
    },
};

use crate::{
    conn::event,
    frame,
    manifold::{
        self, owners, requests,
        server::{
            self, deadline,
            deadline::state::State,
            rotation,
            session::finish,
            task::ingress::{
                head::{self, Head, Outcome},
                item::Item,
            },
        },
    },
};

pub(super) fn accept<'a, 'scope, 'd, R, O, L, E, B, const ID: u8, const RECONCILE: bool>(
    item: Item<'a, 'scope, 'd, R, O, L, E, B, ID, RECONCILE>,
    id: requests::Id,
    message: event::Message,
    head: RetainedHead<<R::Plan as section::Policy>::Block, Selected<R::Plan>, Option<block::Plan>>,
    section: Section,
) where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Recv,
{
    let Item {
        state,
        app,
        life,
        conn,
        handle,
        now,
    } = item;
    let app = app.into_ref().get_ref();
    let boundary = message.boundary();
    if matches!(section, Section::Trailing) {
        if matches!(boundary, Boundary::End) {
            head::finish(state, app, life, conn, handle, id, now);
        }
        return;
    }
    let limit = state.requests.limits().max_request_body_bytes;
    let claim = match state.requests.head(id) {
        Some(claim::Head::Reserved(claim)) => claim,
        Some(claim::Head::Stopped) => {
            if matches!(boundary, Boundary::End) {
                head::finish(state, app, life, conn, handle, id, now);
            }
            return;
        }
        None => {
            manifold::close_connection_scoped(conn, frame::ErrorCode::Internal);
            return;
        }
    };
    if let Err(rotation::Admission::Draining { .. }) =
        rotation::State::admit(&mut state.rotation, message.stream_id())
    {
        state.reject(
            life,
            conn,
            handle,
            id,
            frame::ErrorCode::RequestRejected,
            now,
        );
        return;
    }
    let span = exchange::Id::new(handle, id.stream_id());
    let Ok(head) = head.try_request() else {
        if matches!(boundary, Boundary::End) {
            life.observe(life::Event::InputFinished(span));
        }
        state.reject(
            life,
            conn,
            handle,
            id,
            server::request_error_code(requests::Error::Malformed),
            now,
        );
        return;
    };
    let claim = claim.admit(head.control().method());
    let request = match decode::Decode::try_head(app, head) {
        Ok(request) => request,
        Err(error) => {
            if matches!(boundary, Boundary::End) {
                life.observe(life::Event::InputFinished(span));
            }
            claim.respond(error.status());
            if let Err(error) = E::transition(
                &mut state.deadline,
                id,
                timeline::Transition::ResponseReady,
                now,
            ) {
                manifold::close_connection_scoped(conn, deadline::code(error));
            }
            return;
        }
    };
    let request = match request::Head::try_start(request, limit, boundary) {
        Ok(request) => request,
        Err(error) => {
            if matches!(boundary, Boundary::End) {
                life.observe(life::Event::InputFinished(span));
            }
            state.reject(
                life,
                conn,
                handle,
                id,
                server::body_error_code(error.error()),
                now,
            );
            return;
        }
    };
    let step = match boundary {
        Boundary::End => timeline::Transition::InputAdmitted,
        Boundary::More => timeline::Transition::HeadAdmitted,
    };
    if let Err(error) = E::transition(&mut state.deadline, id, step, now) {
        manifold::close_connection_scoped(conn, deadline::code(error));
        return;
    }
    life.observe(life::Event::HeadAdmitted(span));
    if matches!(boundary, Boundary::End) {
        life.observe(life::Event::InputFinished(span));
    }
    let outcome = Head {
        h3: &mut state.h3,
        deadline: &mut state.deadline,
        app,
        effects: owners::Projection::new(life, handle),
        conn,
        claim,
        boundary,
        now,
    }
    .begin(request);
    match outcome {
        Outcome::Done => {}
        Outcome::Reject(code) => state.reject(life, conn, handle, id, code, now),
        Outcome::Close(code) => manifold::close_connection_scoped(conn, code),
        Outcome::Stopped | Outcome::Removed => {
            let retired = finish::removed(
                &mut state.h3,
                now.at(&mut state.deadline),
                conn,
                id,
                matches!(outcome, Outcome::Stopped),
            );
            span.retire(life, life::Outcome::Rejected);
            if let Err(error) = retired {
                manifold::close_connection_scoped(conn, error.code());
            }
        }
    }
}
