use std::pin;

use conn::scope;
use dope::core::driver::schedule::clock;
use dope_quic::{
    conn,
    conn::{
        id,
        stream::{access::bindings, resident::Handle},
    },
    runtime::mux,
    stream,
    wire::stream::Id,
};
use o3::buffer::resident;
use sark_core::{
    deadline::timeline,
    life::{self, exchange},
    manifold::task::app::Runtime,
};

use crate::{
    conn::io::ingress::schedule,
    manifold::{
        self, owners, requests,
        server::{
            self, deadline, deadline::state::State, rotation, session::finish,
            task::ingress::item::Item,
        },
    },
};

pub(in crate::manifold::server) fn accept<
    'scope,
    'd,
    const FLUSH: bool,
    R,
    O,
    L,
    E,
    const ID: u8,
    B,
    const RECONCILE: bool,
>(
    state: &mut super::Ready<'_, 'd, R, L, E>,
    mut app: pin::Pin<&mut R>,
    life: &mut O,
    conn: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    note: manifold::Notification<'scope>,
    now: clock::Moment<'d>,
) -> mux::Disposition
where
    L: rotation::Policy<'d>,
    E: State<'d>,
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    B: stream::Retain<'d>,
{
    let state = &mut *state.state;
    if schedule::Status::new(state.h3.h3()).has_work() {
        Item::new(state, app.as_mut(), life, conn, handle, now).step();
        return if conn.view().lifecycle().is_closing() {
            mux::Disposition::Accept
        } else {
            mux::Disposition::Defer
        };
    }
    let note = match note.bind(&mut state.h3, conn) {
        Ok(note) => note,
        Err(error) => {
            life.diagnostic(&error);
            manifold::close_connection_scoped(conn, error.code());
            return mux::Disposition::Accept;
        }
    };
    let raw = note.event;
    let local = note.handle;
    let rejected = raw
        .stream_id()
        .is_some_and(|stream| schedule::Status::new(state.h3.h3()).rejected(stream));
    let ident = local.map(Handle::local).and_then(requests::Identity::new);
    let slot = match local {
        Some(local) => match conn.streams().binding().try_slot(local) {
            Ok(slot) => slot,
            Err(stale) => {
                life.diagnostic(&stale);
                let error: manifold::Error = stale.into();
                manifold::close_connection_scoped(conn, error.code());
                return mux::Disposition::Accept;
            }
        },
        None => None,
    };
    if matches!(raw, manifold::Event::Readable { .. })
        && !rejected
        && let Some((ident, local)) = ident.zip(local)
        && let None = slot
    {
        if let Err(rotation::Admission::Draining { .. }) =
            rotation::State::accepting(&state.rotation)
        {
            state.reject_fresh(
                life,
                conn,
                handle,
                local,
                crate::frame::ErrorCode::RequestRejected,
            );
            return mux::Disposition::Accept;
        }
        let held = match state.requests.reserve_head(ident) {
            Ok(held) => held,
            Err(error) => {
                life.diagnostic(&error);
                finish::reject_new(state, life, conn, handle, local, error);
                return mux::Disposition::Accept;
            }
        };
        let id = held.id();
        match conn.streams().binding().try_bind(local, id.slot()) {
            Ok(bindings::Bind::Added | bindings::Bind::Present) => {}
            Err(fault) => {
                life.diagnostic(&fault);
                held.discard();
                let error: manifold::Error = fault.into();
                manifold::close_connection_scoped(conn, error.code());
                return mux::Disposition::Accept;
            }
        }
        life.observe(life::Event::ExchangeOpened(exchange::Id::new(
            handle,
            id.stream_id(),
        )));
        if FLUSH && let Err(error) = conn.streams().binding().try_prepare(local) {
            life.diagnostic(&error);
            let error: manifold::Error = error.into();
            manifold::close_connection_scoped(conn, error.code());
            return mux::Disposition::Accept;
        }
        if let Err(error) = E::transition(
            &mut state.deadline,
            id,
            timeline::Transition::AwaitingHead,
            now,
        ) {
            life.diagnostic(&error);
            manifold::close_connection_scoped(conn, deadline::code(error));
            return mux::Disposition::Accept;
        }
    }
    if !FLUSH && !rejected && matches!(raw, manifold::Event::Readable { .. }) && ident.is_some() {
        return mux::Disposition::Defer;
    }
    let report = match state.h3.ingress().drive(conn, note, life, handle) {
        Ok(report) => report,
        Err(error) => {
            life.diagnostic(&error);
            manifold::close_connection_scoped(conn, error.code());
            return mux::Disposition::Accept;
        }
    };
    if let Some(report) = report
        && let Err(error) = state.account(report)
    {
        life.diagnostic(&error);
        manifold::close_connection_scoped(conn, server::request_error_code(error));
        return mux::Disposition::Accept;
    }
    Item::new(state, app.as_mut(), life, conn, handle, now).step();
    if conn.view().lifecycle().is_closing() {
        return mux::Disposition::Accept;
    }
    if FLUSH && let Err(error) = state.flush_h3(life, conn, handle) {
        life.diagnostic(&error);
        manifold::close_connection_scoped(conn, error.code());
    }
    mux::Disposition::Accept
}
