use o3::buffer::resident;
use sark_core::{
    body::credit,
    decode::{self, exec, request, state},
    manifold::schedule::typed,
};
use sark_http::{field::metadata::section::rules, payload::owned};

use crate::{
    conn::{self, event, machine},
    frame::numbers,
    server::{
        self, payload, response,
        state::exchange::{self, Deferred, Rejected, Routed},
        sync::book::{Delayed, Receiving, State, phase::Phase},
    },
    stream,
};

type Connection<'d, R> = machine::Connection<
    crate::ServerRole,
    <R as decode::Decode>::Plan,
    resident::Shared<'d>,
    rules::RejectPush,
    event::Ignore,
>;

type Entry<'a, 'd, R> = typed::Entry<
    'a,
    stream::Key,
    Phase<'d, R>,
    response::Pending<'d>,
    Receiving<'d, R>,
    conn::DataBlock,
>;

/// # Safety
/// The entry and account must be selected together from the same table.
pub(super) unsafe fn finish<'d, R: decode::Decode>(
    entry: Entry<'_, 'd, R>,
    account: &mut credit::raw::Account,
    conn: &mut Connection<'d, R>,
    stream: stream::Key,
    limits: server::Config,
) where
    Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
{
    match entry {
        typed::Entry::Control(entry) => {
            let (entry, finished) = entry.change(Phase::try_finish);
            if let Some(Err(error)) = finished {
                // SAFETY: change preserved this same entry and its original account.
                let error = unsafe { close(State::Control(entry.remove()), account, conn) }
                    .err()
                    .unwrap_or(error)
                    .code();
                conn.streams().cancel(stream, error);
            }
        }
        typed::Entry::Idle(entry) => {
            let result = entry.transition(|input| {
                // SAFETY: transition consumes the same selected account-owned body.
                match unsafe { try_body::<R>(input, account) } {
                    Ok(ready) => {
                        let blocked = (conn.output().len() >= limits.max_outbound_bytes)
                            .then_some(conn::DataBlock::Egress);
                        (
                            Some(typed::Next::Control(Phase::Request(ready), blocked)),
                            Ok(()),
                        )
                    }
                    Err(error) => (None, Err(error)),
                }
            });
            if let Err(error) = result {
                conn.streams().cancel(stream, error);
            }
        }
        typed::Entry::Data(_) => {}
    }
}

/// # Safety
/// The removed value and account must come from the same exclusive table.
pub(super) unsafe fn close<'d, R: decode::Decode>(
    value: State<'d, R>,
    account: &mut credit::raw::Account,
    conn: &mut Connection<'d, R>,
) -> Result<(), payload::Error>
where
    Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
{
    // SAFETY: the caller proves the unchanged value/account pair.
    if let Some(head) = unsafe { try_close(value, account) }? {
        head.discard(conn);
    }
    Ok(())
}

/// # Safety
/// The phase must be selected from the runtime held beside this exact account;
/// its charged body must have remained in that table's exclusive custody.
pub(super) unsafe fn try_push<'d, R: decode::Decode>(
    phase: &mut Phase<'d, R>,
    account: &mut credit::raw::Account,
    data: event::DataPayload<resident::Shared<'d>>,
    limits: server::Config,
) -> Result<
    payload::Push<event::DataPayload<resident::Shared<'d>>>,
    event::DataPayload<resident::Shared<'d>>,
>
where
    Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
{
    match phase {
        Phase::Routed(routed) => Ok(account.scope(|mut ledger| {
            // SAFETY: the caller retains this exact route and account together.
            let route =
                unsafe { exchange::raw::Route::new(Deferred::route_mut(routed), &mut ledger) };
            route.try_push(credit::Disabled::new(), data, limits)
        })),
        _ => Err(data),
    }
}

/// # Safety
/// The removed value and account must come from the same exclusive table.
pub(super) unsafe fn try_close<'d, R: decode::Decode>(
    value: State<'d, R>,
    account: &mut credit::raw::Account,
) -> Result<Option<response::Head<'d>>, payload::Error>
where
    Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
{
    Ok(match value {
        State::Control(Phase::Routed(request)) => {
            // SAFETY: the route is the same table-owned field supplied by the caller.
            unsafe { discard_route(Deferred::into_route(request), account) }?;
            None
        }
        State::Idle(request) => {
            // SAFETY: the body is the same table-owned field supplied by the caller.
            unsafe { discard(Rejected::Body(request.into_body()), account) }?;
            None
        }
        State::Control(Phase::Head(head)) => Some(head),
        State::Control(Phase::Request(_) | Phase::Status(_)) | State::Data(_) => None,
    })
}

/// # Safety
/// The route must belong to this account's exclusive table, or be a new,
/// uncharged route admitted here whose initial insertion failed.
pub(super) unsafe fn discard_route<'d, P, F>(
    route: Routed<'d, P, F>,
    account: &mut credit::raw::Account,
) -> Result<(), payload::Error>
where
    P: request::Prepared,
    F: state::Family<P>,
{
    account.scope(|mut ledger| {
        // SAFETY: the caller proves this route's original account custody.
        let route = unsafe { exchange::raw::Routed::new(route, &mut ledger) };
        route.discard(credit::Disabled::new())
    })
}

/// # Safety
/// The rejected body must come from this account's selected request, or its
/// fresh head admission before any body credit was charged.
pub(super) unsafe fn discard(
    input: Rejected<'_>,
    account: &mut credit::raw::Account,
) -> Result<(), payload::Error> {
    account.scope(|mut ledger| {
        // SAFETY: the caller proves this rejected body's original account.
        let input = unsafe { ledger.bind(input) };
        input.discard(&mut ledger, credit::Disabled::new())
    })
}

/// # Safety
/// The request must come from this account's selected entry or newly admitted
/// head, and all prior body charges must belong to this exact account.
pub(super) unsafe fn try_body<'d, R: decode::Decode>(
    input: Receiving<'d, R>,
    account: &mut credit::raw::Account,
) -> Result<request::Ready<exec::Sync, R::Prepared, owned::Segments<'d>>, numbers::ErrorCode> {
    account.scope(|mut ledger| {
        input
            .try_map_body(|body| {
                // SAFETY: the caller proves this selected body's original account.
                let body = unsafe { ledger.bind(body) };
                body.try_finish(&mut ledger, credit::Disabled::new())
            })
            .map(request::Admitted::ready)
            .map_err(|failure| {
                payload::release_rejected(failure, &mut ledger, &mut credit::Disabled::new())
            })
    })
}
