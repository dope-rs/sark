use std::time::Duration;

use dope::core::driver::schedule::clock::timer::Timer;
use sark_http::payload::response::status;

use crate::common::{retained::State, route::body::OwnedReply as Reply};

#[sark_fiber::request(ordered)]
pub struct EchoReq {
    #[authority]
    pub authority: storage::Shared,
    /// Captures bind by name despite the reversed route position.
    #[path]
    pub team_id: storage::Shared,
    #[path]
    pub user_id: storage::Shared,
    #[header("x-echo-marker", default = "MISSING")]
    pub marker: storage::Shared,
}

#[sark_fiber::handler(pub)]
async fn echo_handler<'state>(request: EchoReq, state: &State<'state>, timer: Timer<'_>) -> Reply {
    let user_ptr = request.user_id.as_slice().as_ptr();
    let team_ptr = request.team_id.as_slice().as_ptr();
    let mark_ptr = request.marker.as_slice().as_ptr();
    let auth_ptr = request.authority.as_slice().as_ptr();
    state.delay(timer, Duration::from_millis(20)).await;
    state.pointer(user_ptr, request.user_id.as_slice().as_ptr());
    state.pointer(team_ptr, request.team_id.as_slice().as_ptr());
    state.pointer(mark_ptr, request.marker.as_slice().as_ptr());
    state.pointer(auth_ptr, request.authority.as_slice().as_ptr());
    let mut body = Vec::new();
    body.extend_from_slice(b"team=");
    body.extend_from_slice(request.team_id.as_slice());
    body.extend_from_slice(b" user=");
    body.extend_from_slice(request.user_id.as_slice());
    body.extend_from_slice(b" marker=");
    body.extend_from_slice(request.marker.as_slice());
    body.extend_from_slice(b" authority=");
    body.extend_from_slice(request.authority.as_slice());
    Reply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub EchoDispatch<'state>: State<'state> => {
        GET "/users/:user_id/teams/:team_id" => request(path, header) async(capacity = 32, body = discarded) echo_handler,
    }
}
