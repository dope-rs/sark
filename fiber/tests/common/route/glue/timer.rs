use std::time::Duration;

use dope::core::driver::schedule::clock::timer::Timer;
use sark_http::payload::response::status;

use crate::common::{retained::State, route::body::OwnedReply};
#[sark_fiber::handler(pub)]
async fn wait_twice<'state>(state: &State<'state>, timer: Timer<'_>) -> OwnedReply {
    state.delay(timer, Duration::from_millis(50)).await;
    state.delay(timer, Duration::from_millis(50)).await;
    OwnedReply {
        status: status::Code::OK,
        body: b"two slept".to_vec(),
    }
}

sark_fiber::define_route! {
    pub Chain<'clock>: State<'clock> => {
        GET "/chain" => async(capacity = 32, body = discarded) wait_twice,
    }
}
