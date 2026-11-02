use core::array;
use std::time::Duration;

use dope::core::driver::schedule::clock::timer::Timer;
use dope_test::harness::observe::VecCellExt as _;
use sark::http::payload::response::dispatch::{Stream, chunk::Chunk};
use sark_fiber::fiber::{IntoStream, IterStream};
use sark_http::payload::{fixed, response::status};

use crate::common::retained::{Failure, State};

pub struct Domain;

#[sark_fiber::response(raw)]
pub struct Output<'d> {
    pub status: status::Code,
    pub body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn sync_handler() -> Output<'d> {
    Output {
        status: status::Code::OK,
        body: fixed::Body::new(b"sync"),
    }
}

#[sark_fiber::handler(pub)]
async fn narrow_handler<'state>(state: &State<'state>, timer: Timer<'_>) -> Output<'d> {
    state.delay(timer, Duration::ZERO).await;
    Output {
        status: status::Code::OK,
        body: fixed::Body::new(b"narrow"),
    }
}

#[sark_fiber::handler(pub)]
async fn wide_handler<'state>(state: &State<'state>, timer: Timer<'_>) -> Output<'d> {
    let padding = [7u8; 256];
    state.delay(timer, Duration::ZERO).await;
    if padding != [7; 256] {
        state.0.push(Failure::Padding(padding.to_vec()));
    }
    Output {
        status: status::Code::OK,
        body: fixed::Body::new(b"wide"),
    }
}

#[sark_fiber::handler(pub)]
async fn held_handler<'state>(state: &State<'state>, timer: Timer<'_>) -> Output<'d> {
    state.delay(timer, Duration::from_secs(30)).await;
    Output {
        status: status::Code::OK,
        body: fixed::Body::new(b"held"),
    }
}

#[sark_fiber::handler(pub)]
fn stream_handler() -> Stream<'req, IterStream<array::IntoIter<Chunk<'d>, 1>>> {
    [Chunk::from(b"stream")].into_stream()
}

sark_fiber::define_route! {
    pub GlobalBudgetApp<'state>: State<'state> => {
        GET "/held" => async(capacity = 2, body = discarded) held_handler,
        GET "/quick" => async(capacity = 2, body = discarded) narrow_handler,
        GET "/stream" => stream(capacity = 2) stream_handler,
    }
}

sark_fiber::define_route! {
    pub MixedStorageApp<'state>: State<'state> => {
        GET "/sync" => sync_handler,
        GET "/narrow" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/wide" => async(capacity = 3, body = discarded) wide_handler,
        GET "/stream" => stream(capacity = 2) stream_handler,
    }
}

sark_fiber::define_route! {
    pub ThirteenStorageApp<'state>: State<'state> => {
        GET "/r0" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r1" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r2" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r3" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r4" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r5" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r6" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r7" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r8" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r9" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r10" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r11" => async(capacity = 1, body = discarded) narrow_handler,
        GET "/r12" => async(capacity = 1, body = discarded) narrow_handler,
    }
}

pub const LEAVES: [&str; 3] = ["/r0", "/r6", "/r12"];
pub const WIDE: &[u8] = b"GET /wide HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
pub const STREAM: &[u8] = b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
pub const HELD: &[u8] = b"GET /held HTTP/1.1\r\nHost: x\r\n\r\n";
pub const QUICK: &[u8] = b"GET /quick HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
