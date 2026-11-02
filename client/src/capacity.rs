use std::mem::size_of;

use dope::{core::driver::schedule::ready::wake::queue, net::link::egress};
use dope_fiber::reply::{order, usage};
use o3::buffer::{segments, write::consume};

/// Exact bounded client resource that rejected one transition.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Error {
    RequestBuffer,
    RequestEgress(egress::Rejection),
    RequestBytes(consume::Error),
    Waiter(queue::Error),
    FailureReport,
    ResponseStaging,
    ResponseRetention,
    ResponseBackpressure(usage::Backpressure),
    ResponseCapacity(usage::Capacity),
    ResponseFront(order::Front),
    ResponseSegments(segments::Error),
    Transport,
    IdentityExhausted,
}

const _: () = {
    const BACKPRESSURE: usize = size_of::<usage::Backpressure>();
    const EGRESS: usize = size_of::<egress::Rejection>();
    const PAYLOAD: usize = if BACKPRESSURE > EGRESS {
        BACKPRESSURE
    } else {
        EGRESS
    };
    assert!(size_of::<Error>() <= PAYLOAD + size_of::<usize>());
};
