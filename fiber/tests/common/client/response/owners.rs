//! Response detach ownership, failure, and allocation contracts.

use std::num::NonZero;

use dope::runtime::executor::drive;
use dope_fiber::{BlockOn, abi::Fiber};
use dope_test::alloc::Tracker;
use sark_client::{protocol::codec::output, reply};

pub const CHUNK: usize = output::DATA_LIMIT;

pub const BODY_LEN: usize = CHUNK * 2;

pub const SCOPED_ALLOCATIONS: usize = 4;

pub enum Value<'app, 'd> {
    Scoped(reply::Scoped<'app, 'd>),
    Retained(reply::Retained),
    Contiguous(reply::Contiguous),
}

#[derive(Debug)]
pub enum Error<E> {
    Drive(drive::Error),
    Response(E),
}

pub fn try_measure<'app, 'd: 'app, R, E, WF, S, SF, C, CF, D, DF>(
    runtime: &mut R,
    warm: WF,
    scoped: S,
    retained: C,
    flat: D,
    mut check: impl FnMut(Value<'app, 'd>),
) -> Result<[(usize, usize); 3], Error<E>>
where
    R: BlockOn<'d>,
    WF: Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, E>>,
    S: FnOnce() -> SF,
    SF: Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, E>>,
    C: FnOnce() -> CF,
    CF: Fiber<'d, Output = Result<reply::Retained, E>>,
    D: FnOnce() -> DF,
    DF: Fiber<'d, Output = Result<reply::Contiguous, E>>,
{
    check(Value::Scoped(
        runtime
            .block_on(warm)
            .map_err(Error::Drive)?
            .map_err(Error::Response)?,
    ));

    let (response, scoped) = Tracker::usage(|| runtime.block_on(scoped()));
    check(Value::Scoped(
        response.map_err(Error::Drive)?.map_err(Error::Response)?,
    ));

    let (response, retained) = Tracker::usage(|| runtime.block_on(retained()));
    check(Value::Retained(
        response.map_err(Error::Drive)?.map_err(Error::Response)?,
    ));

    let (response, flat) = Tracker::usage(|| runtime.block_on(flat()));
    check(Value::Contiguous(
        response.map_err(Error::Drive)?.map_err(Error::Response)?,
    ));

    Ok([scoped, retained, flat])
}

pub fn wire(chunk: NonZero<usize>) -> Vec<u8> {
    let chunk = chunk.get();
    let mut wire = Vec::with_capacity(chunk * 2 + 256);
    wire.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\nX-Origin: fixture\r\nTrailer: X-Checksum\r\n\r\n",
    );
    wire.extend_from_slice(format!("{chunk:x}\r\n").as_bytes());
    wire.resize(wire.len() + chunk, b'a');
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("{chunk:x}\r\n").as_bytes());
    wire.resize(wire.len() + chunk, b'b');
    wire.extend_from_slice(b"\r\n0\r\nX-Checksum: 42\r\n\r\n");
    wire
}
