use std::pin::Pin;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit};

pub trait Routing {
    fn try_consume(
        self: Pin<&mut Self>,
        permit: DispatchPermit,
        bytes: &[u8],
        write: &mut [u8],
        conn: &mut ConnState,
    ) -> ConsumeOutcome;
}
