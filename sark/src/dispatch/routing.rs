use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit};

pub trait Routing {
    fn try_consume(
        &mut self,
        permit: DispatchPermit,
        bytes: &[u8],
        write: &mut [u8],
        conn: &mut ConnState,
    ) -> ConsumeOutcome;
}
