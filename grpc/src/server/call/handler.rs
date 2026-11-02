/// Co-located protocol and execution state for one live call.
#[doc(hidden)]
pub struct Handler<Protocol, Execution> {
    protocol: Protocol,
    execution: Execution,
}

impl<Protocol, Execution> Handler<Protocol, Execution> {
    pub(in crate::server) fn new(protocol: Protocol, state: Execution) -> Self {
        Self {
            protocol,
            execution: state,
        }
    }

    pub(in crate::server) fn split(&mut self) -> (&mut Protocol, &mut Execution) {
        (&mut self.protocol, &mut self.execution)
    }
}
