use sark_client::protocol::{self, codec::output, response};

pub struct Output<'d> {
    output: output::Output<'d>,
}

impl<'d> Output<'d> {
    pub const fn new(output: output::Output<'d>) -> Self {
        Self { output }
    }

    pub fn only(self) -> Option<Result<response::Event<'d>, protocol::Error>> {
        let mut events = self.output.into_events();
        let event = events.next()?;
        events.next().is_none().then_some(event)
    }
}
