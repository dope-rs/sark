use sark_client::protocol::{self, codec::output, response};

pub fn only<'d>(
    output: output::Output<'d>,
) -> Option<Result<response::Event<'d>, protocol::Error>> {
    let mut events = output.into_events();
    let event = events.next()?;
    events.next().is_none().then_some(event)
}
