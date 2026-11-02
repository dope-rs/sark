use sark_core::life::session;

use crate::{
    protocol::request,
    server::{call::input, service::handler},
};

enum State<T> {
    Retained {
        start: handler::Start<T>,
        head: request::Head,
    },
    Live(T),
    Producing(T),
}

/// Opening result and exact request-head owner disposition.
#[doc(hidden)]
pub struct Opened<T>(State<T>);

impl<T> Opened<T> {
    pub(in crate::server) fn retained(start: handler::Start<T>, head: request::Head) -> Self {
        Self(State::Retained { start, head })
    }

    pub(in crate::server) fn live(call: T) -> Self {
        Self(State::Live(call))
    }

    pub(in crate::server) fn producing(call: T) -> Self {
        Self(State::Producing(call))
    }

    pub(in crate::server) fn into_input<C: session::Storage>(
        self,
        limit: usize,
    ) -> (T, handler::Production, input::Input<C>) {
        let (call, output, request) = match self.0 {
            State::Retained { start, head } => {
                let request = match start.mode {
                    handler::StreamMode::Live => input::Request::Live,
                    handler::StreamMode::Buffered => input::Request::Buffered(head),
                };
                (start.call, start.output, request)
            }
            State::Live(call) => (call, handler::Production::Idle, input::Request::Live),
            State::Producing(call) => (call, handler::Production::Ready, input::Request::Live),
        };
        (call, output, input::Input::new(request, limit))
    }
}
