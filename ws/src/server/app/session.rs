use sark_core::duplex::{self, Action};
use sark_http::payload::{owned, response::Body as _};

use crate::server::{ConnState, Phase, WS_MAX_ACC, WS_MAX_FRAME, output::Output};

use super::{Sink, frames::Frames};

/// The existing frame engine after a carrier commits its opening response.
pub struct Session<H: Sink> {
    user: H,
    state: ConnState<()>,
    limit: usize,
}

mod sealed {
    pub trait Session {}
}

impl<H: Sink> sealed::Session for Session<H> {}

pub trait Upgrade: sealed::Session {
    type Handler: Sink;

    fn handler(&self) -> &Self::Handler;
}

impl<H: Sink> Upgrade for Session<H> {
    type Handler = H;

    fn handler(&self) -> &H {
        &self.user
    }
}

impl<H: Sink> Session<H> {
    pub fn new(user: H, max_frame_payload: usize, max_message_size: usize) -> Self {
        Self {
            user,
            state: ConnState {
                phase: Phase::Active,
                ..ConnState::new(max_message_size)
            },
            limit: max_frame_payload.min(WS_MAX_FRAME),
        }
    }

    fn complete<'d>(&mut self, out: Output<'_>) -> (usize, Action<'d>) {
        let state = &mut self.state;
        let (written, body) = out.into_parts(state);
        let resume = matches!(state.phase, Phase::MessageTooBig | Phase::Rejected(_))
            || (state.phase == Phase::Active
                && ((body.is_some() && (state.eof || !state.acc.is_empty()))
                    || state
                        .inbound
                        .required()
                        .is_some_and(|need| need <= state.acc.len())));
        let action = match (state.phase == Phase::Closed, body) {
            (true, Some(body)) => Action::send_close(body),
            (false, Some(body)) => Action::send(body),
            (true, None) => Action::close(),
            (false, None) => Action::idle(),
        };
        (written, if resume { action.activate() } else { action })
    }
}

impl<'d, H: Sink> duplex::Session<'d> for Session<H> {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        output: &mut [u8],
        scratch: &mut Vec<u8>,
    ) -> (usize, Action<'d>) {
        if self.state.phase != Phase::Active {
            return self.activate(output, scratch);
        }
        let mut out = Output::new(output);
        let frames = Frames::new(&self.user, self.limit);
        for chunk in input.segments() {
            if self.state.phase != Phase::Active {
                break;
            }
            if self.state.acc.is_empty() && chunk.len() <= WS_MAX_ACC {
                self.state.phase = frames.chunk(scratch, &mut self.state, chunk, &mut out);
            } else if frames.append(&mut self.state, chunk) && out.body.is_none() {
                frames.drive(&mut self.state, &mut out);
            }
        }
        self.complete(out)
    }

    fn finish(&mut self, output: &mut [u8], scratch: &mut Vec<u8>) -> (usize, Action<'d>) {
        self.state.eof = true;
        self.activate(output, scratch)
    }

    fn activate(&mut self, output: &mut [u8], _: &mut Vec<u8>) -> (usize, Action<'d>) {
        let mut out = Output::new(output);
        Frames::new(&self.user, self.limit).pump(&mut self.state, &mut out);
        self.complete(out)
    }
}
