use std::ops;

use o3::buffer::write::consume::Consume;

use crate::{
    fragment, inbound,
    server::{
        self,
        app::{Sink, opening},
        output::Output,
        sources,
    },
};

pub(super) struct Frames<'a, H> {
    user: &'a H,
    limit: usize,
}

impl<'a, H: Sink> Frames<'a, H> {
    pub(super) const fn new(user: &'a H, limit: usize) -> Self {
        Self { user, limit }
    }

    pub(super) fn pump<'b>(
        &self,
        path: &str,
        state: &mut server::ConnState,
        buffer: &'b mut [u8],
    ) -> Output<'b> {
        let mut out = Output::new(buffer);
        if state.phase == server::Phase::Handshake
            && let Err(error) = opening::drive(path, state, &mut out.response, self.user)
        {
            self.user.failed(server::Error::Capacity(error));
            state.phase = server::Phase::Closed;
        }
        match state.phase {
            server::Phase::Active => self.drive(state, &mut out),
            server::Phase::Rejected(error) => {
                state.phase = self.reject_frame(&mut out.response, error);
            }
            _ => {}
        }
        if state.phase == server::Phase::MessageTooBig {
            if let Err(error) = out.response.close_code(1009) {
                self.user.failed(server::Error::Capacity(error));
            }
            state.phase = server::Phase::Closed;
        }
        out
    }

    pub(super) fn drive(&self, state: &mut server::ConnState, out: &mut Output<'_>) {
        let mut bytes = match state.acc.try_as_mut() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.user.failed(server::Error::Allocate(error));
                state.phase = server::Phase::Closed;
                return;
            }
        };
        let before = bytes.len();
        let phase = self.drive_source(&mut bytes, &mut state.inbound, out);
        let consumed = before - bytes.len();
        state.acc.consume_max(consumed);
        if phase != server::Phase::Active {
            state.phase = phase;
        }
    }

    pub(super) fn chunk(
        &self,
        temp: &mut Vec<u8>,
        state: &mut server::ConnState,
        chunk: &[u8],
        out: &mut Output<'_>,
    ) -> server::Phase {
        let source = sources::Borrowed {
            chunk,
            scratch: temp,
            acc: &mut state.acc,
        };
        self.drive_source(source, &mut state.inbound, out)
    }

    /// Discards terminal input without allocating after its cause has been retained.
    fn drive_source<S: sources::Frame>(
        &self,
        mut src: S,
        input: &mut inbound::Inbound<inbound::ClientPeer>,
        out: &mut Output<'_>,
    ) -> server::Phase {
        out.response.reserve();
        let mut phase = server::Phase::Active;
        loop {
            if out.body.is_some() {
                break;
            }
            let result = src.read(input, self.limit, |frame, data| {
                let prepared = match frame.prepare(data) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        return sources::Mask::Restore(self.reject_frame(&mut out.response, error));
                    }
                };
                match prepared.split() {
                    inbound::Split::Message(message) => {
                        let result = self.user.capacity(message.len()).and_then(|capacity| {
                            out.try_message(capacity, |response| {
                                self.dispatch(message.commit(), response)
                            })
                        });
                        match result {
                            Ok(Some(next)) => sources::Mask::Commit(next),
                            Ok(None) => sources::Mask::Restore(server::Phase::Active),
                            Err(error) => {
                                self.user.failed(error);
                                sources::Mask::Commit(server::Phase::Closed)
                            }
                        }
                    }
                    inbound::Split::Control(control) => {
                        let next = match control.commit_with(|reply| out.response.reply(reply)) {
                            Ok(ops::ControlFlow::Break(())) => server::Phase::Closed,
                            Ok(ops::ControlFlow::Continue(())) => server::Phase::Active,
                            Err(_) => return sources::Mask::Restore(server::Phase::Active),
                        };
                        sources::Mask::Commit(next)
                    }
                }
            });
            let result = match result {
                Ok(Some(result)) => result,
                Ok(None) => break,
                Err(server::Error::Frame(error)) => {
                    phase = self.reject_frame(&mut out.response, error);
                    break;
                }
                Err(error) => {
                    self.user.failed(error);
                    phase = server::Phase::Closed;
                    break;
                }
            };
            match result {
                sources::Mask::Commit(server::Phase::Active) => {}
                sources::Mask::Commit(next) | sources::Mask::Restore(next) => {
                    phase = next;
                    break;
                }
            }
        }
        if let Err(error) = src.finish(phase != server::Phase::Active) {
            self.user.failed(error);
            phase = server::Phase::Closed;
        }
        phase
    }

    fn reject_frame(&self, out: &mut server::Response<'_>, error: inbound::Fault) -> server::Phase {
        if out.close_code(error.close_code()).is_ok() {
            self.user.failed(server::Error::Frame(error));
            server::Phase::Closed
        } else {
            server::Phase::Rejected(error)
        }
    }

    fn dispatch(
        &self,
        message: fragment::Message<'_>,
        out: &mut server::Response<'_>,
    ) -> Result<server::Phase, server::Error> {
        let view = match &message {
            fragment::Message::WholeText(text) => server::Message::Text(text),
            fragment::Message::WholeBinary(data) => server::Message::Binary(data),
            fragment::Message::JoinedText(text) => server::Message::Text(text),
            fragment::Message::JoinedBinary(data) => server::Message::Binary(data),
        };
        let control = self.user.message(view, out)?;
        Ok(if control.is_break() {
            server::Phase::Closed
        } else {
            server::Phase::Active
        })
    }
}
