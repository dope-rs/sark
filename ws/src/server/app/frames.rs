use o3::buffer::write::{Sink as _, consume, consume::Consume};

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

    pub(super) fn append<P>(&self, state: &mut server::ConnState<P>, chunk: &[u8]) -> bool {
        let buffered = state.acc.len();
        if let Err(error) = consume::Error::check(chunk.len(), server::WS_MAX_ACC - buffered) {
            if matches!(
                state.phase,
                server::Phase::Handshake | server::Phase::Opening
            ) {
                self.user.failed(opening::too_large(buffered + chunk.len()));
                state.phase = server::Phase::Closed;
            } else {
                state.phase = server::Phase::MessageTooBig;
                state.acc.clear();
                self.user.failed(server::Error::Capacity(error));
            }
            return false;
        }
        if let Err(error) = state.acc.write_slice(chunk) {
            self.user.failed(server::Error::Allocate(error));
            state.phase = server::Phase::Closed;
            return false;
        }
        true
    }

    pub(super) fn pump<P>(&self, state: &mut server::ConnState<P>, out: &mut Output<'_>) {
        match state.phase {
            server::Phase::Active => self.drive(state, out),
            server::Phase::Rejected(code) if out.response.close_code(code as u16).is_ok() => {
                state.phase = server::Phase::Closed;
            }
            _ => {}
        }
        if state.phase == server::Phase::MessageTooBig {
            if let Err(error) = out.response.close_code(1009) {
                self.user.failed(server::Error::Capacity(error));
            }
            state.phase = server::Phase::Closed;
        }
    }

    pub(super) fn drive<P>(&self, state: &mut server::ConnState<P>, out: &mut Output<'_>) {
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

    pub(super) fn chunk<P>(
        &self,
        temp: &mut Vec<u8>,
        state: &mut server::ConnState<P>,
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
            let result = src.read(input, self.limit, |prepared| match prepared {
                inbound::Prepared::Message(message) => {
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
                inbound::Prepared::Fragment(fragment) => {
                    fragment.commit();
                    sources::Mask::Commit(server::Phase::Active)
                }
                inbound::Prepared::Pong(pong) => {
                    pong.commit();
                    sources::Mask::Commit(server::Phase::Active)
                }
                inbound::Prepared::Required(need) => {
                    let next = match need.commit_with(|reply| out.response.reply(reply)) {
                        Ok(inbound::Event::Close(_)) => server::Phase::Closed,
                        Ok(inbound::Event::Ping(_)) => server::Phase::Active,
                        Err(_) => return sources::Mask::Restore(server::Phase::Active),
                    };
                    sources::Mask::Commit(next)
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
        let code = error.code();
        let result = out.close_code(code as u16);
        self.user.failed(server::Error::Frame(error));
        if result.is_ok() {
            server::Phase::Closed
        } else {
            server::Phase::Rejected(code)
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
