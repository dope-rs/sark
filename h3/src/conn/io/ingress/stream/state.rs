use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::{Boundary, Section};
use sark_http::{
    self,
    field::metadata::{
        block::{self, Response},
        section::rules,
    },
    payload::response::Method,
};

use crate::{
    conn,
    conn::{
        event,
        io::ingress::{
            Parts,
            receive::Recv,
            stream::{Barrier, decoder},
        },
        push,
    },
    frame,
    qpack::{self, identity::Identity},
};

#[derive(Default)]
pub(in crate::conn) struct State {
    phase: event::Phase,
    content: block::Content,
    remaining: u64,
    // The front field section keeps its absolute RIC while QPACK or feedback waits.
    required: Option<qpack::Reference>,
    pub(super) method: Option<Method>,
    pub(super) data_barrier: Barrier,
}

impl State {
    pub(super) fn request(method: Method) -> Self {
        Self {
            method: Some(method),
            ..Self::default()
        }
    }

    fn receive(&self, stream: Id, bytes: usize, boundary: Boundary) -> Result<u64, conn::Error> {
        self.content
            .receive(self.remaining, bytes, matches!(boundary, Boundary::End))
            .map_err(|error| conn::Error::Content {
                stream_id: stream.get(),
                error,
            })
    }

    fn violation<const PUSH: bool>(
        &self,
        stream: Id,
        kind: u64,
        role: event::Role,
    ) -> event::Violation {
        let site = if PUSH {
            event::Site::Push(self.phase)
        } else {
            event::Site::Request(self.phase)
        };
        event::Violation::Receive(event::Frame {
            stream_id: stream.get(),
            kind,
            role,
            site,
        })
    }
}

pub(in crate::conn::io::ingress) fn ingest<
    'd,
    const PUSH: bool,
    P: rules::Policy,
    A: rules::Push,
>(
    parts: &mut Parts<'_, 'd, P, A>,
    stream: Id,
    recv: &mut Recv<'d>,
    state: &mut State,
    push_id: u64,
    boundary: Boundary,
    released: &mut usize,
) -> Result<Option<Id>, conn::Error> {
    if state.data_barrier == Barrier::Discarding {
        if parts.queues.events.is_full()
            || recv.has_credit() && parts.queues.receive_credits.is_full()
        {
            parts.park(stream, recv)?;
            return Ok(None);
        }
        *released = conn::try_add_credit(*released, recv.clear(&mut parts.ingress.budget)?)?;
        if matches!(boundary, Boundary::End) {
            parts.queues.try_event()?.push_back(event::Event::Finished {
                stream_id: recv.token,
            });
        }
        return Ok(None);
    }
    let mut emitted = Boundary::More;
    let mut wake = None;
    if !recv.buf.is_empty() || recv.decoder.is_pending() {
        if parts.queues.events.is_full()
            || recv.has_credit() && parts.queues.receive_credits.is_full()
        {
            parts.park(stream, recv)?;
            return Ok(None);
        }
        let Some(frame::Read {
            frame,
            consumed: n,
            next,
        }) = recv.decode_frame(stream, parts.settings.max_frame_size)?
        else {
            return Ok(None);
        };
        let edge = if matches!(boundary, Boundary::End) && n == crate::Source::len(&recv.buf) {
            Boundary::End
        } else {
            Boundary::More
        };
        let message = event::Message::new(recv.token, edge);
        match frame {
            frame::Decoded::Headers(block) => {
                let (next, section) = match state.phase {
                    event::Phase::Idle => (event::Phase::Headers, Section::Initial),
                    event::Phase::Headers | event::Phase::Data => {
                        (event::Phase::Trailers, Section::Trailing)
                    }
                    event::Phase::Trailers => {
                        return Err(conn::Error::FrameUnexpected(state.violation::<PUSH>(
                            stream,
                            frame::TYPE_HEADERS.get(),
                            parts.settings.role,
                        )));
                    }
                };
                let kind = match (PUSH, parts.settings.role, section) {
                    (false, event::Role::Server, Section::Initial) => rules::Kind::Request,
                    (false, event::Role::Server, Section::Trailing) => rules::Kind::RequestTrailers,
                    (false, event::Role::Client, Section::Initial)
                    | (true, event::Role::Client | event::Role::Server, Section::Initial) => {
                        rules::Kind::Response
                    }
                    (false, event::Role::Client, Section::Trailing)
                    | (true, event::Role::Client | event::Role::Server, Section::Trailing) => {
                        rules::Kind::ResponseTrailers
                    }
                };
                let decoded =
                    decoder::decode(parts, stream, &block, kind, None, &mut state.required)?;
                let (head, response) = match decoded {
                    decoder::Decoded::Ready { head, response, .. } => (head, response),
                    decoder::Decoded::Blocked => return Ok(None),
                    decoder::Decoded::FeedbackBlocked => {
                        parts.defer(stream, recv)?;
                        return Ok(None);
                    }
                    decoder::Decoded::RejectPush => {
                        return Err(conn::Error::PushRejected {
                            stream_id: stream.get(),
                        });
                    }
                };
                let next = match response {
                    Some(Response::Informational) => {
                        if matches!(edge, Boundary::End) {
                            return Err(conn::Error::MessageMissing {
                                stream_id: stream.get(),
                            });
                        }
                        event::Phase::Idle
                    }
                    Some(Response::Final) | None => next,
                };
                let (content, remaining, method) =
                    match section {
                        Section::Initial => {
                            let framing = head.framing().ok_or(conn::Error::Malformed {
                                stream_id: stream.get(),
                                kind,
                            })?;
                            let method = framing.method().or(state.method).ok_or(
                                conn::Error::Malformed {
                                    stream_id: stream.get(),
                                    kind,
                                },
                            )?;
                            let (content, remaining) =
                                framing
                                    .content(method)
                                    .map_err(|error| conn::Error::Content {
                                        stream_id: stream.get(),
                                        error,
                                    })?;
                            let remaining = content
                                .receive(remaining, 0, matches!(edge, Boundary::End))
                                .map_err(|error| conn::Error::Content {
                                    stream_id: stream.get(),
                                    error,
                                })?;
                            (content, remaining, Some(method))
                        }
                        Section::Trailing => {
                            if state.content == block::Content::Tunnel {
                                return Err(conn::Error::FrameUnexpected(state.violation::<PUSH>(
                                    stream,
                                    frame::TYPE_HEADERS.get(),
                                    parts.settings.role,
                                )));
                            }
                            state.content.trailers(state.remaining).map_err(|error| {
                                conn::Error::Content {
                                    stream_id: stream.get(),
                                    error,
                                }
                            })?;
                            (state.content, state.remaining, state.method)
                        }
                    };
                let vacancy = parts.queues.try_event()?;
                state.phase = next;
                if response != Some(Response::Informational) {
                    state.content = content;
                    state.remaining = remaining;
                }
                state.method = method;
                let event = if PUSH {
                    event::Event::PushHeaders {
                        message,
                        push_id,
                        head,
                        section,
                    }
                } else {
                    event::Event::Headers {
                        message,
                        head,
                        section,
                    }
                };
                vacancy.push_back(event);
                emitted = edge;
            }
            frame::Decoded::Data(data) => {
                if !matches!(state.phase, event::Phase::Headers | event::Phase::Data) {
                    return Err(conn::Error::FrameUnexpected(state.violation::<PUSH>(
                        stream,
                        frame::TYPE_DATA.get(),
                        parts.settings.role,
                    )));
                }
                let remaining = state.receive(stream, data.len(), edge)?;
                if data.is_empty() && next.is_pending() {
                    recv.consume(n, released, &mut parts.ingress.budget)?;
                    recv.decoder = next;
                    state.phase = event::Phase::Data;
                    return Ok(None);
                }
                let vacancy = parts.queues.try_event()?;
                state.phase = event::Phase::Data;
                let (data, ready) = recv.take_payload(
                    data,
                    n,
                    parts.ingress.data_credit,
                    &mut parts.ingress.budget,
                )?;
                recv.decoder = next;
                *released = conn::try_add_credit(*released, ready)?;
                state.remaining = remaining;
                let event = if PUSH {
                    event::Event::PushData {
                        message,
                        push_id,
                        data,
                    }
                } else {
                    event::Event::Data { message, data }
                };
                vacancy.push_back(event);
                if matches!(parts.ingress.data_credit, conn::CreditMode::Held)
                    && matches!(edge, Boundary::More)
                {
                    state.data_barrier = Barrier::Queued;
                    return Ok(None);
                }
                if !recv.buf.is_empty() {
                    parts.park(stream, recv)?;
                }
                return Ok(None);
            }
            frame::Decoded::Unknown { .. } => {}
            frame::Decoded::PushPromise { push_id, block } => {
                if PUSH
                    || parts.settings.role != event::Role::Client
                    || state.content == block::Content::Tunnel
                {
                    return Err(conn::Error::FrameUnexpected(state.violation::<PUSH>(
                        stream,
                        frame::TYPE_PUSH_PROMISE.get(),
                        parts.settings.role,
                    )));
                }
                if matches!(edge, Boundary::End) {
                    state.receive(stream, 0, edge)?;
                }
                push::check(push_id.get(), parts.session.max_push_id).map_err(conn::Error::Push)?;
                let mut identity = Identity::new();
                let decoded = decoder::decode(
                    parts,
                    stream,
                    &block,
                    rules::Kind::PushPromise,
                    Some(&mut identity),
                    &mut state.required,
                )?;
                match decoded {
                    decoder::Decoded::Ready { head, .. } => {
                        let method = head.framing().and_then(block::Framing::method).ok_or(
                            conn::Error::Malformed {
                                stream_id: stream.get(),
                                kind: rules::Kind::PushPromise,
                            },
                        )?;
                        wake = parts
                            .session
                            .pushes
                            .promised(push_id, identity, method)
                            .map_err(conn::Error::Push)?;
                        parts
                            .queues
                            .try_event()?
                            .push_back(event::Event::PushPromise {
                                message,
                                push_id: push_id.get(),
                                head,
                            });
                        emitted = edge;
                    }
                    decoder::Decoded::Blocked => return Ok(None),
                    decoder::Decoded::FeedbackBlocked => {
                        parts.defer(stream, recv)?;
                        return Ok(None);
                    }
                    decoder::Decoded::RejectPush => {
                        parts.cancel_push(push_id)?;
                    }
                }
            }
            frame => {
                return Err(conn::Error::FrameUnexpected(state.violation::<PUSH>(
                    stream,
                    frame.kind(),
                    parts.settings.role,
                )));
            }
        }
        recv.consume(n, released, &mut parts.ingress.budget)?;
        state.required = None;
        recv.decoder = next;
    }
    if !recv.buf.is_empty() {
        parts.park(stream, recv)?;
        return Ok(wake);
    }
    if matches!(boundary, Boundary::End) {
        if state.phase == event::Phase::Idle {
            return Err(conn::Error::MessageMissing {
                stream_id: stream.get(),
            });
        }
        if matches!(emitted, Boundary::More) {
            if parts.queues.events.is_full() {
                parts.park(stream, recv)?;
                return Ok(wake);
            }
            state.receive(stream, 0, Boundary::End)?;
            parts.queues.try_event()?.push_back(event::Event::Finished {
                stream_id: recv.token,
            });
        }
    }
    Ok(wake)
}
