use core::ops::ControlFlow;

use conn::{
    event, ingress,
    ingress::{header, headers},
    registry,
};
use ingress::events;
use registry::store;
use sark_core::{
    life::session,
    manifold::stream::{Boundary, Section},
};
use sark_http::{
    field::metadata::{
        block,
        section::{rules, state::Selected},
    },
    payload::response::Method,
};

use crate::{
    Endpoint, conn,
    conn::machine::{self, decoder, ops::state},
    frame::{self, numbers},
    stream,
};

pub(super) mod chain;
pub(super) mod promise;

enum Opening<'s, 'h, R: crate::Role> {
    Open(store::Vacant<'s>, registry::Peer<'h, R>),
    Refuse(registry::Peer<'h, R>),
    Reset(numbers::ErrorCode),
}

/// Terminal disposition of one admitted header block.
///
/// Not `Copy`: the terminal arms must *move* the payload out of the admission flow so the
/// store borrow it holds ends before the removal mutates that same store.
enum Outcome<'a, P: rules::Policy, C: session::Storage, E: event::Evidence> {
    Done,
    Reset(
        stream::Key,
        numbers::ErrorCode,
        Option<header::Reset<'a, P, C, E>>,
    ),
}

fn complete<'a, R, F, C, E>(
    key: stream::Key,
    record: &mut registry::Active,
    prepared: header::Prepared<'a, F, C, E>,
    boundary: Boundary,
    section: Section,
) -> Result<store::Action<Outcome<'a, F, C, E>>, conn::Error>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    let interim = matches!(prepared.response(), Some(block::Response::Informational));
    let next = if matches!(prepared.validation(), header::Validation::Rejected { .. })
        || interim && matches!(boundary, Boundary::End)
    {
        Err(numbers::ErrorCode::ProtocolError)
    } else {
        match record
            .stream
            .state
            .recv(stream::Event::Headers { boundary })
        {
            Ok(next) => Ok(next),
            Err(error @ stream::Error::Protocol { .. }) => {
                prepared.discard();
                return Err(conn::Error::Transition(key.id(), error));
            }
            Err(stream::Error::StreamClosed { .. }) => Err(numbers::ErrorCode::StreamClosed),
        }
    };
    let next = next.and_then(|next| {
        let (content, remaining) = match section {
            Section::Trailing => {
                record.stream.content.trailers(record.remaining)?;
                (record.stream.content, record.remaining)
            }
            Section::Initial if interim => (record.stream.content, record.remaining),
            Section::Initial => {
                let (content, remaining) =
                    prepared.content(record.stream.method.unwrap_or(Method::Other))?;
                (
                    content,
                    content.receive(remaining, 0, matches!(boundary, Boundary::End))?,
                )
            }
        };
        Ok((next, content, remaining))
    });
    let (next, content, remaining) = match next {
        Ok(next) => next,
        Err(error) => {
            let notify = match R::ENDPOINT {
                Endpoint::Client => true,
                Endpoint::Server => matches!(record.stream.peer_section, Section::Trailing),
            };
            let reset = if notify {
                Some(prepared.reject())
            } else {
                prepared.discard();
                None
            };
            return Ok(store::Action::Reset(
                error,
                Outcome::Reset(key, error, reset),
            ));
        }
    };
    record.stream.state = next;
    record.stream.content = content;
    record.remaining = remaining;
    if matches!((R::ENDPOINT, section), (Endpoint::Server, Section::Initial)) {
        record.stream.method = prepared.method();
    }
    if !interim {
        record.stream.peer_section = Section::Trailing;
    }
    prepared.commit(key, boundary, section);
    Ok(if next == stream::State::Closed {
        store::Action::Remove(Outcome::Done)
    } else {
        store::Action::Keep(Outcome::Done)
    })
}

impl<R, F, C, A, E> decoder::Decoder<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    fn prepare<'a>(
        input: &'a mut header::Input<'_, '_, C>,
        events: &'a mut events::Events<Selected<F>, F::Block, C, E>,
        headers: &mut headers::Blocks<F>,
        start: usize,
        len: usize,
        total: usize,
        section: Section,
    ) -> Result<header::Prepared<'a, F, C, E>, conn::Error> {
        headers.prepare::<C, A, E>(
            input,
            events,
            headers::Fragment {
                start,
                len,
                total,
                kind: R::ENDPOINT.section(section),
            },
        )
    }

    /// Dispatches one admitted header block, releasing the admission borrow
    /// before a terminal outcome mutates the store.
    pub(in crate::conn::machine) fn handle(
        &mut self,
        stream: stream::Nonzero,
        mode: (Boundary, impl Into<frame::Block>),
        start: usize,
        len: usize,
        total: usize,
        borrowed: Option<(&[u8], &mut bool)>,
    ) -> Result<(), conn::Error> {
        let (boundary, chain) = mode;
        let chain = chain.into();
        let id = stream.get();
        let machine::Connection {
            ingress,
            egress,
            state,
            ..
        } = &mut *self.conn;
        let state::State {
            settings: state::Settings { local, peer },
            lifecycle: life,
            active_streams: active,
            stream_history: history,
            ..
        } = &mut *state;
        let ingress::Ingress {
            input,
            events,
            headers,
            ..
        } = ingress;
        let mut input = match borrowed {
            Some((bytes, consumed)) => header::Input::Borrowed { bytes, consumed },
            None => header::Input::Queued(input),
        };
        let limit = match local.max_concurrent_streams {
            Some(limit) => limit as usize,
            None => usize::MAX,
        };
        let outcome;
        'open: {
            let admitted = active.admit(history, stream, limit, |key, record| {
                let section = record.stream.peer_section;
                let action = if matches!((section, boundary), (Section::Trailing, Boundary::More)) {
                    headers::Action::Reset(numbers::ErrorCode::ProtocolError)
                } else {
                    headers::Action::Deliver
                };
                if matches!(chain, frame::Block::More) {
                    let pending = headers::Pending {
                        stream_id: stream,
                        kind: headers::Kind::Headers {
                            boundary,
                            section,
                            action,
                        },
                        continuations: headers::Continuation::ZERO,
                    };
                    let kind = match action {
                        headers::Action::Deliver => Some(R::ENDPOINT.section(section)),
                        headers::Action::Reset(_) => None,
                    };
                    headers.begin::<C, A>(
                        kind,
                        pending,
                        &mut input,
                        headers::Fragment {
                            start,
                            len,
                            total,
                            kind: R::ENDPOINT.section(section),
                        },
                    )?;
                    return Ok(store::Action::Keep(Outcome::Done));
                }
                if let headers::Action::Reset(error) = action {
                    let vacancy = events.vacant()?;
                    headers.discard(
                        &mut input,
                        headers::Fragment {
                            start,
                            len,
                            total,
                            kind: R::ENDPOINT.section(section),
                        },
                    )?;
                    let reset = events::Reset::new(vacancy, None);
                    return Ok(store::Action::Reset(
                        error,
                        Outcome::Reset(key, error, Some(reset)),
                    ));
                }
                let prepared =
                    Self::prepare(&mut input, events, headers, start, len, total, section)?;
                complete::<R, _, _, _>(key, record, prepared, boundary, section)
            })?;
            let flow = match admitted {
                store::Admission::Active(done) => ControlFlow::Break(done),
                store::Admission::Exhausted(id) => {
                    return Err(conn::Error::Admission(conn::Admission::IdentityExhausted(
                        id,
                    )));
                }
                store::Admission::Full(_) | store::Admission::Open(_, _)
                    if matches!(R::ENDPOINT, Endpoint::Client) =>
                {
                    return Err(conn::Error::Protocol(conn::Protocol::Idle(id)));
                }
                store::Admission::Past(registry::Past::Closed) => {
                    ControlFlow::Continue(Opening::Reset(numbers::ErrorCode::StreamClosed))
                }
                store::Admission::Past(registry::Past::Idle) => {
                    return Err(conn::Error::Protocol(conn::Protocol::Peer {
                        actual: id,
                        last: history.last_peer_id(),
                        endpoint: R::ENDPOINT,
                    }));
                }
                store::Admission::Full(opening) => ControlFlow::Continue(Opening::Refuse(opening)),
                store::Admission::Open(vacant, opening) => {
                    if life.is_goaway() {
                        ControlFlow::Continue(Opening::Refuse(opening))
                    } else {
                        ControlFlow::Continue(Opening::Open(vacant, opening))
                    }
                }
            };
            let opening = match flow {
                ControlFlow::Continue(opening) => opening,
                ControlFlow::Break(done) => {
                    outcome = done;
                    break 'open;
                }
            };
            let action = match &opening {
                Opening::Open(_, _) => headers::Action::Deliver,
                Opening::Refuse(_) => headers::Action::Reset(numbers::ErrorCode::RefusedStream),
                Opening::Reset(error) => headers::Action::Reset(*error),
            };
            if matches!(chain, frame::Block::More) {
                let pending = headers::Pending {
                    stream_id: stream,
                    kind: headers::Kind::Headers {
                        boundary,
                        section: Section::Initial,
                        action,
                    },
                    continuations: headers::Continuation::ZERO,
                };
                let kind = match action {
                    headers::Action::Deliver => Some(R::ENDPOINT.section(Section::Initial)),
                    headers::Action::Reset(_) => None,
                };
                headers.begin::<C, A>(
                    kind,
                    pending,
                    &mut input,
                    headers::Fragment {
                        start,
                        len,
                        total,
                        kind: R::ENDPOINT.section(Section::Initial),
                    },
                )?;
                match opening {
                    Opening::Open(vacant, opening) => {
                        vacant.insert(
                            stream::Lifecycle::new(),
                            peer.initial_window_size,
                            local.initial_window_size,
                            0,
                        );
                        opening.commit();
                    }
                    Opening::Refuse(opening) => opening.commit(),
                    Opening::Reset(_) => {}
                }
                return Ok(());
            }
            let (vacant, opening) = match opening {
                Opening::Open(vacant, opening) => (vacant, opening),
                Opening::Refuse(opening) => {
                    headers.discard(
                        &mut input,
                        headers::Fragment {
                            start,
                            len,
                            total,
                            kind: R::ENDPOINT.section(Section::Initial),
                        },
                    )?;
                    opening.commit();
                    egress
                        .outbound
                        .reset(id, numbers::ErrorCode::RefusedStream)?;
                    return Ok(());
                }
                Opening::Reset(error) => {
                    headers.discard(
                        &mut input,
                        headers::Fragment {
                            start,
                            len,
                            total,
                            kind: R::ENDPOINT.section(Section::Initial),
                        },
                    )?;
                    egress.outbound.reset(id, error)?;
                    return Ok(());
                }
            };
            let prepared = Self::prepare(
                &mut input,
                events,
                headers,
                start,
                len,
                total,
                Section::Initial,
            )?;
            match prepared.validation() {
                header::Validation::Valid => {}
                header::Validation::Rejected { .. } => {
                    prepared.discard();
                    opening.commit();
                    egress
                        .outbound
                        .reset(id, numbers::ErrorCode::ProtocolError)?;
                    return Ok(());
                }
            }
            let content = prepared
                .content(Method::Other)
                .and_then(|(content, remaining)| {
                    Ok((
                        content,
                        content.receive(remaining, 0, matches!(boundary, Boundary::End))?,
                    ))
                });
            let (content, remaining) = match content {
                Ok(content) => content,
                Err(error) => {
                    prepared.discard();
                    opening.commit();
                    egress.outbound.reset(id, error)?;
                    return Ok(());
                }
            };
            let key = vacant.insert(
                stream::Lifecycle::open_remote(boundary, prepared.method(), content),
                peer.initial_window_size,
                local.initial_window_size,
                remaining,
            );
            opening.commit();
            prepared.commit(key, boundary, Section::Initial);
            outcome = Outcome::Done;
        }
        match outcome {
            Outcome::Done => {}
            Outcome::Reset(key, error, notify) => {
                state.finish_reset(egress, key, error, notify)?;
            }
        }
        Ok(())
    }
}
