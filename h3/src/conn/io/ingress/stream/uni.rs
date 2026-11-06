use core::marker;

use dope_quic::wire::{stream::Id, varint};
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn::io::{
        Resume,
        ingress::{
            Parts,
            stream::{Barrier, control, push, table},
        },
        write::feedback::{Feedback, FeedbackMut},
    },
    conn::{self, event},
    frame, link,
    qpack::{self, decoder::state::Status},
    stream,
};

const QPACK_ENCODER_QUANTUM: usize = 4 * 1024;

pub(super) fn resume<'d, P: rules::Policy, A: rules::Push>(
    conn: &mut conn::Connection<'d, P, A>,
    id: Id,
    resume: Resume,
) -> Result<link::Status, conn::Error> {
    let conn::Connection {
        settings,
        qpack,
        session,
        streams,
        queues,
        ingress,
        ..
    } = conn;
    let mut parts: Parts<'_, 'd, P, A> = Parts {
        settings,
        qpack,
        session,
        queues,
        ingress,
        push: marker::PhantomData,
    };
    if let Some(recv) = streams.recv_mut(id)
        && !recv.buf.is_empty()
        && recv.has_credit()
        && parts.queues.receive_credits.is_full()
    {
        parts.park(id, recv)?;
        return Ok(link::Status::Suspended);
    }
    let mut released = 0;
    let mut requeue = false;
    let mut wake = None;
    let mut barrier = false;
    let (boundary, kind) = loop {
        match streams.uni_mut(id).ok_or(conn::Error::StreamMissing {
            stream_id: id.get(),
        })? {
            table::Uni::Unclassified(mut stream) => {
                let recv = stream.recv();
                let (kind, width) = match frame::decode_varint_source(&recv.buf) {
                    Ok(value) => value,
                    Err(
                        frame::ParseError::NeedMore
                        | frame::ParseError::VarInt(varint::Error::Underflow { .. }),
                    ) => match recv.boundary {
                        Boundary::End => {
                            released = recv.clear(&mut parts.ingress.budget)?;
                            break (Boundary::End, stream::UniType::Unknown(0));
                        }
                        Boundary::More => return Ok(link::Status::Open),
                    },
                    Err(error) => return Err(conn::Error::Parse(error)),
                };
                let kind = stream::UniType::from_wire(kind);
                recv.consume(width, &mut released, &mut parts.ingress.budget)?;
                stream.classify(kind, parts.settings.role)?;
            }
            table::Uni::Control(recv) => {
                let boundary = recv.boundary;
                control::Control::new(&mut parts).ingest(id, recv, boundary, &mut released)?;
                break (boundary, stream::UniType::Control);
            }
            table::Uni::Push(recv, state) => {
                let boundary = recv.boundary;
                wake = match push::ingest(&mut parts, id, recv, state, boundary, &mut released) {
                    Ok(wake) => wake,
                    Err(error) => {
                        parts.release(id, released, Resume::Idle)?;
                        return Err(error);
                    }
                };
                barrier = state.is_waiting()
                    || matches!(
                        state.message.data_barrier,
                        Barrier::Queued | Barrier::Held | Barrier::Rejected
                    );
                break (boundary, stream::UniType::Push);
            }
            table::Uni::QpackEncoder(recv) => {
                let fragment = match recv.buf.front() {
                    Some(fragment) => fragment,
                    None => &[],
                };
                let take = fragment.len().min(QPACK_ENCODER_QUANTUM);
                let fragment = fragment.split_at(take).0;
                let limit =
                    Feedback::new(&parts.qpack.feedback, parts.session, parts.queues).limit();
                let decoder = &mut parts.qpack.qpack_decoder;
                let feedback = &mut parts.qpack.feedback;
                let progress = FeedbackMut::new(feedback, &mut parts.queues.write_budget)
                    .try_ingest(decoder, fragment, limit)?;
                if progress.consumed > 0 {
                    recv.consume(progress.consumed, &mut released, &mut parts.ingress.budget)?;
                    requeue = true;
                }
                if matches!(progress.insertion, crate::qpack::Insertion::Committed) {
                    wake = parts.wake();
                }
                if matches!(progress.feedback, Status::Blocked(_)) {
                    parts.defer(id, recv)?;
                }
                if matches!(recv.boundary, Boundary::End) {
                    return Err(conn::Error::ClosedCriticalStream(id.get()));
                }
                break (recv.boundary, stream::UniType::QpackEncoder);
            }
            table::Uni::QpackDecoder(recv) => {
                const MAX_INSTRUCTION: usize = 11;
                if !recv.buf.is_empty() {
                    let len = crate::Source::len(&recv.buf).min(MAX_INSTRUCTION);
                    let mut bytes = [0u8; MAX_INSTRUCTION];
                    let target = bytes.split_at_mut(len).0;
                    recv.buf.copy(target)?;
                    let mut tail = &*target;
                    while let Some((instruction, consumed)) =
                        qpack::stream::Instruction::decode(tail)
                            .map_err(conn::Error::QpackDecoderStream)?
                    {
                        let cancellation = match instruction {
                            qpack::stream::Instruction::StreamCancellation { stream_id } => {
                                let stream_id = Id::try_from(stream_id)
                                    .map_err(qpack::stream::Error::StreamId)
                                    .map_err(conn::Error::QpackDecoderStream)?;
                                let Some(vacancy) = parts.queues.events.vacant() else {
                                    parts.park(id, recv)?;
                                    break;
                                };
                                Some((stream_id, vacancy))
                            }
                            _ => None,
                        };
                        let published = parts.qpack.published();
                        parts
                            .qpack
                            .qpack_encoder
                            .apply(instruction, published)
                            .map_err(conn::Error::QpackDecoderStream)?;
                        recv.consume(consumed, &mut released, &mut parts.ingress.budget)?;
                        if let Some((stream_id, vacancy)) = cancellation {
                            vacancy.push_front(event::Event::QpackCancelled { stream_id });
                            // The vacancy proves this increment fits the admitted event capacity.
                            parts.queues.cancellations += 1;
                        }
                        tail = &tail[consumed..];
                        requeue = true;
                    }
                }
                if matches!(recv.boundary, Boundary::End) {
                    return Err(conn::Error::ClosedCriticalStream(id.get()));
                }
                break (recv.boundary, stream::UniType::QpackDecoder);
            }
            table::Uni::Unknown(recv) => {
                let bytes = recv.clear(&mut parts.ingress.budget)?;
                released = conn::try_add_credit(released, bytes)?;
                break (recv.boundary, stream::UniType::Unknown(0));
            }
        }
    };

    if let Some(ready) = wake {
        let recv = streams.recv_mut(ready).ok_or(conn::Error::StreamMissing {
            stream_id: ready.get(),
        })?;
        parts.park(ready, recv)?;
    }

    let recv = streams.recv_mut(id).ok_or(conn::Error::StreamMissing {
        stream_id: id.get(),
    })?;
    if requeue && !recv.buf.is_empty() && recv.wait.is_none() {
        parts.park(id, recv)?;
    }

    if matches!(boundary, Boundary::End) && kind.is_critical() {
        return Err(conn::Error::ClosedCriticalStream(id.get()));
    }
    let held = barrier || parts.qpack.blocked.contains(id) || recv.wait.is_some();
    let resume = if matches!(resume, Resume::Ready) && !held && matches!(boundary, Boundary::More) {
        Resume::Ready
    } else {
        Resume::Idle
    };
    parts.release(id, released, resume)?;
    if !held && matches!(boundary, Boundary::End) {
        streams.close(id)?;
        return Ok(link::Status::Open);
    }
    Ok(if held {
        link::Status::Suspended
    } else {
        link::Status::Open
    })
}
