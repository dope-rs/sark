use std::{marker::PhantomData, rc};

use dope_quic::{
    conn::{id, scope},
    runtime::mux::{
        self, handler,
        handler::hook::{
            data::stream,
            life::{create, ready},
            set,
            wire::{cert, ingress, packet},
        },
    },
    stream::Retain,
};
use o3::buffer::resident;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        event::{Completion, Event, Role},
        io::Step,
    },
    manifold::{Notification, Session},
    stream::Owned,
};
use sark_http::{
    field::{self, Entry, Lower, Pseudo, Value, metadata::block::Connect},
    payload::compress::dynamic::hash,
};

use crate::common::server::h3::{
    observer::protocol,
    proof::{DIALER, Dispatch, Failure, Result, Shared, connection_key},
};

pub(crate) struct Link<'d> {
    pub(crate) h3: Session<'d>,
    pub(crate) handle: id::Handle<id::Driver<'d>, DIALER>,
    pub(crate) sent: bool,
    deferred: Vec<(usize, Owned)>,
}

pub(crate) struct Client<'a, Q = packet::On> {
    pub(crate) seed: hash::Seed,
    pub(crate) proof: rc::Rc<Shared<'a>>,
    packet: PhantomData<fn() -> Q>,
}

impl<'a, Q> Client<'a, Q> {
    pub(crate) fn new(seed: hash::Seed, proof: rc::Rc<Shared<'a>>) -> Self {
        Self {
            seed,
            proof,
            packet: PhantomData,
        }
    }
}

impl<'d> Link<'d> {
    pub(crate) fn try_send<B: Retain<'d>, const RECONCILE: bool>(
        &mut self,
        proof: &Shared<'_>,
        quic: &mut scope::Access<'_, DIALER, B, RECONCILE, resident::Shared<'d>>,
        dispatch: Dispatch,
    ) -> Result<()> {
        let index = usize::try_from(self.handle.index()).map_err(Failure::Index)?;
        let requests = proof.read()?.requests.clone();
        for (slot, request) in requests.iter().enumerate() {
            if request.connection != index || request.dispatch != dispatch {
                continue;
            }
            let target = if dispatch == Dispatch::AfterGoAway {
                let pending = self
                    .deferred
                    .iter()
                    .position(|(pending, _)| *pending == slot)
                    .ok_or_else(|| {
                        Failure::Scenario("missing deferred H3 request stream".to_owned())
                    })?;
                self.deferred.swap_remove(pending).1
            } else {
                self.h3.egress().open_scope(quic).map_err(Failure::Flush)?
            };
            let stream = target.stream_id();
            proof.write()?.replies.streams[slot] = Some((connection_key(self.handle), stream));
            if let Some(wire) = request.head.wire {
                let mut streams = quic.streams();
                let send = streams
                    .view()
                    .resolve_send(stream)
                    .map_err(Failure::Stream)?;
                streams.send().send(send, wire).map_err(Failure::Stream)?;
                if request.data.boundary == Boundary::End {
                    streams.send().finish(send).map_err(Failure::Stream)?;
                }
                continue;
            }
            let mut target = self.h3.send().bind(target).map_err(Failure::Send)?;
            let length = match request.data.declared_body_len {
                Some(length) => length,
                None => request.data.bytes.len(),
            }
            .to_string();
            let boundary = if request.data.bytes.is_empty() {
                request.data.boundary
            } else {
                Boundary::More
            };
            let method = Entry::pseudo(Pseudo::Method, Value::try_bytes(request.head.method)?);
            let scheme = const {
                Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https")))
            };
            let host = const {
                Entry::pseudo(
                    Pseudo::Authority,
                    field::literal!(Value::try_bytes(b"example.com")),
                )
            };
            let path = Entry::pseudo(Pseudo::Path, Value::try_bytes(request.head.path)?);
            let length = Entry::new(
                field::literal!(Lower::try_bytes(b"content-length")),
                Value::try_bytes(length.as_bytes())?,
            );
            let mut fields = [method, scheme, host, path, length, length];
            let count = if let Some(protocol) = request.head.protocol {
                fields[4] = Entry::pseudo(Pseudo::Protocol, Value::try_bytes(protocol)?);
                5
            } else if request.head.method == b"CONNECT" {
                fields[1] = const {
                    Entry::pseudo(
                        Pseudo::Authority,
                        field::literal!(Value::try_bytes(b"example.com:443")),
                    )
                };
                fields[2] = length;
                2 + usize::from(request.data.declared_body_len.is_some())
            } else if let Some(media) = request.head.content_type {
                fields[4] = Entry::new(
                    field::literal!(Lower::try_bytes(b"content-type")),
                    Value::try_bytes(media)?,
                );
                6
            } else {
                5
            };
            target
                .headers(&fields[..count], boundary)
                .map_err(Failure::Send)?;
            if !request.data.bytes.is_empty() {
                if let Some(split) = request.data.split {
                    let (first, rest) =
                        request
                            .data
                            .bytes
                            .split_at_checked(split)
                            .ok_or(Failure::Split {
                                length: request.data.bytes.len(),
                                at: split,
                            })?;
                    target.data(first, Boundary::More).map_err(Failure::Send)?;
                    if !request.data.defer_tail {
                        target
                            .data(rest, request.data.boundary)
                            .map_err(Failure::Send)?;
                    }
                } else {
                    let size = request
                        .data
                        .chunk_size
                        .map_or(request.data.bytes.len(), |n| n.get());
                    let mut chunks = request.data.bytes.chunks(size).peekable();
                    while let Some(bytes) = chunks.next() {
                        let boundary = if chunks.peek().is_none() {
                            request.data.boundary
                        } else {
                            Boundary::More
                        };
                        target.data(bytes, boundary).map_err(Failure::Send)?;
                    }
                }
            }
        }
        if dispatch == Dispatch::Initial {
            for (slot, request) in requests.iter().enumerate() {
                if request.connection == index && request.dispatch == Dispatch::AfterGoAway {
                    let target = self.h3.egress().open_scope(quic).map_err(Failure::Flush)?;
                    self.deferred.push((slot, target));
                }
            }
        }
        self.h3
            .egress()
            .settle_scope(quic)
            .map_err(Failure::Flush)?;
        if dispatch == Dispatch::Initial {
            self.sent = true;
        }
        Ok(())
    }
}

impl<Q> Client<'_, Q> {
    pub(crate) fn try_collect<'d, B: Retain<'d>, const RECONCILE: bool>(
        &self,
        link: &mut Link<'d>,
        quic: &mut scope::Access<'_, DIALER, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<()> {
        loop {
            let result = link
                .h3
                .ingress()
                .consume(quic, |event| -> Result<_> {
                    let done = event.completion();
                    let mut tail = None;
                    match event {
                        Event::Settings(_) if !link.sent => {}
                        Event::Rejected { stream_id, error } => {
                            return Err(Failure::Receive(
                                sark_h3::conn::Error::Content {
                                    stream_id: stream_id.get(),
                                    error: *error,
                                }
                                .into(),
                            ));
                        }
                        Event::Headers { message, head, .. } => {
                            let stream = message.stream_id();
                            tail = {
                                let mut proof = self.proof.write()?;
                                let Some(slot) = proof.slot(connection_key(link.handle), stream)
                                else {
                                    return Ok(None);
                                };
                                let request = proof.requests[slot];
                                proof.replies.status_ok[slot] = head.fields().iter().any(|field| {
                                    field.name == b":status" && field.value == request.expect.status
                                });
                                proof.tail(slot)?
                            };
                        }
                        Event::Data { message, data, .. } => {
                            let stream = message.stream_id();
                            let mut proof = self.proof.write()?;
                            let Some(slot) = proof.slot(connection_key(link.handle), stream) else {
                                return Ok(None);
                            };
                            proof.replies.bodies[slot].extend(data.segments().flatten().copied());
                        }
                        Event::Finished { .. } => {}
                        Event::GoAway { id } => {
                            self.proof.write()?.network.goaway = Some(*id);
                        }
                        Event::Reset {
                            stream_id: stream,
                            error_code: code,
                            ..
                        }
                        | Event::Stopped {
                            stream_id: stream,
                            error_code: code,
                            ..
                        } => {
                            let mut proof = self.proof.write()?;
                            let Some(slot) = proof.slot(connection_key(link.handle), *stream)
                            else {
                                return Ok(None);
                            };
                            match proof.requests[slot].expect.error {
                                Some(expected) if expected.wire_value() == code.get() => {
                                    proof.replies.error_ok[slot] = true;
                                    if !proof.replies.finished[slot] {
                                        proof.replies.finished[slot] = true;
                                        proof.replies.completion_order.push(slot);
                                    }
                                    proof.stop_done();
                                    if proof.requests[slot].dispatch == Dispatch::AfterGoAway
                                        && let Some(pending) =
                                            proof.requests.iter().position(|request| {
                                                request.dispatch == Dispatch::Initial
                                                    && request.data.defer_tail
                                            })
                                    {
                                        tail = proof.tail(pending)?;
                                    }
                                }
                                Some(expected) => proof.fail(Failure::Scenario(format!(
                                    "stream {} failed with H3 code {}, expected {}",
                                    stream.get(),
                                    code.get(),
                                    expected.wire_value()
                                ))),
                                None if matches!(done, Completion::Stopped { .. })
                                    && code.get()
                                        == sark_h3::frame::ErrorCode::RequestCancelled
                                            .wire_value() =>
                                {
                                    proof.replies.error_ok[slot] = true;
                                }
                                None if proof.cancel.request.is_none() => {
                                    proof.fail(Failure::Scenario(format!(
                                        "stream {} failed with H3 code {}",
                                        stream.get(),
                                        code.get()
                                    )))
                                }
                                None => {}
                            }
                        }
                        _ => {}
                    }
                    if let Completion::End(stream) = done {
                        let mut proof = self.proof.write()?;
                        if let Some(slot) = proof.slot(connection_key(link.handle), stream) {
                            if !proof.replies.finished[slot] {
                                proof.replies.finished[slot] = true;
                                proof.replies.completion_order.push(slot);
                            }
                            proof.stop_done();
                        }
                    }
                    Ok(tail)
                })
                .map_err(Failure::Receive)?;
            match result {
                Step::Item(result) => {
                    if let Some((stream, bytes, boundary)) = result? {
                        link.h3
                            .send()
                            .data(stream, bytes, boundary)
                            .map_err(Failure::Send)?;
                    }
                }
                Step::More => {}
                Step::Idle => break,
            }
        }
        Ok(())
    }
    fn try_stream<'scope, 'd, B: Retain<'d>, const RECONCILE: bool>(
        &self,
        link: &mut Link<'d>,
        conn: &mut scope::Access<'scope, DIALER, B, RECONCILE, resident::Shared<'d>>,
        event: Notification<'scope>,
    ) -> Result<()> {
        link.h3
            .ingress()
            .ingest(conn, event)
            .map_err(Failure::Receive)?;
        self.try_collect(link, conn)?;
        let draining = {
            let proof = self.proof.read()?;
            proof.network.goaway.is_some()
                && !proof.cancel.followup
                && proof
                    .requests
                    .iter()
                    .any(|request| request.dispatch == Dispatch::AfterGoAway)
        };
        if draining {
            link.try_send(&self.proof, conn, Dispatch::AfterGoAway)?;
            self.proof.write()?.cancel.followup = true;
        } else if !link.sent && link.h3.h3().settings().peer().is_some() {
            let requires = self
                .proof
                .read()?
                .requests
                .iter()
                .any(|request| request.head.protocol.is_some());
            if requires
                && !link
                    .h3
                    .h3()
                    .settings()
                    .peer()
                    .is_some_and(|settings| matches!(settings.connect(), Connect::Enabled))
            {
                return Err(Failure::Scenario(
                    "generated extended route did not advertise H3 CONNECT support".to_owned(),
                ));
            }
            link.try_send(&self.proof, conn, Dispatch::Initial)?;
        } else if link.sent {
            link.h3
                .egress()
                .settle_scope(conn)
                .map_err(Failure::Flush)?;
        }
        Ok(())
    }
}

impl<'d, B: Retain<'d>, Q> handler::Handler<DIALER, B, resident::Shared<'d>, id::Driver<'d>>
    for Client<'_, Q>
where
    Q: packet::Mode<Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
{
    type Connection = Link<'d>;
    type Create = Self;
    type Events =
        set::Set<set::On, set::Off, set::On, set::Off, set::Off, set::Off, ingress::Off, Q>;
}

impl<'d, B: Retain<'d>, Q> create::Mode<Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>
    for Client<'_, Q>
where
    Q: packet::Mode<Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
{
    fn create(
        event: create::Event<'_, Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> create::Decision<Link<'d>> {
        let (this, _, handle) = event.into_parts();
        let h3 = protocol()
            .and_then(|protocol| Session::from_seed(Role::Client, protocol, this.seed.clone()));
        match h3 {
            Ok(h3) => create::Decision::Install(Link {
                h3,
                handle,
                sent: false,
                deferred: Vec::new(),
            }),
            Err(error) => {
                this.proof.fail(Failure::Session(error));
                create::Decision::Reject
            }
        }
    }
}

impl<'d, B: Retain<'d>, Q> ready::Sink<DIALER, B, resident::Shared<'d>, id::Driver<'d>>
    for Client<'_, Q>
where
    Q: packet::Mode<Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
{
    fn established(event: ready::Event<'_, Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>) {
        let (this, mut slot, _, _) = event.into();
        let (link, conn) = slot.split();
        if let Err(error) = link.h3.egress().start_scope(conn) {
            this.proof.fail(Failure::Flush(error));
        }
    }
}

impl<'d, B: Retain<'d>, Q> stream::Sink<DIALER, B, resident::Shared<'d>, id::Driver<'d>>
    for Client<'_, Q>
where
    Q: packet::Mode<Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
{
    fn stream(
        event: stream::Event<'_, Self, DIALER, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        let (this, mut slot, _, event, _) = event.into();
        let (link, conn) = slot.split();
        if let Err(error) = this.try_stream(link, conn, Notification::resident(event)) {
            this.proof.fail(error);
        }
        mux::Disposition::Accept
    }
}

impl<'d, Q> cert::Sink<DIALER, id::Driver<'d>> for Client<'_, Q> {
    fn certificate(event: cert::Event<'_, '_, Self, DIALER, id::Driver<'d>>) {
        let (client, _, _, _) = event.into_parts();
        client.proof.reject();
    }
}

impl<'d, B: Retain<'d>, Q> packet::Sink<DIALER, B, resident::Shared<'d>, id::Driver<'d>>
    for Client<'_, Q>
{
    fn packet(event: packet::Event<'_, Self>) {
        let (client, _, _, _) = event.into_parts();
        client.proof.reject();
    }
}
