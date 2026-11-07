use core::iter;

use o3::buffer::write::consume::Consume as _;
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::{
    self,
    field::metadata::section::{rules, state::Selected},
};

use crate::{
    conn::{self, event, ingress::input::Source as _, machine},
    frame::{self, controls, layout, numbers, payload},
    stream,
};

pub(super) mod data;
pub(super) mod headers;
mod settings;

pub(super) struct Decoder<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(super) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

pub(in crate::conn::machine) enum Feed<T> {
    Consumed,
    Pending(T),
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Refill {
    Added,
    Exhausted,
}

const _: () = {
    assert!(core::mem::size_of::<Refill>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Refill>() == core::mem::align_of::<bool>());
};

impl<R, F, C, A, E> Decoder<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(super) fn push_event(
        &mut self,
        event: event::Event<Selected<F>, F::Block, C, E>,
    ) -> Result<(), conn::Error> {
        self.conn.ingress.events.push(event)
    }

    pub(super) fn ensure_event(&self) -> Result<(), conn::Error> {
        self.conn.ingress.events.try_admit()
    }

    pub(super) fn acknowledge(&mut self, header: layout::Header) -> Result<(), conn::Error> {
        let event = self.conn.ingress.events.vacant()?;
        header.check_conn().map_err(conn::Error::ParseError)?;
        if header.length != numbers::Length::ZERO {
            return Err(conn::Error::ParseError(numbers::ParseError::SettingsSize {
                actual: header.length.into_usize(),
                ack: frame::Ack::Complete,
            }));
        }
        let maximum = self.conn.state.settings.local.header_table_size;
        self.conn.ingress.headers.acknowledge(maximum as usize)?;
        event.push_back(event::Event::SettingsAck);
        self.conn.state.lifecycle.mark_acked();
        Ok(())
    }

    fn accept_peer(&mut self, header: layout::Header) -> Result<(), conn::Error> {
        let ack = frame::Ack::from(header.flags);
        if header.kind != numbers::Type::Settings
            || ack == frame::Ack::Complete
            || !header.stream_id.is_zero()
        {
            return Err(conn::Error::InitialSettings {
                kind: header.kind as u8,
                ack,
                stream: header.stream_id,
            });
        }
        if !header.length.into_u32().is_multiple_of(6) {
            return Err(conn::Error::ParseError(numbers::ParseError::SettingsSize {
                actual: header.length.into_usize(),
                ack: frame::Ack::Required,
            }));
        }
        settings::apply(self.conn, header)
    }

    pub(super) fn flush_chain(&mut self, full: conn::Error) -> Result<(), conn::Error> {
        let max = self.conn.state.settings.local.max_frame_size;
        self.flush(max, full)
    }

    pub(super) fn try_chain<T: AsRef<[u8]>>(&mut self, input: T) -> Result<Feed<T>, conn::Error> {
        let max = self.conn.state.settings.local.max_frame_size;
        self.try_feed(max, input)
    }

    pub(super) fn drive(&mut self) -> Result<(), conn::Error> {
        self.drive_run(&mut iter::empty())
    }

    fn refill(&mut self, source: &mut impl Iterator<Item = C>) -> Result<Refill, conn::Error> {
        let bytes = match source.next() {
            Some(bytes) => bytes,
            None => return Ok(Refill::Exhausted),
        };
        match self.conn.ingress.input.push(bytes) {
            Ok(()) => {}
            Err(full) => return Err(full.reject()),
        }
        Ok(Refill::Added)
    }

    pub(super) fn drive_run(
        &mut self,
        source: &mut impl Iterator<Item = C>,
    ) -> Result<(), conn::Error> {
        loop {
            use conn::ingress::{self, input::Preface};
            let max = self.conn.state.settings.local.max_frame_size;
            let preface = {
                let ingress::Ingress { input, events, .. } = &mut self.conn.ingress;
                input.poll_preface(events, max)?
            };
            let header = match preface {
                Preface::Established => break,
                Preface::Progress => continue,
                Preface::Missing => match self.refill(source)? {
                    Refill::Added => continue,
                    Refill::Exhausted => return Ok(()),
                },
                Preface::Settings(header) => header,
            };
            self.ensure_event()?;
            self.accept_peer(header)?;
            self.conn.ingress.input.establish();
        }

        loop {
            use numbers::{Flags, ParseError, Type};
            let max = self.conn.state.settings.local.max_frame_size;
            if self.conn.ingress.headers.pending().is_some() {
                self.run(max, source)?;
                if self.conn.ingress.headers.pending().is_some() {
                    return Ok(());
                }
                continue;
            }
            let header = match self.conn.ingress.input.next_frame(max)? {
                Some(header) => header,
                None => match self.refill(source)? {
                    Refill::Added => continue,
                    Refill::Exhausted => return Ok(()),
                },
            };
            let total = frame::HEADER_LEN + header.length.into_usize();
            let emits = match header.kind {
                Type::Settings => frame::Ack::from(header.flags) == frame::Ack::Required,
                Type::Ping | Type::GoAway | Type::RstStream => true,
                Type::Headers
                | Type::PushPromise
                | Type::Continuation
                | Type::Data
                | Type::WindowUpdate
                | Type::Priority => false,
            };
            if emits {
                self.ensure_event()?;
            }
            match header.kind {
                Type::Settings => match frame::Ack::from(header.flags) {
                    frame::Ack::Complete => self.acknowledge(header)?,
                    frame::Ack::Required => {
                        header.check_conn().map_err(conn::Error::ParseError)?;
                        if !header.length.into_u32().is_multiple_of(6) {
                            return Err(conn::Error::ParseError(ParseError::SettingsSize {
                                actual: header.length.into_usize(),
                                ack: frame::Ack::Required,
                            }));
                        }
                        settings::apply(self.conn, header)?;
                        continue;
                    }
                },
                Type::Ping => {
                    let mut bytes = [0; 8];
                    self.conn.ingress.input.copy_frame(header, &mut bytes)?;
                    let parsed =
                        payload::Ping::parse(header, &bytes).map_err(conn::Error::ParseError)?;
                    match parsed.ack {
                        frame::Ack::Required => {
                            let pong = payload::Ping {
                                ack: frame::Ack::Complete,
                                opaque: parsed.opaque,
                            };
                            let wire = pong.wire_bytes();
                            let append = self.conn.egress.outbound.try_append([&wire])?;
                            self.conn
                                .ingress
                                .input
                                .bytes
                                .try_consume(total)
                                .map_err(conn::Error::Consume)?;
                            append.commit();
                            self.push_event(event::Event::Ping {
                                ack: frame::Ack::Required,
                                opaque: parsed.opaque,
                            })?;
                            continue;
                        }
                        frame::Ack::Complete => self.push_event(event::Event::Ping {
                            ack: frame::Ack::Complete,
                            opaque: parsed.opaque,
                        })?,
                    }
                }
                Type::GoAway => {
                    if header.length.into_u32() < 8 {
                        return Err(conn::Error::ParseError(ParseError::FrameMinimum {
                            kind: Type::GoAway,
                            actual: header.length.into_usize(),
                            minimum: 8,
                        }));
                    }
                    let mut prefix = [0; 8];
                    self.conn
                        .ingress
                        .input
                        .copy(frame::HEADER_LEN, &mut prefix)?;
                    let parsed =
                        payload::GoAway::parse(header, &prefix).map_err(conn::Error::ParseError)?;
                    let len = header.length.into_usize() - prefix.len();
                    let debug = self
                        .conn
                        .ingress
                        .input
                        .retain_data(frame::HEADER_LEN + prefix.len(), len)?;
                    self.conn.state.lifecycle.goaway_received = Some(parsed.error());
                    self.push_event(event::Event::GoAway {
                        last_stream_id: parsed.last(),
                        error: parsed.error(),
                        debug,
                    })?;
                }
                Type::WindowUpdate => {
                    use payload::WindowUpdate;
                    let mut bytes = [0; 4];
                    self.conn.ingress.input.copy_frame(header, &mut bytes)?;
                    let parsed =
                        WindowUpdate::parse(header, &bytes).map_err(conn::Error::ParseError)?;
                    self.conn
                        .ingress
                        .input
                        .bytes
                        .try_consume(total)
                        .map_err(conn::Error::Consume)?;
                    data::Data::new(&mut *self.conn).apply_window(parsed)?;
                    continue;
                }
                Type::Headers => {
                    let sid = header.try_stream().map_err(conn::Error::ParseError)?;
                    let (mut start, mut len) = self.conn.ingress.input.unpadded(header)?;
                    if header.flags.contains(Flags::PRIORITY) {
                        use layout::Priority;
                        if len < 5 {
                            return Err(conn::Error::ParseError(ParseError::FrameMinimum {
                                kind: Type::Headers,
                                actual: len,
                                minimum: 5,
                            }));
                        }
                        let mut bytes = [0; 5];
                        self.conn.ingress.input.copy(start, &mut bytes)?;
                        Priority::read(&bytes)
                            .check(sid)
                            .map_err(conn::Error::ParseError)?;
                        start += bytes.len();
                        len -= bytes.len();
                    }
                    let boundary = if header.flags.contains(Flags::END_STREAM) {
                        Boundary::End
                    } else {
                        Boundary::More
                    };
                    let chain = if header.flags.contains(Flags::END_HEADERS) {
                        frame::Block::End
                    } else {
                        frame::Block::More
                    };
                    self.handle(sid, (boundary, chain), start, len, total, None)?;
                    continue;
                }
                Type::Data => {
                    data::Data::new(&mut *self.conn).apply_data(header)?;
                    continue;
                }
                Type::Continuation => {
                    return Err(conn::Error::Continuation(conn::Continuation::Unexpected(
                        header.stream_id,
                    )));
                }
                Type::RstStream => {
                    let mut bytes = [0; 4];
                    self.conn.ingress.input.copy_frame(header, &mut bytes)?;
                    let parsed = controls::RstStream::parse(header, &bytes)
                        .map_err(conn::Error::ParseError)?;
                    self.conn
                        .ingress
                        .input
                        .bytes
                        .try_consume(total)
                        .map_err(conn::Error::Consume)?;
                    data::Data::new(&mut *self.conn).apply_reset(parsed)?;
                    continue;
                }
                Type::PushPromise => {
                    use headers::promise;

                    let id = header.try_stream().map_err(conn::Error::ParseError)?;
                    let (start, len) = self.conn.ingress.input.unpadded(header)?;
                    if len < 4 {
                        return Err(conn::Error::ParseError(ParseError::FrameMinimum {
                            kind: Type::PushPromise,
                            actual: len,
                            minimum: 4,
                        }));
                    }
                    let mut wire = [0; 4];
                    self.conn.ingress.input.copy(start, &mut wire)?;
                    let promised = stream::Id::from_wire(u32::from_be_bytes(wire));
                    let push =
                        promised
                            .nonzero()
                            .ok_or(conn::Error::ParseError(ParseError::Promise {
                                stream: id.get(),
                                promised,
                            }))?;
                    let base = start + 4;
                    let size = len - 4;
                    let chain = if header.flags.contains(Flags::END_HEADERS) {
                        frame::Block::End
                    } else {
                        frame::Block::More
                    };
                    promise::Promise::new(id, push).handle(self, chain, base, size, total)?;
                    continue;
                }
                Type::Priority => {
                    use controls::Priority;
                    let mut bytes = [0; 5];
                    self.conn.ingress.input.copy_frame(header, &mut bytes)?;
                    Priority::read(header, &bytes).map_err(conn::Error::ParseError)?;
                }
            }
            self.conn
                .ingress
                .input
                .bytes
                .try_consume(total)
                .map_err(conn::Error::Consume)?;
        }
    }
}
