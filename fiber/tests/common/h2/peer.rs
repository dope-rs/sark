use std::{
    io::{self, Read as _, Write as _},
    iter,
    net::{SocketAddr, TcpStream},
    num::NonZero,
    thread,
    time::Duration,
};

use o3::alloc;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{
        self, DataOutcome, config,
        event::Event,
        machine::{Connection, streams::Limit},
    },
    frame::{
        self, controls,
        layout::{Header, Headers},
        numbers, payload,
    },
    stream::Id,
};
use sark_http::field::metadata::map::Owned;

pub fn flush(client: &mut Connection<ClientRole>, wire: &mut impl io::Write) -> io::Result<()> {
    loop {
        let output = client.output().first();
        if output.is_empty() {
            return wire.flush();
        }
        wire.write_all(output)?;
        let length = output.len();
        client.transfer().drain(length);
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Alloc(alloc::Error),
    Config(config::Error),
    Connection(conn::Error),
    Parse(numbers::ParseError),
    Closed,
    MissingHeaders,
    OpenHeaders,
    Frame {
        kind: numbers::Type,
        stream: Id,
    },
    UnknownStream(Id),
    Blocked(conn::DataBlock),
    Incomplete {
        sent: usize,
        expected: usize,
    },
    StreamReset {
        stream: Id,
        error: numbers::ErrorCode,
    },
    GoAway,
    Status {
        found: Vec<bool>,
        expected: Vec<u8>,
    },
}

pub struct Peer<'a> {
    socket: &'a mut TcpStream,
    client: &'a mut Connection<ClientRole>,
}
impl<'a> Peer<'a> {
    pub fn new(socket: &'a mut TcpStream, client: &'a mut Connection<ClientRole>) -> Self {
        Self { socket, client }
    }
    pub fn try_connect(
        addr: SocketAddr,
        timeout: Duration,
    ) -> Result<(TcpStream, Connection<ClientRole>), Error> {
        let socket = Self::try_open(addr, timeout).map_err(Error::Io)?;
        let client = Connection::<ClientRole>::try_new().map_err(Error::Config)?;
        Ok((socket, client))
    }
    pub fn try_open(addr: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
        let socket = TcpStream::connect(addr)?;
        socket.set_read_timeout(Some(timeout))?;
        Ok(socket)
    }

    pub fn try_flush(self) -> Result<(), Error> {
        flush(self.client, self.socket).map_err(Error::Io)
    }

    pub fn try_faults(self, stream: Id) -> Result<(), Error> {
        Peer::new(self.socket, self.client).try_flush()?;
        let mut output = Vec::new();
        Headers::new(stream, Boundary::End, frame::Block::End, None, &[0x82])
            .map_err(Error::Parse)?
            .encode(&mut output)
            .map_err(Error::Alloc)?;
        output.extend_from_slice(
            &Header {
                length: numbers::Length::ZERO,
                kind: numbers::Type::Headers,
                flags: numbers::Flags(numbers::Flags::END_HEADERS),
                stream_id: Id::CONNECTION,
            }
            .wire_bytes(),
        );
        self.socket.write_all(&output).map_err(Error::Io)?;
        Ok(())
    }

    pub fn try_settings(self) -> Result<(), Error> {
        let Self {
            socket: stream,
            client,
        } = self;
        Peer::new(stream, client).try_flush()?;
        let mut input = [0; 4096];
        loop {
            let read = stream.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed);
            }
            client
                .ingress()
                .ingest(&input[..read])
                .map_err(Error::Connection)?;
            let mut applied = false;
            while let Some(event) = client.ingress().poll_event() {
                applied |= matches!(event, Event::SettingsApplied);
            }
            Peer::new(stream, client).try_flush()?;
            if applied {
                return Ok(());
            }
        }
    }

    pub fn try_pause(self) -> Result<(), Error> {
        let mut output = Vec::new();
        controls::Settings::new(frame::Ack::Required, &[0, 4, 0, 0, 0, 0])
            .ok_or(Error::Frame {
                kind: numbers::Type::Settings,
                stream: Id::CONNECTION,
            })?
            .encode(&mut output)
            .map_err(Error::Alloc)?;
        self.socket.write_all(&output).map_err(Error::Io)
    }

    pub fn try_credit(self, stream: Id, delta: numbers::Increment) -> Result<(), Error> {
        let mut output = Vec::new();
        payload::Payload::WindowUpdate(payload::WindowUpdate {
            stream_id: stream,
            increment: delta,
        })
        .encode(&mut output)
        .map_err(Error::Alloc)?;
        self.socket.write_all(&output).map_err(Error::Io)
    }

    pub fn try_fragment(self, fragment: NonZero<usize>) -> Result<(), Error> {
        let Self {
            socket: stream,
            client,
        } = self;
        use sark_h2::{
            conn::CLIENT_PREFACE,
            frame::{self, layout, numbers, payload},
        };

        let fragment = fragment.get();
        let output = client.output().first();
        let mut offset = CLIENT_PREFACE.len();
        let mut frame = None;
        while offset < output.len() {
            let header = layout::Header::parse(&output[offset..]).map_err(Error::Parse)?;
            let total = frame::HEADER_LEN + header.length.into_usize();
            if header.kind == numbers::Type::Headers {
                frame = Some((offset, header, total));
                break;
            }
            offset += total;
        }
        let (offset, header, total) = frame.ok_or(Error::MissingHeaders)?;
        if !header.flags.contains(numbers::Flags::END_HEADERS) {
            return Err(Error::OpenHeaders);
        }
        let start = offset + frame::HEADER_LEN;
        let end = offset + total;
        let block = output
            .get(start..end)
            .ok_or(Error::Parse(numbers::ParseError::Need {
                actual: output.len(),
                required: end,
            }))?;
        let len = fragment.min(block.len());
        let write = |mut stream: &TcpStream, bytes: &[u8]| -> io::Result<()> {
            if bytes.is_empty() {
                return Ok(());
            }
            stream.write_all(bytes)?;
            thread::sleep(Duration::from_millis(2));
            Ok(())
        };
        write(stream, &output[..offset]).map_err(Error::Io)?;
        let mut first = Vec::with_capacity(frame::HEADER_LEN + len);
        let boundary = if header.flags.contains(numbers::Flags::END_STREAM) {
            Boundary::End
        } else {
            Boundary::More
        };
        let chain = if len == block.len() {
            frame::Block::End
        } else {
            frame::Block::More
        };
        layout::Headers::new(header.stream_id, boundary, chain, None, &block[..len])
            .map_err(Error::Parse)?
            .encode(&mut first)
            .map_err(Error::Alloc)?;
        write(stream, &first).map_err(Error::Io)?;
        let rest = &block[len..];
        let chunks = rest.chunks(fragment);
        let count = chunks.len();
        for (index, chunk) in chunks.enumerate() {
            let mut next = Vec::with_capacity(frame::HEADER_LEN + chunk.len());
            let chain = if index + 1 == count {
                frame::Block::End
            } else {
                frame::Block::More
            };
            payload::Continuation::new(header.stream_id, chain, chunk)
                .ok_or(Error::Frame {
                    kind: numbers::Type::Continuation,
                    stream: header.stream_id,
                })?
                .encode(&mut next)
                .map_err(Error::Alloc)?;
            write(stream, &next).map_err(Error::Io)?;
        }
        write(stream, &output[end..]).map_err(Error::Io)?;
        let written = output.len();
        client.transfer().drain(written);

        Ok(())
    }

    pub fn try_response(self, wanted: Id) -> Result<(Vec<Owned>, Vec<u8>), Error> {
        let Self { socket, client } = self;
        let mut headers = Vec::new();
        let mut body = Vec::new();
        let mut input = [0; 16 << 10];
        'read: loop {
            let read = socket.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed);
            }
            client
                .ingress()
                .ingest(&input[..read])
                .map_err(Error::Connection)?;
            while let Some(event) = client.ingress().poll_event() {
                match event {
                    Event::Headers {
                        stream_id: stream,
                        head,
                        boundary,
                        ..
                    } if stream == wanted => {
                        headers = head.fields().try_to_owned().map_err(Error::Alloc)?;
                        if matches!(boundary, Boundary::End) {
                            break 'read;
                        }
                    }
                    Event::Data {
                        stream_id: stream,
                        data,
                        boundary,
                    } if stream == wanted => {
                        data.for_each(|segment| body.extend_from_slice(segment));
                        if matches!(boundary, Boundary::End) {
                            break 'read;
                        }
                    }
                    Event::StreamReset {
                        stream_id: stream,
                        error,
                        ..
                    } if stream == wanted => {
                        return Err(Error::StreamReset {
                            stream: stream.id(),
                            error,
                        });
                    }
                    Event::GoAway { .. } => return Err(Error::GoAway),
                    _ => {}
                }
            }
            Peer::new(socket, client).try_flush()?;
        }
        Ok((headers, body))
    }

    pub fn try_reset(self, wanted: Id) -> Result<numbers::ErrorCode, Error> {
        let Self { socket, client } = self;
        let mut input = [0; 16 << 10];
        loop {
            let read = socket.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed);
            }
            client
                .ingress()
                .ingest(&input[..read])
                .map_err(Error::Connection)?;
            while let Some(event) = client.ingress().poll_event() {
                if let Event::StreamReset {
                    stream_id: stream,
                    error,
                    ..
                } = event
                    && stream == wanted
                {
                    return Ok(error);
                }
            }
            Peer::new(socket, client).try_flush()?;
        }
    }

    pub fn try_statuses(
        self,
        wanted: &[Id],
        status: &[u8],
    ) -> Result<(Vec<usize>, Vec<Vec<u8>>), Error> {
        let Self { socket, client } = self;
        let mut bodies = iter::repeat_with(Vec::new)
            .take(wanted.len())
            .collect::<Vec<_>>();
        let mut ok = vec![false; wanted.len()];
        let mut done = vec![false; wanted.len()];
        let mut order = Vec::with_capacity(wanted.len());
        let mut input = [0; 16 << 10];
        while order.len() != wanted.len() {
            let read = socket.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Closed);
            }
            let mut result = client.ingress().ingest(&input[..read]);
            loop {
                let mut drained = false;
                while let Some(event) = client.ingress().poll_event() {
                    drained = true;
                    match event {
                        Event::Headers {
                            stream_id: stream,
                            head,
                            boundary,
                            ..
                        } => {
                            let Some(index) = wanted.iter().position(|wanted| *wanted == stream)
                            else {
                                continue;
                            };
                            ok[index] = head
                                .fields()
                                .iter()
                                .any(|field| field.name == b":status" && field.value == status);
                            if matches!(boundary, Boundary::End) && !done[index] {
                                done[index] = true;
                                order.push(index);
                            }
                        }
                        Event::Data {
                            stream_id: stream,
                            data,
                            boundary,
                        } => {
                            let Some(index) = wanted.iter().position(|wanted| *wanted == stream)
                            else {
                                continue;
                            };
                            data.for_each(|segment| bodies[index].extend_from_slice(segment));
                            if matches!(boundary, Boundary::End) && !done[index] {
                                done[index] = true;
                                order.push(index);
                            }
                        }
                        Event::StreamReset {
                            stream_id: stream,
                            error,
                            ..
                        } if wanted.contains(&stream.id()) => {
                            return Err(Error::StreamReset {
                                stream: stream.id(),
                                error,
                            });
                        }
                        Event::GoAway { .. } => return Err(Error::GoAway),
                        _ => {}
                    }
                }
                match result {
                    Ok(()) => break,
                    Err(error) if drained && error.is_pressure() => {
                        result = client.ingress().resume();
                    }
                    Err(error) => return Err(Error::Connection(error)),
                }
            }
            Peer::new(socket, client).try_flush()?;
        }
        if !ok.iter().all(|status| *status) {
            return Err(Error::Status {
                found: ok,
                expected: status.to_vec(),
            });
        }
        Ok((order, bodies))
    }
}

pub struct Sender<'a>(&'a mut Connection<ClientRole>);
impl<'a> Sender<'a> {
    pub fn new(client: &'a mut Connection<ClientRole>) -> Self {
        Self(client)
    }
    pub fn try_send(self, stream: Id, bytes: &[u8], boundary: Boundary) -> Result<(), Error> {
        let client = self.0;

        let key = client
            .status()
            .stream_key(stream)
            .ok_or(Error::UnknownStream(stream))?;
        let mut offset = 0;
        loop {
            let outcome = client
                .streams()
                .try_to(key, &bytes[offset..], &[], boundary, Limit::MAX)
                .map_err(Error::Connection)?;
            let sent = match outcome {
                DataOutcome::Sent(sent) => Ok(sent),
                DataOutcome::Ended => {
                    if offset != bytes.len() {
                        return Err(Error::Incomplete {
                            sent: offset,
                            expected: bytes.len(),
                        });
                    }
                    return Ok(());
                }
                DataOutcome::Blocked(cause) => Err(cause),
            }
            .map_err(Error::Blocked)?;
            offset += sent.get();
            if offset == bytes.len() {
                return Ok(());
            }
        }
    }
}
