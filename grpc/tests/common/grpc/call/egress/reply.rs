use std::{
    io::{self, Read},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use sark_core::manifold::stream::{Boundary, Section};
use sark_grpc::{message, message::Compression};
use sark_h2::{
    ClientRole,
    conn::{self, DataOutcome, config, event, machine::Connection},
    frame::{Ack, numbers::ErrorCode},
    stream::Id,
};
use sark_http::{field, field::metadata::packed};

use crate::common::{
    codec::{Bytes, Parse},
    grpc::{
        call::egress::capacity,
        session::fairness::Client as _,
        wire::{Failure, Peer},
    },
};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Peer(Failure),
    Config(config::Error),
    Protocol(conn::Error),
    Field(field::Error),
    Frame(message::Error),
    Window(capacity::Error),
    Fence {
        opaque: [u8; 8],
        sent: bool,
    },
    Send {
        actual: DataOutcome,
        expected: usize,
    },
    Reset {
        stream: Id,
        code: ErrorCode,
    },
    Decode(Parse),
    MissingStatus,
    Status(sark_grpc::Status),
}

pub struct Reply {
    pub messages: Vec<Vec<u8>>,
    pub headers: Vec<(Section, packed::Decoded)>,
}

impl Reply {
    pub fn status(&self) -> Result<sark_grpc::Status, Error> {
        let (_, fields) = self.headers.last().ok_or(Error::MissingStatus)?;
        sark_grpc::Status::from_fields(fields).map_err(Error::Status)
    }
    pub fn try_exchange(
        addr: SocketAddr,
        path: &[u8],
        messages: &[&[u8]],
        end: bool,
    ) -> Result<Self, Error> {
        let mut peer = Peer::try_config(addr, config::Config::default()).map_err(Error::Peer)?;
        Self::try_reuse(&mut peer, path, messages, end)
    }

    pub fn try_reuse(
        peer: &mut Peer,
        path: &[u8],
        messages: &[&[u8]],
        end: bool,
    ) -> Result<Self, Error> {
        let mut request = Vec::new();
        for message in messages {
            Bytes::try_frame(Compression::Plain, message, &mut request).map_err(Error::Frame)?;
        }
        Self::try_transfer(
            &mut peer.socket,
            &mut peer.client,
            path,
            &request,
            end,
            None,
        )
    }

    pub fn try_failure(addr: SocketAddr, window: u8, invalid: bool) -> Result<Self, Error> {
        let request = [if invalid { 2 } else { 0 }, 0, 0, 0, 0];
        let path = if window == 0 {
            b"/failure/initial".as_slice()
        } else {
            b"/failure/after"
        };
        Self::try_request(addr, path, &request, true, Some(window))
    }

    fn try_request(
        addr: SocketAddr,
        path: &[u8],
        request: &[u8],
        end: bool,
        window: Option<u8>,
    ) -> Result<Self, Error> {
        let mut socket = TcpStream::connect(addr).map_err(Error::Io)?;
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(Error::Io)?;
        let mut client = Connection::<ClientRole>::try_new().map_err(Error::Config)?;
        Self::try_transfer(&mut socket, &mut client, path, request, end, window)
    }

    fn try_transfer(
        socket: &mut TcpStream,
        client: &mut Connection<ClientRole>,
        path: &[u8],
        request: &[u8],
        end: bool,
        window: Option<u8>,
    ) -> Result<Self, Error> {
        if let Some(window) = window {
            client.try_flush(socket).map_err(Error::Io)?;
            capacity::try_window(socket, window).map_err(Error::Window)?;
        }
        let stream = client
            .streams()
            .headers()
            .request(
                Peer::try_headers(path).map_err(Error::Field)?,
                Boundary::More,
            )
            .map_err(Error::Protocol)?;
        client
            .control()
            .ping(*b"reserved")
            .map_err(Error::Protocol)?;
        client.try_flush(socket).map_err(Error::Io)?;

        let mut wire = Vec::new();
        let mut heads = Vec::new();
        let mut finished = false;
        let mut reserved = false;
        let mut sent = false;
        let mut input = [0u8; 4096];
        while !finished {
            let read = socket.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "gRPC trailers",
                )));
            }
            client
                .ingress()
                .ingest(&input[..read])
                .map_err(Error::Protocol)?;
            while let Some(received) = client.ingress().poll_event() {
                match received {
                    event::Event::Ping {
                        ack: Ack::Complete,
                        opaque,
                    } => {
                        if opaque != *b"reserved" || reserved {
                            return Err(Error::Fence { opaque, sent });
                        }
                        reserved = true;
                    }
                    event::Event::StreamReset {
                        stream_id: id,
                        error,
                        ..
                    } => {
                        if id.id() == stream {
                            return Err(Error::Reset {
                                stream,
                                code: error,
                            });
                        }
                    }
                    event::Event::Data {
                        stream_id: id,
                        data,
                        ..
                    } if id.id() == stream => {
                        data.for_each(|part| wire.extend_from_slice(part));
                    }
                    event::Event::Headers {
                        stream_id: id,
                        head,
                        section: Section::Trailing,
                        boundary: Boundary::End,
                        ..
                    } if id.id() == stream => {
                        heads.push((Section::Trailing, head.into_parts().0));
                        finished = true;
                    }
                    event::Event::Headers {
                        stream_id: id,
                        head,
                        section,
                        ..
                    } if id.id() == stream => {
                        heads.push((section, head.into_parts().0));
                    }
                    _ => {}
                }
            }
            if reserved && !sent && window.is_none_or(|window| wire.len() >= usize::from(window)) {
                if !finished && (!request.is_empty() || end) {
                    let outcome = client
                        .streams()
                        .try_send(
                            stream,
                            request,
                            if end { Boundary::End } else { Boundary::More },
                        )
                        .map_err(Error::Protocol)?;
                    if !(matches!(outcome, DataOutcome::Sent(written) if written.get() == request.len())
                        || request.is_empty() && matches!(outcome, DataOutcome::Ended))
                    {
                        return Err(Error::Send {
                            actual: outcome,
                            expected: request.len(),
                        });
                    }
                }
                client.try_flush(socket).map_err(Error::Io)?;
                sent = true;
            }
        }
        Ok(Self {
            messages: Bytes::try_messages(&wire).map_err(Error::Decode)?,
            headers: heads,
        })
    }
}
