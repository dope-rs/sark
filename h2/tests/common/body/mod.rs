use std::net;

use dope::{
    core::{driver::route::KeyTag, io::socket::option::Switch},
    manifold::app::env::quota::PerIp,
};
use sark_h2::{conn::DataOutcome, frame::numbers};
use sark_http::{field, o3::buffer::resident::bound, payload::response::status::Code};

use crate::common::headers::{TEXT, entry};

pub mod fairness;

use std::{
    io::{self, Read, Write},
    net::TcpStream,
};

use dope::{
    core::{
        driver::{route::table::conn, settings},
        io::socket,
    },
    manifold::network::listener::config,
    net::tcp,
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{Error, event::Event, machine::Connection},
    frame::Ack,
    server::{
        self, Config, runtime,
        service::{Handler, Request},
    },
    stream::Id,
};
use sark_http::{
    payload::response::{fixed, headers},
    request::body,
};

pub fn headers<'f>(
    path: &'f [u8],
    length: &'f [u8],
) -> Result<[field::Entry<'f>; 5], field::Error> {
    Ok([
        field::literal!(entry(b":method", b"POST")),
        field::literal!(entry(b":scheme", b"http")),
        entry(b":path", path)?,
        field::literal!(entry(b":authority", b"localhost")),
        entry(b"content-length", length)?,
    ])
}

pub const PROBE: [u8; 8] = *b"retained";

pub const LIMITS: Config = Config {
    max_request_body_bytes: 64 << 10,
    max_request_body_chunks: 4 << 10,
    max_connection_body_bytes: 64 << 10,
    max_connection_body_chunks: 16 << 10,
    max_outbound_bytes: 64 << 10,
};

pub trait Configure {
    fn try_plan(
        self,
        bind: net::SocketAddr,
    ) -> Result<runtime::Plan<KeyTag<0>>, server::ConfigError>;
}

impl Configure for Config {
    fn try_plan(
        self,
        bind: net::SocketAddr,
    ) -> Result<runtime::Plan<KeyTag<0>>, server::ConfigError> {
        runtime::Plan::new(
            config::Prepared {
                bind,
                connections: conn::Capacity::fixed::<4>(),
                per_ip: Some(PerIp::fixed::<4>()),
                direct_flights: Default::default(),
                backlog: socket::Backlog::fixed::<16>(),
                stream: tcp::StreamConfig {
                    no_delay: Some(Switch::On),
                    ..Default::default()
                },
                transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
                egress: Default::default(),
            },
            settings::Receive::fixed::<2, { 64 << 10 }>(),
            self,
        )
    }
}

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    Protocol(Error),
    Closed,
    Send(DataOutcome),
}

pub trait Peer {
    fn try_send_all(&mut self, id: Id, bytes: &[u8], fin: bool) -> Result<(), ReadError>;
    fn try_flush(&mut self, socket: &mut TcpStream) -> io::Result<()>;
    fn try_events<T>(
        &mut self,
        socket: &mut TcpStream,
        visit: impl FnMut(Event) -> Option<T>,
    ) -> Result<T, ReadError>;
    fn try_ping(&mut self, socket: &mut TcpStream) -> Result<(), ReadError>;
    fn try_read(&mut self, socket: &mut TcpStream, id: Id) -> Result<Vec<u8>, ReadError>;
    fn try_reset(
        &mut self,
        socket: &mut TcpStream,
        id: Id,
    ) -> Result<numbers::ErrorCode, ReadError>;
}

impl Peer for Connection<ClientRole> {
    fn try_send_all(&mut self, id: Id, bytes: &[u8], fin: bool) -> Result<(), ReadError> {
        let mut offset = 0;
        while offset < bytes.len() {
            let written = self
                .streams()
                .try_send(
                    id,
                    &bytes[offset..],
                    if fin { Boundary::End } else { Boundary::More },
                )
                .map_err(ReadError::Protocol)?;
            let written = match written {
                DataOutcome::Sent(count) => count,
                other => return Err(ReadError::Send(other)),
            };
            offset += written.get();
        }
        Ok(())
    }

    fn try_flush(&mut self, socket: &mut TcpStream) -> io::Result<()> {
        loop {
            let bytes = self.output().first();
            if bytes.is_empty() {
                return Ok(());
            }
            let len = bytes.len();
            socket.write_all(bytes)?;
            self.transfer().drain(len);
        }
    }

    fn try_events<T>(
        &mut self,
        socket: &mut TcpStream,
        mut visit: impl FnMut(Event) -> Option<T>,
    ) -> Result<T, ReadError> {
        let mut input = [0; 16 * 1024];
        loop {
            while let Some(event) = self.ingress().poll_event() {
                if let Some(value) = visit(event) {
                    self.try_flush(socket).map_err(ReadError::Io)?;
                    return Ok(value);
                }
            }
            self.try_flush(socket).map_err(ReadError::Io)?;
            let result = if self.ingress().is_blocked() {
                self.ingress().resume()
            } else {
                let read = socket.read(&mut input).map_err(ReadError::Io)?;
                if read == 0 {
                    return Err(ReadError::Closed);
                }
                self.ingress().ingest(&input[..read])
            };
            match result {
                Err(Error::HeaderDecodeFull { .. } | Error::PayloadFull { .. })
                    if self.ingress().has_event() => {}
                result => result.map_err(ReadError::Protocol)?,
            }
        }
    }

    fn try_ping(&mut self, socket: &mut TcpStream) -> Result<(), ReadError> {
        self.try_events(socket, |event| {
            matches!(event, Event::Ping { ack: Ack::Complete, opaque } if opaque == PROBE)
                .then_some(())
        })
    }

    fn try_read(&mut self, socket: &mut TcpStream, id: Id) -> Result<Vec<u8>, ReadError> {
        let mut body = Vec::new();
        self.try_events(socket, |event| match event {
            Event::Headers {
                stream_id: seen,
                boundary,
                ..
            } if seen == id => matches!(boundary, Boundary::End).then_some(()),
            Event::Data {
                stream_id: seen,
                data,
                boundary,
            } if seen == id => {
                data.for_each(|segment| body.extend_from_slice(segment));
                matches!(boundary, Boundary::End).then_some(())
            }
            _ => None,
        })?;
        Ok(body)
    }

    fn try_reset(
        &mut self,
        socket: &mut TcpStream,
        id: Id,
    ) -> Result<numbers::ErrorCode, ReadError> {
        self.try_events(socket, |event| {
            if let Event::StreamReset {
                stream_id: seen,
                error,
                ..
            } = event
                && seen == id
            {
                return Some(error);
            }
            None
        })
    }
}

pub struct Echo;

impl Handler for Echo {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = fixed::Direct<[headers::Entry<'req>; 1], body::Ref<'req, 'd>>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        fixed::Direct::new(Code::OK, [TEXT], request.body())
    }
}
