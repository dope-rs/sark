use std::{
    array,
    io::{self, Read, Write as _},
    net::{SocketAddr, TcpStream},
    task::Poll,
    time::Duration,
};

use dope_test::harness::signals::LocalGateRef;
use o3::{alloc, buffer};
use sark_core::manifold::stream::{Boundary, Section};
use sark_grpc::{
    Status,
    protocol::request,
    server::service::{
        self,
        handler::{self, Handler},
        reply::{Reply, Write},
        timers,
    },
};
use sark_h2::{
    ClientRole,
    conn::{self, config, event::Event, machine::Connection},
    frame::{Ack, controls, numbers::ErrorCode},
    stream::Id,
};
use sark_http::field;

use crate::common::{
    codec::Bytes,
    grpc::{session::fairness::Client as _, wire::Peer},
};

pub struct Failed<'a>(pub LocalGateRef<'a>);

impl Handler for Failed<'_> {
    type Call = bool;
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        _: &mut Reply<'_, W>,
    ) -> handler::Start<bool> {
        if head.path() == b"/capacity/produce" {
            handler::Start::buffered(true)
        } else {
            handler::Start::live(head.path() == b"/capacity/held").producing()
        }
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut bool,
        _: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        response.push_message(vec![1]);
        response.finish_with(Status::ok());
        handler::RequestState::Produce
    }

    fn output<W: Write>(
        &mut self,
        _: &mut (),
        failed: &mut bool,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        if *failed {
            self.0.hit();
            reply.push_message(vec![1]);
            reply.finish_with(Status::ok());
            return handler::OutputState::Produced;
        }
        reply.finish_with(Status::ok());
        handler::OutputState::Complete
    }
}

#[derive(Default)]
pub struct Capacity<'a> {
    pub blocked: Option<LocalGateRef<'a>>,
    pub resumed: Option<LocalGateRef<'a>>,
    pub attempts: Option<LocalGateRef<'a>>,
    pub completed: Option<LocalGateRef<'a>>,
}

#[derive(Default)]
pub struct State {
    payload: Option<Vec<u8>>,
    blocked: bool,
}

impl Handler for Capacity<'_> {
    type Call = State;
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<State> {
        let path = head.path();
        if path == b"/capacity/held" {
            reply.push_message(vec![0; 4]);
        }
        handler::Start::live(State {
            payload: (path == b"/capacity/ready").then(|| vec![1; 4]),
            blocked: false,
        })
        .producing()
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut State,
        _: service::Request<'_, S>,
        _: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        handler::RequestState::Complete
    }

    fn output<W: Write>(
        &mut self,
        _: &mut (),
        call: &mut State,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        if call.payload.is_some()
            && let Some(attempts) = self.attempts
        {
            attempts.hit();
        }
        let result = match reply.poll_ready() {
            Ok(Poll::Ready(())) => match call.payload.as_ref() {
                Some(payload) => reply.try_encode(&mut Bytes, payload),
                None => {
                    reply.finish_with(Status::ok());
                    if let Some(completed) = self.completed {
                        completed.hit();
                    }
                    return handler::OutputState::Complete;
                }
            },
            result => result,
        };
        match result {
            Ok(Poll::Ready(())) => {
                call.payload.take();
                if call.blocked
                    && let Some(resumed) = self.resumed
                {
                    resumed.hit();
                }
                handler::OutputState::Produced
            }
            Ok(Poll::Pending) => {
                if !call.blocked {
                    call.blocked = true;
                    if let Some(blocked) = self.blocked {
                        blocked.hit();
                    }
                }
                handler::OutputState::Blocked
            }
            Err(status) => {
                reply.finish_with(status);
                handler::OutputState::Complete
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Encode(alloc::Error),
    Io(io::Error),
    Config(config::Error),
    Settings,
    Protocol(conn::Error),
    Field(field::Error),
    Stream(Id),
    Status(Status),
    StreamReset,
    GoAway,
}

type Result<T> = std::result::Result<T, Error>;

pub fn try_window(socket: &mut TcpStream, value: u8) -> Result<()> {
    let params = [0, 4, 0, 0, 0, value];
    let frame = controls::Settings::new(Ack::Required, &params).ok_or(Error::Settings)?;
    let mut wire = Vec::new();
    frame.encode(&mut wire).map_err(Error::Encode)?;
    socket.write_all(&wire).map_err(Error::Io)
}

pub struct Waiters<const N: usize> {
    socket: TcpStream,
    client: Connection<ClientRole>,
    ids: [Id; N],
}

impl Waiters<3> {
    pub fn try_new(address: SocketAddr) -> Result<Self> {
        Self::try_paths(
            address,
            [b"/capacity/held", b"/capacity/empty", b"/capacity/ready"],
        )
    }
}

impl Waiters<2> {
    pub fn try_pair(address: SocketAddr) -> Result<Self> {
        Self::try_paths(address, [b"/capacity/held", b"/capacity/ready"])
    }

    pub fn try_cancel(self) -> Result<Waiters<1>> {
        let Self {
            socket,
            mut client,
            ids: [held, ready],
        } = self;
        let key = client
            .status()
            .stream_key(held)
            .ok_or(Error::Stream(held))?;
        client
            .streams()
            .try_cancel(key, ErrorCode::Cancel)
            .map_err(conn::Error::InactiveStream)
            .map_err(Error::Protocol)?;
        client.try_flush(&socket).map_err(Error::Io)?;
        Ok(Waiters {
            socket,
            client,
            ids: [ready],
        })
    }
}

impl<const N: usize> Waiters<N> {
    pub fn try_window(&mut self, value: u8) -> Result<()> {
        try_window(&mut self.socket, value)
    }

    fn try_paths(address: SocketAddr, paths: [&[u8]; N]) -> Result<Self> {
        let mut socket =
            TcpStream::connect_timeout(&address, Duration::from_secs(2)).map_err(Error::Io)?;
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(Error::Io)?;
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .map_err(Error::Io)?;
        let mut client = Connection::<ClientRole>::try_new().map_err(Error::Config)?;
        client.try_flush(&socket).map_err(Error::Io)?;
        try_window(&mut socket, 0)?;
        let mut ids = [Id::CONNECTION; N];
        for (id, path) in ids.iter_mut().zip(paths) {
            *id = client
                .streams()
                .headers()
                .request(
                    Peer::try_headers(path).map_err(Error::Field)?,
                    Boundary::More,
                )
                .map_err(Error::Protocol)?;
        }
        client.try_flush(&socket).map_err(Error::Io)?;
        Ok(Self {
            socket,
            client,
            ids,
        })
    }

    pub fn try_read(self) -> Result<[(Vec<u8>, Option<Status>); N]> {
        let Self {
            mut socket,
            mut client,
            ids,
        } = self;
        let mut replies = array::from_fn(|_| (Vec::new(), None));
        let mut input = [0; 4096];
        while replies.iter().any(|(_, status)| status.is_none()) {
            let read = socket.read(&mut input).map_err(Error::Io)?;
            if read == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "response trailers",
                )));
            }
            client
                .ingress()
                .ingest(&input[..read])
                .map_err(Error::Protocol)?;
            while let Some(event) = client.ingress().poll_event() {
                match event {
                    Event::Data {
                        stream_id: id,
                        data,
                        ..
                    } => {
                        let index = ids
                            .iter()
                            .position(|stream| *stream == id.id())
                            .ok_or(Error::Stream(id.id()))?;
                        data.for_each(|bytes| replies[index].0.extend_from_slice(bytes));
                    }
                    Event::Headers {
                        stream_id: id,
                        head,
                        section: Section::Trailing,
                        boundary: Boundary::End,
                        ..
                    } => {
                        let index = ids
                            .iter()
                            .position(|stream| *stream == id.id())
                            .ok_or(Error::Stream(id.id()))?;
                        replies[index].1 =
                            Some(Status::from_fields(head.fields()).map_err(Error::Status)?);
                    }
                    Event::StreamReset { .. } => return Err(Error::StreamReset),
                    Event::GoAway { .. } => return Err(Error::GoAway),
                    _ => {}
                }
            }
        }
        Ok(replies)
    }
}
