use dope::{core::driver::schedule::clock, runtime::control};
use dope_quic::{
    conn::{id, stream::resident::Key},
    errors::Code,
    runtime::{
        embedded::{
            self,
            link::{self, Link},
        },
        mux::{
            self,
            handler::{
                self,
                hook::{
                    data::stream,
                    life::{create, ready},
                    set,
                    wire::cert,
                },
            },
        },
    },
    stream::RecvBuffer,
    wire::stream::Id,
};
use o3::buffer::resident;
use sark_h3::manifold::{self, Notification, Session};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

pub const SERVER: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443));
pub const CLIENT: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000));

pub struct Receiver<'d> {
    pub session: Option<Session<'d>>,
    pub ready: bool,
    pub first: Option<(Id, Option<Key>)>,
    pub terminal: Option<(Id, Code, Option<Key>)>,
    pub terminals: usize,
    pub failure: Option<manifold::Error>,
    pub certificate_failed: bool,
    pub generation_changed: bool,
}

impl<'d> Receiver<'d> {
    pub fn new(session: Session<'d>) -> Self {
        Self {
            session: Some(session),
            ready: false,
            first: None,
            terminal: None,
            terminals: 0,
            failure: None,
            certificate_failed: false,
            generation_changed: false,
        }
    }
}

impl<'d, const ID: u8> handler::Handler<ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Receiver<'d>
{
    type Connection = Session<'d>;
    type Create = Self;
    type Events = set::Set<set::On, set::Off, set::On>;
}

impl<'d, const ID: u8> create::Mode<Self, ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Receiver<'d>
{
    fn create(
        event: create::Event<'_, Self, ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
    ) -> create::Decision<Session<'d>> {
        let (this, _, _) = event.into_parts();
        match this.get_mut().session.take() {
            Some(session) => create::Decision::Install(session),
            None => create::Decision::Reject,
        }
    }
}

impl<'d, const ID: u8> ready::Sink<ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Receiver<'d>
{
    fn established(
        event: ready::Event<'_, Self, ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        let (this, _, _, _) = event.into();
        this.get_mut().ready = true;
    }
}

impl<'d, const ID: u8> stream::Sink<ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Receiver<'d>
{
    fn stream(
        event: stream::Event<'_, Self, ID, RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        use dope_quic::conn::stream::Event;

        let (this, mut slot, _, event, _) = event.into();
        let this = this.get_mut();
        match event.event() {
            Event::Readable { id } => {
                let first = this.first.get_or_insert((id, event.key()));
                this.generation_changed |= *first != (id, event.key());
                return mux::Disposition::Defer;
            }
            Event::Reset { id, error_code } | Event::Stopped { id, error_code } => {
                this.terminal = Some((id, error_code, event.key()));
                this.terminals += 1;
            }
            Event::Writable => {}
        }
        let (h3, quic) = slot.split();
        if let Err(error) = h3.ingress().ingest(quic, Notification::resident(event)) {
            this.failure = Some(error);
        }
        mux::Disposition::Accept
    }
}

impl<'d, const ID: u8> cert::Sink<ID, id::Driver<'d>> for Receiver<'d> {
    fn certificate(event: cert::Event<'_, '_, Self, ID, id::Driver<'d>>) {
        let (this, _, _, _) = event.into_parts();
        this.get_mut().certificate_failed = true;
    }
}

pub type EndpointControl<'step, 'd, const ID: u8> =
    embedded::Control<'step, 'd, ID, Receiver<'d>, RecvBuffer<'d>, embedded::Idle>;

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub struct Native<'d, F>
where
    F: for<'step> FnMut(
        EndpointControl<'step, 'd, 1>,
        EndpointControl<'step, 'd, 2>,
        link::Control<'step, 'd, 3, 1, 2>,
        link::Control<'step, 'd, 4, 2, 1>,
        clock::Moment<'d>,
    ) -> control::Flow,
{
    #[pin]
    #[manifold(control)]
    pub client: embedded::Endpoint<'d, 1, Receiver<'d>, RecvBuffer<'d>>,
    #[pin]
    #[manifold(control)]
    pub server: embedded::Endpoint<'d, 2, Receiver<'d>, RecvBuffer<'d>>,
    #[pin]
    #[manifold(control)]
    pub forward: Link<'d, 3, 1, 2>,
    #[pin]
    #[manifold(control)]
    pub reverse: Link<'d, 4, 2, 1>,
    #[dispatcher(state)]
    pub run: F,
}

impl<'d, F> Native<'d, F>
where
    F: for<'step> FnMut(
        EndpointControl<'step, 'd, 1>,
        EndpointControl<'step, 'd, 2>,
        link::Control<'step, 'd, 3, 1, 2>,
        link::Control<'step, 'd, 4, 2, 1>,
        clock::Moment<'d>,
    ) -> control::Flow,
{
    pub fn new(
        client: embedded::Endpoint<'d, 1, Receiver<'d>, RecvBuffer<'d>>,
        server: embedded::Endpoint<'d, 2, Receiver<'d>, RecvBuffer<'d>>,
        forward: Link<'d, 3, 1, 2>,
        reverse: Link<'d, 4, 2, 1>,
        run: F,
    ) -> Self {
        Self {
            client,
            server,
            forward,
            reverse,
            run,
        }
    }

    fn coordinate(this: native_coord::Fields<'_, 'd, F>) -> control::Flow {
        (this.run)(
            this.client,
            this.server,
            this.forward,
            this.reverse,
            this.step.now(),
        )
    }
}
