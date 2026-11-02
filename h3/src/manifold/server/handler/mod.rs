//! QUIC mux handler that drives generated HTTP/3 tasks.
use dope::core::{
    driver::{result::SubmitCause, schedule},
    io::datagram::{ReceiveError, Residency},
};

use crate::manifold::server::deadline::state::State;
pub(super) mod mode;
pub(super) mod tasks;
use std::marker;

use dope_quic::{
    conn::id,
    runtime::{mux, mux::handler},
    stream::Retain,
    wire::stream::Id,
};
use handler::{
    hook::{
        data::{release, stream},
        life::{close, closing, create, ready},
        wire::{cert, endpoint, packet, socket},
    },
    net,
};
use mux::app;
use o3::buffer::resident;
use sark_core::{
    duplex,
    life::{
        self,
        endpoint::{Event, Mode, Observer, Outcome, Snapshot},
        mode::Observed,
        session,
    },
    manifold::task::app::Runtime,
};
use sark_http::{field::metadata::block, payload::compress::dynamic::hash};
use schedule::ready::task::owner;
use task::{Ready, receive};

use crate::{
    frame, manifold,
    manifold::{
        owners,
        server::{self, deadline, failure, rotation, task},
    },
};

#[pin_project::pin_project(!Unpin)]
pub struct Handler<
    'd,
    R: Runtime<'d>,
    O,
    L = session::Disabled,
    E = deadline::Disabled,
    Q = packet::On,
> {
    #[pin]
    pub(super) router: R,
    observer: O,
    config: server::Setup,
    seed: hash::Seed,
    lifetime: L,
    deadline: E,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    packet: marker::PhantomData<fn() -> Q>,
}
impl<'d, R, M, L, E, Q> Handler<'d, R, M, L, E, Q>
where
    R: Runtime<'d>,
{
    pub(super) fn from_parts(
        router: R,
        mut config: server::Setup,
        observer: M,
        lifetime: L,
        deadline: E,
        seed: hash::Seed,
    ) -> Self {
        match <R as duplex::Application<'d>>::CONNECT {
            block::Connect::Disabled => {}
            block::Connect::Enabled => config.protocol.set_connect(),
        }
        Self {
            router,
            observer,
            config,
            seed,
            lifetime,
            deadline,
            driver: marker::PhantomData,
            packet: marker::PhantomData,
        }
    }
    fn accept<const ID: u8, const FLUSH: bool, B>(
        event: stream::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition
    where
        L: rotation::Policy<'d>,
        E: deadline::Policy<'d>,
        E::State: State<'d>,
        R: Runtime<'d, Target = server::Handle<'d, ID>>,
        M: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        M::Retention: owners::Handoff,
        B: Retain<'d>,
        Self: handler::Handler<
                ID,
                B,
                resident::Shared<'d>,
                id::Driver<'d>,
                Connection = server::Session<'d, R, L, E::State>,
            >,
    {
        let (this, mut slot, handle, event, now) = event.into();
        let (ready, result) = {
            let (state, conn) = slot.split();
            let mut session = Ready { state };
            let this = this.project();
            let result = receive::accept::<FLUSH, R, M, L, E::State, ID, B, true>(
                &mut session,
                this.router,
                this.observer,
                conn,
                handle,
                manifold::Notification::resident(event),
                now,
            );
            conn.application().sync(session.deadline());
            (session.state.has_work(), result)
        };
        slot.set_ready(ready);
        result
    }
}
impl<'d, R, O, L, E, Q, const ID: u8, B>
    handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>> for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    Q: packet::Mode<Self, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    O: server::Mode<'d, ID>,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    B: Retain<'d>,
{
    type Connection = server::Session<'d, R, L, E::State>;
    type Create = Self;
    type Events = O::Events<R, L, E, Q, B>;
}

impl<'d, R, O, L, E, Q, const ID: u8, B> socket::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d>,
    B: Retain<'d>,
    Self: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    O: Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
{
    fn socket(event: socket::Event<'_, Self, ID, id::Driver<'d>>) {
        let (this, event) = event.into_parts();
        let observer = this.project().observer;
        let event = match event {
            net::Event::Transmit(error) => {
                let (conn, error) = error.into_parts();
                failure::Sink::failure(observer, failure::Event::Transmit { conn, error });
                return;
            }
            net::Event::Rejected { error, .. } => {
                failure::Sink::failure(observer, failure::Event::Rejected(error));
                return;
            }
            net::Event::Receive { endpoint, error } => {
                if matches!(error, ReceiveError::Submit(error)
                    if error.cause() == SubmitCause::IdentityExhausted)
                {
                    failure::Sink::failure(observer, failure::Event::Receive(error));
                }
                Event::Receive { endpoint, error }
            }
            net::Event::Accepted(accepted) => Event::Accepted {
                owner: accepted.owner(),
                bytes: accepted.bytes(),
                datagrams: accepted.shape().datagrams(),
                segmented: accepted.shape().is_segmented(),
            },
            net::Event::Transferred(transfer) => Event::Transferred {
                from: transfer.from(),
                to: transfer.to(),
            },
            net::Event::Settled(settled) => {
                let outcome = match settled.outcome() {
                    net::Outcome::Sent(bytes) => Outcome::Sent { bytes: *bytes },
                    net::Outcome::Failed(errno) => Outcome::Failed { errno: *errno },
                    net::Outcome::Submit(error) => {
                        if error.cause() == SubmitCause::IdentityExhausted {
                            failure::Sink::failure(observer, failure::Event::Submit(*error));
                        }
                        Outcome::Submit(*error)
                    }
                    net::Outcome::Shutdown => Outcome::Shutdown,
                };
                Event::Settled {
                    owner: settled.owner(),
                    bytes: settled.payload().len(),
                    outcome,
                }
            }
        };
        Mode::observe(observer, event);
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> closing::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d>,
    B: Retain<'d>,
    Self: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
{
    fn closing(event: closing::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>) {
        let (this, _, handle) = event.into();
        life::Mode::observe(this.project().observer, life::Event::SessionClosing(handle));
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> close::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    Self: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
            Create = Self,
        >,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: Retain<'d>,
{
    /// QUIC has consumed the transport connection before issuing this event.
    fn close(event: close::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>) {
        let (this, _, state, handle) = event.into_parts();
        let mut this = this.project();
        let snapshot = state.cancel_owned::<ID, _>(this.router.as_mut(), this.observer, handle);
        if let Err(cause) = Runtime::try_retire(this.router.as_ref(), owner::Owner::from(handle)) {
            life::Mode::diagnostic(this.observer, &cause);
        }
        life::Mode::close(this.observer, handle, move || snapshot.wire_bytes(0));
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> endpoint::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, Observed<O>, L, E, Q>
where
    R: Runtime<'d>,
    B: Retain<'d>,
    Self: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    O: Observer<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>,
{
    fn endpoint(event: endpoint::Event<'_, Self, ID, id::Driver<'d>>) {
        match event {
            endpoint::Event::Opened { handler, endpoint } => {
                Observer::observe(
                    handler.project().observer.observer_mut(),
                    Event::Opened(endpoint),
                );
            }
            endpoint::Event::Finalized {
                handler,
                endpoint,
                receive,
            } => {
                let snapshot = Snapshot {
                    receive_capacity_bytes: receive.map(Residency::capacity),
                    receive_resident_bytes: receive.map(Residency::resident),
                };
                let observer = handler.project().observer.observer_mut();
                Observer::observe(&mut *observer, Event::Audit { endpoint, snapshot });
                Observer::observe(observer, Event::Closed(endpoint));
            }
        }
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> release::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d>,
    B: Retain<'d>,
    Self: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
{
    fn release(event: release::Event<'_, Self, ID, id::Driver<'d>>) -> app::Readiness {
        let (this, handle, release) = event.into_parts();
        let mut proof = owners::Projection::new(this.project().observer, handle);
        proof.released(release);
        app::Readiness::Preserve
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> ready::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    Self: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
            Create = Self,
        >,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: Retain<'d>,
{
    fn established(event: ready::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>) {
        let (this, mut slot, handle, now) = event.into();
        let ready = {
            let (state, conn) = slot.split();
            let mut session = Ready { state };
            let this = this.project();
            let prepared = (|| -> Result<(), manifold::Error> {
                let owner = session.state.h3.bind_scope(conn)?.owner();
                for identity in session.state.requests.reserved() {
                    let bound = conn.streams().resident().bind(owner, identity.local())?;
                    conn.streams().binding().try_prepare(bound)?;
                }
                Ok(())
            })();
            if let Err(error) = prepared {
                manifold::close_connection_scoped(conn, error.code());
                return;
            }
            let report = match session
                .state
                .h3
                .egress()
                .start_with(conn, this.observer, handle)
            {
                Ok(report) => report,
                Err(error) => {
                    manifold::close_connection_scoped(conn, error.code());
                    return;
                }
            };
            if let Err(error) = session.state.account(report) {
                manifold::close_connection_scoped(conn, server::request_error_code(error));
                return;
            }
            if let Err(error) =
                rotation::State::try_open(&mut session.state.rotation, this.lifetime, now)
            {
                let code = match error {
                    rotation::Error::Age { .. }
                    | rotation::Error::Open { .. }
                    | rotation::Error::Draining { .. } => frame::ErrorCode::Internal,
                };
                manifold::close_connection_scoped(conn, code);
                return;
            }
            conn.application().sync(session.deadline());
            life::Mode::observe(this.observer, life::Event::SessionEstablished(handle));
            session.state.has_work()
        };
        slot.set_ready(ready);
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B>
    create::Mode<Self, ID, B, resident::Shared<'d>, id::Driver<'d>> for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
    Self: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
            Create = Self,
        >,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: Retain<'d>,
{
    fn create(
        event: create::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> create::Decision<server::Session<'d, R, L, E::State>> {
        let (this, mut conn, handle) = event.into_parts();
        let view = this.as_ref().project_ref();
        let session = match server::Session::try_new(view.config, view.deadline, view.seed.clone())
        {
            Ok(session) => session,
            Err(error) => {
                failure::Sink::failure(
                    this.project().observer,
                    failure::Event::Create {
                        conn: handle,
                        error: &error,
                    },
                );
                manifold::close_connection_scoped(&mut conn, frame::ErrorCode::Internal);
                return create::Decision::Reject;
            }
        };
        if let Err(cause) = Runtime::register(
            this.as_ref().project_ref().router,
            owner::Owner::from(handle),
        ) {
            life::Mode::diagnostic(this.project().observer, &cause);
            manifold::close_connection_scoped(&mut conn, frame::ErrorCode::Internal);
            return create::Decision::Reject;
        }
        life::Mode::observe(this.project().observer, life::Event::SessionOpened(handle));
        create::Decision::Install(session)
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> stream::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>,
    Self: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
            Create = Self,
        >,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: Retain<'d>,
{
    fn stream(
        event: stream::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        Self::accept::<ID, true, B>(event)
    }

    fn early(
        event: stream::Event<'_, Self, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        Self::accept::<ID, false, B>(event)
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B> packet::Sink<ID, B, resident::Shared<'d>, id::Driver<'d>>
    for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d>,
    B: Retain<'d>,
    O: failure::Packet,
{
    fn packet(event: packet::Event<'_, Self>) {
        let (handler, from, error, len) = event.into_parts();
        handler.project().observer.packet(from, len, error);
    }
}

impl<'d, R, O, L, E, Q, const ID: u8> cert::Sink<ID, id::Driver<'d>> for Handler<'d, R, O, L, E, Q>
where
    R: Runtime<'d>,
    O: failure::Sink<id::Handle<id::Driver<'d>, ID>>,
{
    fn certificate(event: cert::Event<'_, '_, Self, ID, id::Driver<'d>>) {
        let (handler, _, _, error) = event.into_parts();
        handler
            .project()
            .observer
            .failure(failure::Event::Certificate(error));
    }
}
