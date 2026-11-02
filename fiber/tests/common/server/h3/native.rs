use dope::{core::driver::schedule::clock, runtime::control};
use dope_quic::{
    conn::id,
    runtime::{
        embedded::{self, link},
        mux::{
            app::Driver,
            handler,
            handler::hook::{
                set,
                wire::{cert, packet},
            },
        },
    },
    stream::RecvBuffer,
};
use o3::buffer::resident;
use sark_h3::manifold::server::Tasks;

use super::{
    Client,
    proof::{DIALER, LISTENER},
};

type ClientControl<'step, 'a, 'd> = embedded::Control<
    'step,
    'd,
    DIALER,
    Client<'a, packet::Off>,
    RecvBuffer<'d, false>,
    embedded::Idle,
>;
type ServerControl<'step, 'd, H> =
    embedded::Control<'step, 'd, LISTENER, H, RecvBuffer<'d, false>, embedded::Run<Tasks>>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub(crate) struct Native<'a, 'd, H, F>
where
    H: handler::Handler<LISTENER, RecvBuffer<'d, false>, resident::Shared<'d>, id::Driver<'d>>
        + cert::Sink<LISTENER, id::Driver<'d>>,
    H::Events: set::Dispatch<
            H,
            LISTENER,
            RecvBuffer<'d, false>,
            resident::Shared<'d>,
            id::Driver<'d>,
            Packet = packet::Off,
        >,
    Tasks: Driver<'d, H, LISTENER, RecvBuffer<'d, false>, resident::Shared<'d>>,
    F: for<'step> FnMut(
        ClientControl<'step, 'a, 'd>,
        ServerControl<'step, 'd, H>,
        link::Control<'step, 'd, 19, DIALER, LISTENER>,
        link::Control<'step, 'd, 20, LISTENER, DIALER>,
        clock::Moment<'d>,
    ) -> control::Flow,
{
    #[pin]
    #[manifold(control)]
    pub(crate) client:
        embedded::Endpoint<'d, DIALER, Client<'a, packet::Off>, RecvBuffer<'d, false>>,
    #[pin]
    #[manifold(control)]
    pub(crate) server:
        embedded::Endpoint<'d, LISTENER, H, RecvBuffer<'d, false>, embedded::Run<Tasks>>,
    #[pin]
    #[manifold(control)]
    pub(crate) forward: link::Link<'d, 19, DIALER, LISTENER>,
    #[pin]
    #[manifold(control)]
    pub(crate) reverse: link::Link<'d, 20, LISTENER, DIALER>,
    #[dispatcher(state)]
    pub(crate) run: F,
}

impl<'a, 'd, H, F> Native<'a, 'd, H, F>
where
    H: handler::Handler<LISTENER, RecvBuffer<'d, false>, resident::Shared<'d>, id::Driver<'d>>
        + cert::Sink<LISTENER, id::Driver<'d>>,
    H::Events: set::Dispatch<
            H,
            LISTENER,
            RecvBuffer<'d, false>,
            resident::Shared<'d>,
            id::Driver<'d>,
            Packet = packet::Off,
        >,
    Tasks: Driver<'d, H, LISTENER, RecvBuffer<'d, false>, resident::Shared<'d>>,
    F: for<'step> FnMut(
        ClientControl<'step, 'a, 'd>,
        ServerControl<'step, 'd, H>,
        link::Control<'step, 'd, 19, DIALER, LISTENER>,
        link::Control<'step, 'd, 20, LISTENER, DIALER>,
        clock::Moment<'d>,
    ) -> control::Flow,
{
    fn coordinate(this: native_coord::Fields<'_, 'a, 'd, H, F>) -> control::Flow {
        (this.run)(
            this.client,
            this.server,
            this.forward,
            this.reverse,
            this.step.now(),
        )
    }
}
