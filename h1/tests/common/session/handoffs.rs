use std::{cell::Cell, convert, pin::Pin};

use super::Frame;
use conn::write;
use dope::{
    core::{
        driver::{
            self,
            route::{self, KeyTag},
            schedule::clock,
            settings,
            settings::{claim, errors},
        },
        random,
    },
    manifold::{
        app::{env::Bundle, receive},
        network::listener::{
            conn, handler,
            handler::{Application, PostDrain, failures, retire, send},
            topology::factory,
        },
        timing::Throughput,
    },
    net::{
        link::egress::data,
        tcp,
        wire::{Identity, domain, domain::Local},
    },
};
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident::bound, storage};
use sark_h1::{
    pump,
    server::payload,
    session, switch,
    wire::{head, output},
};
use sark_http::{h1::switch::Kind, payload::fixed};

const SWITCH_LEN: usize = 1024 * 1024;
pub static SWITCH_BYTES: [u8; SWITCH_LEN] = [b'S'; SWITCH_LEN];

pub struct Staged {
    owner: u8,
    tail: Option<storage::Shared>,
}

pub struct App<'a> {
    pub sent: Gate,
    pub moved: Gate,
    pub closed: Gate,
    pub raw: &'a Cell<Vec<u8>>,
    pub owner: &'a Cell<Option<u8>>,
    pub rejected: &'a Cell<Option<pump::Reject>>,
}

impl App<'_> {
    fn absorb_raw(&self, bytes: &[u8]) {
        if !bytes.is_empty() {
            let mut raw = self.raw.take();
            raw.extend_from_slice(bytes);
            self.raw.set(raw);
        }
    }
}

impl<'observe: 'd, 'd, ID: route::Identity> Application<'d, ID> for App<'observe> {
    type Conn = session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = data::Bound<payload::Owner<'d>, bound::Unbounded>;
    type Input = receive::Gated;
    type Send = send::Silent;

    fn connection(self: Pin<&Self>, _: &random::Seed) -> Result<Self::Conn, Self::Build> {
        Ok(session::Conn::default())
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn is_blocked(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Identity, Self::Conn, Self::Egress>,
    ) -> bool {
        matches!(conn.state().mode(), switch::Mode::Switching(_))
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn, Self::Egress>,
    ) {
        self.get_mut().closed.hit();
    }
}

impl<'observe: 'd, 'd, ID: route::Identity> handler::Gated<'d, ID> for App<'observe> {
    fn send_drained(
        self: Pin<&mut Self>,
        mut conn: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn, Self::Egress>,
    ) -> handler::PostDrain {
        let this = self.get_mut();
        if let Some((staged, input)) = session::Conn::finish_switch(&mut conn) {
            assert!(input.is_none(), "affine tail stayed out of HTTP input");
            if let Some(tail) = staged.tail.take() {
                this.absorb_raw(tail.as_slice());
            }
            this.owner.set(Some(staged.owner));
            assert_eq!(conn.state().mode(), switch::Mode::Raw(Kind::Connect));
            this.moved.hit();
        }
        PostDrain::Idle
    }
}

impl<'observe: 'd, 'd, ID: route::Identity> handler::Borrowed<'d, ID> for App<'observe> {
    fn chunk<'e, R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn, Self::Egress>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        let this = self.get_mut();
        if let Ok(raw) = conn.state().raw() {
            assert_eq!(raw.owner, 7);
            this.absorb_raw(chunk.as_ref());
            conn.close_after();
            return receive::Outcome::Continue;
        }
        let http = conn.state_mut();
        let bytes = match http.preflight(chunk.as_ref()) {
            pump::Preflight::Drive => chunk.as_ref(),
            pump::Preflight::Resume => &[][..],
            pump::Preflight::Settled | pump::Preflight::Finished => {
                return receive::Outcome::Continue;
            }
            pump::Preflight::Rejected(error) => {
                this.rejected.set(Some(error));
                return receive::Outcome::CloseAfter;
            }
        };

        let mut native_slot = std::mem::MaybeUninit::uninit();
        let mut native = conn
            .try_output(driver, &mut native_slot)
            .expect("switch output admission");
        let receipt = {
            let (http, writer) = native.get_mut().parts();
            let mut disp = Frame {
                next: Some(session::Outcome::Switch {
                    next: switch::Transition::new(
                        Kind::Connect,
                        Staged {
                            owner: 7,
                            tail: None,
                        },
                    ),
                    buffered: session::Buffered::new(1),
                    response: output::Switch::Static(fixed::Body::new(&SWITCH_BYTES)),
                }),
                ..Frame::default()
            };
            http.drive(Pin::new(&mut disp), bytes, writer)
        };
        assert!(matches!(receipt.connection, head::Connection::KeepAlive));
        assert!(
            matches!(&receipt.state, Ok(pump::Egress::Static { .. })),
            "switch must publish its exact static response owner"
        );
        let egress = receipt.state.expect("switch protocol state");
        let result = payload::write::try_publish(native, egress.into_body(()), receipt.connection);
        result
            .map_err(|cause| match cause {
                write::body::Failure::Payload(cause) => format!("payload: {cause:?}"),
                write::body::Failure::Capacity(cause) => format!("capacity: {cause:?}"),
                write::body::Failure::Mismatch(cause) => format!("mismatch: {cause:?}"),
                write::body::Failure::Reserve(cause) => format!("reserve: {cause:?}"),
                write::body::Failure::Bound { charge, maximum } => {
                    format!("bound: {charge:?}, {maximum}")
                }
            })
            .expect("switch output");
        let pump::Tail::Handoff { range, .. } = receipt.tail else {
            return receive::Outcome::Failed(receive::Failure::Contract);
        };
        let tail = chunk
            .try_retain()
            .expect("retain tail")
            .project(range)
            .map_err(o3::mem::Rejected::discard)
            .expect("restrict tail");
        let staged = conn
            .state_mut()
            .staged_mut()
            .expect("switching before drain");
        assert!(
            staged.tail.replace(tail).is_none(),
            "one affine switch tail"
        );
        this.sent.hit();
        receive::Outcome::Continue
    }
}

impl settings::Resource for App<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl factory::ReceiveSpec for App<'_> {
    type Input = receive::Gated;
}

impl factory::Direct<KeyTag<0>, Bundle<tcp::Tcp, Identity, Throughput>> for App<'_> {
    type Application<'d>
        = Self
    where
        Self: 'd;
    type Error = convert::Infallible;
    type Wire<'d>
        = domain::Local<Identity>
    where
        Self: 'd;

    fn build<'d>(self) -> Result<(Self, Self::Wire<'d>), Self::Error>
    where
        Self: 'd,
    {
        Ok((self, Local::new(Identity)))
    }
}
