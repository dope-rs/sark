use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use conn::write::{self, error};
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
        link::egress::{
            self,
            data::{self, compose},
        },
        tcp,
        wire::{Identity, domain, domain::Local},
    },
};
use dope_test::harness::signals::Gate;
use o3::buffer::{Complete as _, bytes::TryRetain, resident::bound, storage, storage::range};
use sark_h1::{
    Input, pump,
    pump::Completion,
    server::{payload, payload::write::CycleCommit},
    session,
    session::{Dispatch, RouteOutcome},
    switch,
    wire::{head, output, parsing},
};
use sark_http::{
    h1::switch::Kind,
    payload::{fixed, owned},
};

const SWITCH_LEN: usize = 1024 * 1024;
pub static SWITCH_BYTES: [u8; SWITCH_LEN] = [b'S'; SWITCH_LEN];

pub struct Staged {
    owner: u8,
    tail: Option<storage::Shared>,
}

struct SwitchOne;

impl<'d> Dispatch<'d, (), convert::Infallible, switch::Enabled<Staged>> for SwitchOne {
    type Ready = convert::Infallible;
    type Head = convert::Infallible;

    fn resume_head(
        self: Pin<&mut Self>,
        pending: Self::Head,
        _: session::Permit,
        _: Input<'_>,
        _: &mut [u8],
        _: &mut session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>,
    ) -> RouteOutcome<'d, convert::Infallible, Staged, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>,
    ) -> RouteOutcome<'d, convert::Infallible, Staged, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn try_consume(
        self: Pin<&mut Self>,
        _: session::Permit,
        _: Input<'_>,
        _: &mut [u8],
        _: &mut session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>,
    ) -> RouteOutcome<'d, convert::Infallible, Staged, Self::Head, parsing::Rejected<'d>> {
        let next = switch::Transition::new(
            Kind::Connect,
            Staged {
                owner: 7,
                tail: None,
            },
        );
        session::Outcome::Switch {
            next,
            buffered: session::Buffered::new(1),
            response: output::Switch::Static(fixed::Body::new(&SWITCH_BYTES)),
        }
        .into()
    }

    fn body_data(
        self: Pin<&mut Self>,
        pending: convert::Infallible,
        _: owned::Segments<'d>,
        _: &mut (),
    ) -> Result<convert::Infallible, fixed::Body<'d>> {
        match pending {}
    }

    fn has_body_work(_: &convert::Infallible) -> bool {
        false
    }

    fn resume_pending(
        self: Pin<&mut Self>,
        pending: convert::Infallible,
        _: &mut (),
    ) -> Result<convert::Infallible, fixed::Body<'d>> {
        Ok(pending)
    }

    fn finish_body(
        self: Pin<&mut Self>,
        pending: convert::Infallible,
        _: session::Complete,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Staged> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Staged> {
        match ready {}
    }
}

type Owner<'d> = compose::Either<payload::Body<'d, ()>, payload::Stream<'d, ()>>;
type Receipt = CycleCommit<(), ()>;
type Commit<'d> = Result<
    write::Outcome<Receipt>,
    error::Error<
        data::Bound<Owner<'d>, bound::Unbounded>,
        Receipt,
        (egress::Excess<payload::Body<'d, ()>>, Receipt),
    >,
>;

pub enum Event<'a, 'd> {
    Switch(switch::Mode, Option<storage::Shared>),
    Raw(&'a Result<&'a mut Staged, switch::Mode<convert::Infallible>>),
    Owner(u8),
    Preflight(&'a pump::Reject),
    Cycle(&'a pump::Cycle<'d, ()>),
    Commit(&'a Commit<'d>),
    Retained(&'a Result<storage::Shared, storage::AllocError>),
    Restricted(&'a Result<(), range::Error>),
    Staged(&'a Result<&'a mut Staged, switch::Mode<Kind, convert::Infallible>>),
    Replaced(Option<storage::Shared>),
}

pub struct App<'a> {
    pub sent: Gate,
    pub moved: Gate,
    pub closed: Gate,
    pub raw: Rc<Cell<Vec<u8>>>,
    pub owner: Rc<Cell<Option<u8>>>,
    pub observe: &'a dyn Fn(Event<'_, '_>),
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
    type Egress = data::Bound<Owner<'d>, bound::Unbounded>;
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

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn, Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        self.get_mut().closed.hit();
        conn
    }
}

impl<'observe: 'd, 'd, ID: route::Identity> handler::Gated<'d, ID> for App<'observe> {
    fn send_drained(
        self: Pin<&mut Self>,
        mut conn: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn, Self::Egress>,
    ) -> handler::PostDrain {
        let this = self.get_mut();
        let http = conn.state_mut();
        if matches!(pump::Drain::complete(http), Completion::Switch) {
            (this.observe)(Event::Switch(http.mode(), http.take_input()));
            let staged = http.raw_mut();
            (this.observe)(Event::Raw(&staged));
            if let Ok(staged) = staged {
                if let Some(tail) = staged.tail.take() {
                    this.absorb_raw(tail.as_slice());
                }
                this.owner.set(Some(staged.owner));
                this.moved.hit();
            }
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
            (this.observe)(Event::Owner(raw.owner));
            this.absorb_raw(chunk.as_ref());
            conn.close_after();
            return receive::Outcome::Continue;
        }
        let http = conn.state_mut();
        let bytes = match pump::Ingress::preflight(http, chunk.as_ref()) {
            pump::Preflight::Drive => chunk.as_ref(),
            pump::Preflight::Resume => &[][..],
            pump::Preflight::Settled | pump::Preflight::Finished => {
                return receive::Outcome::Continue;
            }
            pump::Preflight::Rejected(error) => {
                (this.observe)(Event::Preflight(&error));
                return receive::Outcome::CloseAfter;
            }
        };

        let commit = conn.write().try_draft(driver, |state, prefix| {
            let http = state;
            let mut disp = SwitchOne;
            let cycle = pump::Ingress::drive(
                http,
                Pin::new(&mut disp),
                bytes,
                prefix,
                head::Connection::KeepAlive,
            );
            (this.observe)(Event::Cycle(&cycle));
            payload::write::try_cycle(cycle, |_| ())
        });
        (this.observe)(Event::Commit(&commit));
        if let Ok(write::Outcome::Committed(receipt)) = commit {
            let tail = receipt.tail;
            if let pump::Tail::Handoff { staged: (), range } = tail {
                let tail = chunk.try_retain();
                (this.observe)(Event::Retained(&tail));
                let Ok(mut tail) = tail else {
                    return receive::Outcome::CloseAfter;
                };
                let restrict = tail.try_restrict(range);
                (this.observe)(Event::Restricted(&restrict));
                if restrict.is_err() {
                    return receive::Outcome::CloseAfter;
                }
                let staged = conn.state_mut().staged_mut();
                (this.observe)(Event::Staged(&staged));
                if let Ok(staged) = staged {
                    (this.observe)(Event::Replaced(staged.tail.replace(tail)));
                    this.sent.hit();
                }
            }
        }
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
        = domain::Local<'d, Identity, KeyTag<0>>
    where
        Self: 'd;

    fn build<'d>(self) -> Result<(Self, Self::Wire<'d>), Self::Error>
    where
        Self: 'd,
    {
        Ok((self, Local::new(Identity)))
    }
}
