use std::{cell::Cell, convert, pin::Pin, rc::Rc};

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
        link::egress::data::{self, compose},
        tcp,
        wire::{Identity, domain, domain::Local},
    },
};
use dope_test::harness::signals::Gate;
use o3::buffer::{Complete as _, bytes::TryRetain, resident::bound, storage};
use sark_h1::{
    Input, pump,
    pump::Completion,
    server::payload,
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
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>,
    ) -> RouteOutcome<'d, convert::Infallible, Staged, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: o3::buffer::write::Slice<'_, &mut usize>,
        conn: &mut session::Conn<'d, (), convert::Infallible, switch::Enabled<Staged>>,
    ) -> RouteOutcome<'d, convert::Infallible, Staged, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn try_consume(
        self: Pin<&mut Self>,
        _: session::Permit,
        _: Input<'_>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
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
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Staged> {
        match pending {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready, Staged> {
        match ready {}
    }
}

type Owner<'d> = compose::Either<payload::Body<'d, ()>, payload::Stream<'d, ()>>;
pub struct App<'a> {
    pub sent: Gate,
    pub moved: Gate,
    pub closed: Gate,
    pub raw: Rc<Cell<Vec<u8>>>,
    pub owner: Rc<Cell<Option<u8>>>,
    pub checks: &'a [Cell<usize>; 9],
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
        let http = conn.state_mut();
        if matches!(pump::Drain::complete(http), Completion::Switch) {
            let mode = http.mode();
            let input = http.take_input();
            assert_eq!(mode, switch::Mode::Raw(Kind::Connect));
            assert!(input.is_none(), "affine tail stayed out of HTTP input");
            this.checks[0].set(this.checks[0].get() + 1);
            let staged = http.raw_mut();
            assert!(staged.is_ok(), "drain must commit the raw owner");
            this.checks[1].set(this.checks[1].get() + 1);
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
            assert_eq!(raw.owner, 7);
            this.checks[2].set(this.checks[2].get() + 1);
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
                this.rejected.set(Some(error));
                return receive::Outcome::CloseAfter;
            }
        };

        let commit = conn.write().try_commit(driver, |state, mut prefix| {
            let http = state;
            let mut disp = SwitchOne;
            let cycle = pump::Ingress::drive(
                http,
                Pin::new(&mut disp),
                bytes,
                prefix.writer(),
                head::Connection::KeepAlive,
            );
            assert!(matches!(cycle.connection, head::Connection::KeepAlive));
            assert!(
                matches!(&cycle.state, Ok(pump::Egress::Static { .. })),
                "switch must publish its exact static response owner"
            );
            this.checks[3].set(this.checks[3].get() + 1);
            payload::write::try_cycle(prefix, cycle, |_| ())
        });
        let commit = commit
            .map_err(|error| match error {
                write::error::Error::Capacity(cause) => Err(Err(cause)),
                write::error::Error::Build((cause, _)) => Ok((
                    cause.load.charge().bytes().get(),
                    cause.load.charge().resident().get(),
                    cause.maximum,
                )),
                write::error::Error::Mismatch(error) => Err(Ok(error.into_parts().0)),
            })
            .expect("switch response admission");
        assert!(
            matches!(&commit, write::Outcome::Committed(_)),
            "switch response must commit"
        );
        if let write::Outcome::Committed(receipt) = commit {
            assert!(
                receipt.state.is_ok(),
                "switch response committed its protocol state"
            );
            assert!(
                matches!(&receipt.tail, pump::Tail::Handoff { .. }),
                "switch response suffix"
            );
            this.checks[4].set(this.checks[4].get() + 1);
            let tail = receipt.tail;
            if let pump::Tail::Handoff { staged: (), range } = tail {
                let tail = chunk.try_retain();
                assert!(tail.is_ok(), "retain tail: {:?}", tail.as_ref().err());
                this.checks[5].set(this.checks[5].get() + 1);
                let Ok(tail) = tail else {
                    return receive::Outcome::CloseAfter;
                };
                let tail = tail
                    .project(range)
                    .map_err(o3::mem::Rejected::discard)
                    .expect("restrict tail");
                this.checks[6].set(this.checks[6].get() + 1);
                let staged = conn.state_mut().staged_mut();
                assert!(staged.is_ok(), "raw owner must remain staged until drain");
                this.checks[7].set(this.checks[7].get() + 1);
                if let Ok(staged) = staged {
                    let previous = staged.tail.replace(tail);
                    assert!(previous.is_none(), "one affine switch tail");
                    this.checks[8].set(this.checks[8].get() + 1);
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
