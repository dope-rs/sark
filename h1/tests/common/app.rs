use std::{cell::Cell, convert::Infallible, pin::Pin, ptr};

use dope::{
    core::{
        driver::{self, route, schedule::clock, settings},
        random,
    },
    manifold::{
        app::{env::Bundle, receive},
        network::listener::{conn, handler, topology::factory},
        timing::Throughput,
    },
    net::{
        link::{
            egress::data::{self, compose},
            slot::types::CloseGate,
        },
        tcp,
        wire::{Identity, domain},
    },
};
use dope_test::harness::signals::Gate;
use handler::{failures, retire, send};
use o3::buffer::{Complete as _, bytes::TryRetain, hold, resident::bound, storage};
use sark_h1::{
    Input,
    server::{
        conn::{
            driver::{self as h1_driver, Flow},
            router::Routes,
        },
        payload,
    },
    session,
    wire::{Failure, InputError, head, output, parsing},
};
use sark_http::payload::{fixed, owned};
use settings::{claim, errors};

pub const PREFIX: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA";

#[derive(Default)]
pub struct Facts {
    pub routed: Cell<usize>,
    pub rejected: Cell<usize>,
    pub deferred: Cell<usize>,
    pub committed: Cell<usize>,
    pub released: Cell<usize>,
    pub permits: Cell<usize>,
    pub closed: Gate,
    pub failure: Cell<Option<hold::Charge>>,
    pub checks: [Cell<usize>; 7],
}

pub struct App<'a> {
    pub reason: &'a [u8],
    pub facts: &'a Facts,
    pub close_early: bool,
}

impl<'a> App<'a> {
    fn finish<'d, ID: route::Identity>(
        &mut self,
        conn: &mut conn::Ctx<
            '_,
            '_,
            '_,
            '_,
            '_,
            'd,
            ID,
            Identity,
            <Self as handler::Application<'d, ID>>::Conn,
            <Self as handler::Application<'d, ID>>::Egress,
        >,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        'a: 'd,
    {
        if conn.state().action().is_none() {
            return;
        }
        let (flow, _) = h1_driver::progress(Pin::new(&mut *self), conn, driver).into_parts();
        let actual = conn.state().action().map(|reason| reason.as_slice());
        let closing = conn.is_closing();
        match flow {
            Flow::Continue => {
                assert!(ptr::eq(actual.expect("deferred reason"), self.reason));
                assert_eq!(closing, self.close_early);
                self.facts.checks[0].set(self.facts.checks[0].get() + 1);
                self.facts.deferred.set(self.facts.deferred.get() + 1);
            }
            Flow::Closed => {
                assert!(actual.is_none());
                assert!(closing);
                self.facts.checks[1].set(self.facts.checks[1].get() + 1);
                self.facts.committed.set(self.facts.committed.get() + 1);
            }
        }
    }
}

impl<'a: 'd, 'd, ID: route::Identity> handler::Application<'d, ID> for App<'a> {
    type Conn = session::Conn<'d, (), Infallible>;
    type Build = Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress =
        data::Bound<compose::Either<payload::Body<'d>, payload::Stream<'d, ()>>, bound::AtMost<0>>;
    type Input = receive::Coupled;
    type Send = send::Run;

    fn connection(self: Pin<&Self>, _: &random::Seed) -> Result<Self::Conn, Self::Build> {
        Ok(session::Conn::default())
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().finish(&mut conn, driver);
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) {
        assert!(conn.state().action().is_none());
        self.facts.checks[2].set(self.facts.checks[2].get() + 1);
        self.facts.closed.hit();
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        match conn.state().action() {
            Some(_) => CloseGate::Deferred,
            None => CloseGate::Immediate,
        }
    }
}

impl<'a: 'd, 'd, ID: route::Identity> handler::Borrowed<'d, ID> for App<'a> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        let this = self.get_mut();
        let (flow, _, _) =
            h1_driver::run(Pin::new(&mut *this), chunk.as_ref(), &mut conn, driver).into_parts();
        if matches!(flow, Flow::Closed) {
            assert!(this.facts.failure.get().is_some());
            this.facts.checks[3].set(this.facts.checks[3].get() + 1);
            return receive::Outcome::CloseAfter;
        }
        if conn.state().action().is_none() {
            return receive::Outcome::Continue;
        }
        while let Some(permit) = conn.work().permit() {
            permit.run(|| this.facts.permits.set(this.facts.permits.get() + 1));
        }
        if this.close_early {
            conn.close_after();
        }
        this.finish(&mut conn, driver);
        receive::Outcome::Continue
    }
}

impl<'a: 'd, 'd, ID: route::Identity> send::Drive<'d, ID> for App<'a> {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().finish(&mut conn, driver);
    }
}

impl<'a: 'd, 'd> session::Dispatch<'d, (), Infallible> for App<'a> {
    type Ready = Infallible;
    type Head = Infallible;

    fn abort_body(
        self: Pin<&mut Self>,
        _: &mut session::Conn<'d, (), Infallible>,
        error: Failure,
    ) -> fixed::Body<'d> {
        panic!("unexpected rejection: {error:?}")
    }

    fn resume_head(
        self: Pin<&mut Self>,
        pending: Self::Head,
        _: session::Permit,
        _: Input<'_>,
        _: &mut [u8],
        _: &mut session::Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        match pending {}
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(permit, Input::Buffered(source.view()), write, conn)
    }

    fn try_consume(
        self: Pin<&mut Self>,
        permit: session::Permit,
        input: Input<'_>,
        output: &mut [u8],
        _: &mut session::Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        match input.as_slice().first() {
            Some(b'A') if input.as_slice().len() < 2 => session::Outcome::NeedMore {
                permit,
                state: session::NeedMore::Head,
            }
            .into(),
            Some(b'A') => {
                self.facts.routed.set(self.facts.routed.get() + 1);
                output[..PREFIX.len()].copy_from_slice(PREFIX);
                session::Outcome::Complete {
                    permit,
                    consumption: session::Consumption::Buffered(session::Buffered::new(1)),
                    response: output::Complete::Inline {
                        written: PREFIX.len(),
                    },
                    connection: head::Connection::KeepAlive,
                }
                .into()
            }
            Some(b'C') => {
                self.facts.routed.set(self.facts.routed.get() + 1);
                let body = match storage::Shared::try_copy(b"bound-owner") {
                    Ok(body) => body,
                    Err(error) => {
                        let error = Failure::Input(InputError::Storage(error));
                        panic!("unexpected rejection: {error:?}");
                    }
                };
                session::Outcome::Complete {
                    permit,
                    consumption: session::Consumption::Buffered(session::Buffered::new(1)),
                    response: output::Complete::Shared {
                        hdr_written: 0,
                        body,
                    },
                    connection: head::Connection::KeepAlive,
                }
                .into()
            }
            Some(b'B') => {
                self.facts.rejected.set(self.facts.rejected.get() + 1);
                session::Outcome::Close(fixed::Body::new(self.reason)).into()
            }
            _ => panic!("unexpected input: {:?}", input.as_slice()),
        }
    }

    fn body_data(
        self: Pin<&mut Self>,
        wait: Infallible,
        _: owned::Segments<'d>,
        _: &mut (),
    ) -> Result<Infallible, fixed::Body<'d>> {
        match wait {}
    }

    fn has_body_work(_: &Infallible) -> bool {
        false
    }

    fn resume_pending(
        self: Pin<&mut Self>,
        wait: Infallible,
        _: &mut (),
    ) -> Result<Infallible, fixed::Body<'d>> {
        Ok(wait)
    }

    fn finish_body(
        self: Pin<&mut Self>,
        wait: Infallible,
        _: session::Complete,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match wait {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        _: session::Permit,
        _: &mut [u8],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match ready {}
    }
}

impl<'a: 'd, 'd> Routes<'d, ()> for App<'a> {
    type Pending = Infallible;
    type TransportTag = ();
    type Egress = bound::AtMost<0>;

    fn response_tag(self: Pin<&mut Self>, _: &mut (), charge: hold::Charge) {
        let previous = self.facts.failure.replace(Some(charge));
        assert!(!charge.is_empty());
        assert!(previous.is_none());
        self.facts.checks[4].set(self.facts.checks[4].get() + 1);
    }

    fn accept(self: Pin<&mut Self>, _: &mut (), _: ()) {
        self.facts.released.set(self.facts.released.get() + 1);
    }

    fn cancel(self: Pin<&mut Self>, _: &mut (), _: ()) {
        assert!(self.facts.failure.get().is_some());
        self.facts.checks[5].set(self.facts.checks[5].get() + 1);
        self.facts.rejected.set(self.facts.rejected.get() + 1);
    }
}

impl settings::Resource for App<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl factory::ReceiveSpec for App<'_> {
    type Input = receive::Coupled;
}

impl factory::Direct<route::KeyTag<0>, Bundle<tcp::Tcp, Identity, Throughput>> for App<'_> {
    type Application<'d>
        = Self
    where
        Self: 'd;
    type Error = Infallible;
    type Wire<'d>
        = domain::Local<'d, Identity, route::KeyTag<0>>
    where
        Self: 'd;

    fn build<'d>(self) -> Result<(Self, Self::Wire<'d>), Self::Error>
    where
        Self: 'd,
    {
        Ok((self, domain::Local::new(Identity)))
    }
}

pub struct Retirement;

impl<'a: 'd, 'd, ID: route::Identity> retire::Policy<'d, App<'a>, ID> for Retirement {
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut App<'a>>,
        conn: conn::Retired<
            '_,
            '_,
            'd,
            ID,
            Identity,
            <App<'a> as handler::Application<'d, ID>>::Conn,
            <App<'a> as handler::Application<'d, ID>>::Egress,
        >,
    ) {
        match app.facts.failure.get() {
            None => assert!(conn.failure().is_none()),
            Some(expected) => {
                let Some(conn::Failure::Write(conn::write::body::Failure::Bound {
                    charge,
                    maximum,
                })) = conn.failure()
                else {
                    panic!("exact owner bound failure must reach final retirement");
                };
                assert_eq!(charge.bytes().get(), expected.logical_bytes);
                assert_eq!(charge.resident().get(), expected.resident_bytes);
                assert_eq!(*maximum, 0);
                assert_eq!(app.facts.rejected.get(), 1);
            }
        }
        assert_eq!(conn.response_len(), 0);
        app.facts.checks[6].set(app.facts.checks[6].get() + 1);
        app.facts.released.set(app.facts.released.get() + 1);
    }
}
