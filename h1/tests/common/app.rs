use std::{cell::Cell, convert::Infallible, pin::Pin};

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
            driver::{Driver, Flow},
            router::{self, Routes},
        },
        date, payload,
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
}

pub enum Event<'a> {
    Progress(&'a Flow, Option<&'a [u8]>, bool),
    Close(Option<&'a [u8]>),
    Stopped,
    Charge(hold::Charge, Option<hold::Charge>),
    Cancelled,
    Retired(Option<&'a conn::Failure>, usize),
    Rejected(&'a Failure),
    Input(&'a [u8]),
}

pub struct App<'a> {
    pub reason: &'a [u8],
    pub facts: &'a Facts,
    pub close_early: bool,
    pub clock: &'a date::Clock,
    pub observe: &'a dyn Fn(Event<'_>),
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
        let clock = self.clock;
        let (flow, _) = Driver::new(Pin::new(&mut *self), clock)
            .progress(conn, driver)
            .into_parts();
        (self.observe)(Event::Progress(
            &flow,
            conn.state().action().map(|reason| reason.as_slice()),
            conn.is_closing(),
        ));
        match flow {
            Flow::Continue => {
                self.facts.deferred.set(self.facts.deferred.get() + 1);
            }
            Flow::Closed => {
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

    fn activate<'e, 'b, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'b, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> conn::Ctx<'e, 'b, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        self.get_mut().finish(&mut conn, driver);
        conn
    }

    fn close<'e, 'b, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'b, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> conn::Ctx<'e, 'b, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        (self.observe)(Event::Close(
            conn.state().action().map(|reason| reason.as_slice()),
        ));
        self.facts.closed.hit();
        conn
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
        let clock = this.clock;
        let (flow, _, _) = Driver::new(Pin::new(&mut *this), clock)
            .run(chunk.as_ref(), &mut conn, driver)
            .into_parts();
        if matches!(flow, Flow::Closed) {
            (this.observe)(Event::Stopped);
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

impl<'a: 'd, 'd, ID: route::Identity> Routes<'d, (), ID> for App<'a> {
    type Pending = Infallible;
    type Ready = Infallible;
    type Head = Infallible;

    type TransportTag = ();
    type Egress = bound::AtMost<0>;

    fn abort_body(
        self: Pin<&mut Self>,
        _: &mut session::Conn<'d, (), Infallible>,
        error: Failure,
    ) -> fixed::Body<'d> {
        (self.observe)(Event::Rejected(&error));
        error.body()
    }

    fn response_tag(self: Pin<&mut Self>, _: conn::Id<'d, ID>, _: &mut (), charge: hold::Charge) {
        (self.observe)(Event::Charge(
            charge,
            self.facts.failure.replace(Some(charge)),
        ));
    }

    fn accept(self: Pin<&mut Self>, _: conn::Id<'d, ID>, _: &mut (), _: ()) {
        self.facts.released.set(self.facts.released.get() + 1);
    }

    fn cancel(self: Pin<&mut Self>, _: conn::Id<'d, ID>, _: &mut (), _: ()) {
        (self.observe)(Event::Cancelled);
        self.facts.rejected.set(self.facts.rejected.get() + 1);
    }

    fn resume_head(
        _: router::Dispatch<'_, 'd, Self, ID>,
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
        id: conn::Id<'d, ID>,
        date: &date::Clock,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut session::Conn<'d, (), Infallible>,
    ) -> session::RouteOutcome<'d, Infallible, Infallible, Self::Head, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(
            id,
            date,
            permit,
            Input::Buffered(source.view()),
            write,
            conn,
        )
    }

    fn try_consume(
        self: Pin<&mut Self>,
        _: conn::Id<'d, ID>,
        _: &date::Clock,
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
                        (self.observe)(Event::Rejected(&error));
                        return session::Outcome::Close(error.body()).into();
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
            _ => {
                (self.observe)(Event::Input(input.as_slice()));
                session::Outcome::Close(Failure::BadRequest.body()).into()
            }
        }
    }

    fn body_data(
        self: Pin<&mut Self>,
        _: conn::Id<'d, ID>,
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
        _: conn::Id<'d, ID>,
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
        _: &[u8; 29],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match wait {}
    }

    fn resume_body(
        self: Pin<&mut Self>,
        _: conn::Id<'d, ID>,
        ready: Self::Ready,
        _: session::Permit,
        _: &mut [u8],
        _: &[u8; 29],
        _: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        match ready {}
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
        (app.observe)(Event::Retired(conn.failure(), conn.response_len()));
        app.facts.released.set(app.facts.released.get() + 1);
    }
}
