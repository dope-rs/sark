use std::{convert, pin};

use dope::core::{
    driver::{
        route::{Identity, KeyTag},
        settings::{claim, errors},
    },
    random::Seed,
};

mod frames;
use frames::Frames;
mod opening;
mod reader;
pub(super) mod session;
mod sink;

use conn::write;
use dope::{
    core::driver::{self, schedule::clock, settings},
    manifold::{
        app::{env, receive},
        network::listener::{conn, handler, topology::factory},
        timing,
    },
    net::{
        link::{egress, slot::types::CloseGate},
        tcp, wire,
    },
};
use handler::{failures, retire, send};
use o3::buffer::{bytes, storage, write::Sink as _};
pub use reader::Reader;
pub use sink::Sink;
use wire::domain;
use write::{body, error};

use crate::server::{self, Phase, WS_MAX_ACC, output::Output};

type Ctx<'e, 'a, 'p, 's, 'w, 'd, ID> =
    conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, wire::Identity, server::ConnState, storage::Shared>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Commit {
    Committed,
    Deferred,
    Closed,
}

#[pin_project::pin_project]
pub struct App<H: Sink> {
    user: H,
    path: String,
    limit: usize,
    max_message_size: usize,
    temp: Vec<u8>,
}

impl<H: Sink> settings::Resource for App<H> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<H: Sink> factory::ReceiveSpec for App<H> {
    type Input = receive::Coupled;
}

impl<H: Sink> factory::Direct<KeyTag<0>, env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>>
    for App<H>
{
    type Application<'d>
        = Self
    where
        Self: 'd;
    type Error = convert::Infallible;

    type Wire<'d>
        = domain::Local<'d, wire::Identity, KeyTag<0>>
    where
        Self: 'd;

    fn build<'d>(self) -> Result<(Self, Self::Wire<'d>), Self::Error>
    where
        Self: 'd,
    {
        Ok((self, domain::Local::new(Default::default())))
    }
}

impl<'d, ID: Identity, H: Sink> handler::Application<'d, ID> for App<H> {
    type Conn = server::ConnState;
    type Build = convert::Infallible;
    type Wire = wire::Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = storage::Shared;
    type Input = receive::Coupled;
    type Send = send::Run;

    fn connection(self: pin::Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(server::ConnState::new(self.max_message_size))
    }

    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn timing(&self) -> handler::Timing {
        // Upgrade admission uses the connection-establishment budget, not established idle time.
        handler::Timing::timeout(<timing::Throughput as timing::Policy>::CONNECT_DEADLINE.get())
    }

    fn accept<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, '_, 'd, ID>,
    ) -> receive::Outcome {
        use conn::deadline::Error;

        let now = conn.work().clock().now();
        let error = match conn.deadline().try_ensure(now) {
            Ok(()) => return receive::Outcome::Continue,
            Err(Error::Disabled) => server::Error::OpenTimerDisabled,
            Err(Error::Sealed) => server::Error::OpenTimerSealed,
            Err(Error::Overflow(error)) => server::Error::OpenTimerRange {
                now: error.origin(),
                window: error.duration(),
            },
        };
        self.user.failed(error);
        Self::close(&mut conn);
        receive::Outcome::CloseAfter
    }

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, '_, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        if matches!(conn.state().phase, Phase::Handshake | Phase::Opening) {
            self.user.failed(server::Error::OpenTimeout);
        }
        conn.state_mut().acc.clear();
        conn.state_mut().phase = Phase::Closed;
        handler::Timeout::Discard
    }

    fn close_gate(
        self: pin::Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        let state = conn.state();
        if state.pending.is_some()
            || matches!(
                state.phase,
                Phase::OpeningRejected | Phase::MessageTooBig | Phase::Rejected(_)
            )
            || (state.eof
                && matches!(
                    state.phase,
                    Phase::Handshake | Phase::Opening | Phase::Active
                )
                && !state.acc.is_empty())
        {
            CloseGate::Deferred
        } else {
            CloseGate::Immediate
        }
    }

    fn eof<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: receive::End,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        conn.state_mut().eof = true;
        self.get_mut().pump(&mut conn, driver);
    }

    fn deadline(self: pin::Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().pump(&mut conn, driver);
    }
}

impl<'d, ID: Identity, H: Sink> handler::Borrowed<'d, ID> for App<H> {
    fn chunk<R: bytes::TryRetain>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        self.get_mut().chunk(conn, chunk, driver)
    }
}

impl<'d, ID: Identity, H: Sink> send::Drive<'d, ID> for App<H> {
    fn send(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().pump(&mut conn, driver);
    }
}

impl<H: Sink> App<H> {
    pub fn new(user: H, path: String, limit: usize, max_message_size: usize) -> Self {
        Self {
            user,
            path,
            limit: limit.min(server::WS_MAX_FRAME),
            max_message_size,
            temp: Vec::new(),
        }
    }

    fn chunk<'d, ID: Identity, R: bytes::TryRetain>(
        &mut self,
        mut conn: Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        use receive::Outcome;
        match conn.state().phase {
            Phase::Closed => return Outcome::Continue,
            Phase::OpeningRejected | Phase::MessageTooBig | Phase::Rejected(_) => {
                self.pump(&mut conn, driver);
                return Outcome::Continue;
            }
            Phase::Handshake | Phase::Opening | Phase::Active => {}
        }
        let chunk = chunk.as_ref();
        if conn.state().phase == Phase::Handshake
            && conn.state().acc.is_empty()
            && chunk.len() <= WS_MAX_ACC
        {
            match conn.write().pending() {
                egress::Pending::Present => {}
                egress::Pending::Absent => match opening::admit(&self.path, chunk) {
                    Ok(None) => {
                        let state = conn.state_mut();
                        if let Err(error) = state.acc.write_slice(chunk) {
                            self.user.failed(server::Error::Allocate(error));
                            Self::close(&mut conn);
                            return Outcome::CloseAfter;
                        }
                        return Outcome::Continue;
                    }
                    Err(error) => {
                        self.user.failed(error);
                        conn.state_mut().phase = Phase::OpeningRejected;
                        conn.state_mut().acc.clear();
                        return match self.try_reject(&mut conn, driver) {
                            Commit::Committed | Commit::Closed => Outcome::CloseAfter,
                            Commit::Deferred => Outcome::Continue,
                        };
                    }
                    Ok(Some((tail, accept, opening))) => {
                        conn.state_mut().phase = Phase::Opening;
                        match self.try_direct(&mut conn, tail, Some((opening, &accept)), driver) {
                            Commit::Committed => {
                                conn.deadline().cancel();
                                return Outcome::Continue;
                            }
                            Commit::Closed => return Outcome::CloseAfter,
                            Commit::Deferred => {}
                        }
                    }
                },
            }
        }

        if conn.state().phase == Phase::Active
            && conn.state().pending.is_none()
            && conn.state().acc.is_empty()
            && chunk.len() <= WS_MAX_ACC
        {
            match conn.write().pending() {
                egress::Pending::Present => {}
                egress::Pending::Absent => match self.try_direct(&mut conn, chunk, None, driver) {
                    Commit::Committed => return Outcome::Continue,
                    Commit::Closed => return Outcome::CloseAfter,
                    Commit::Deferred => {}
                },
            }
        }

        if conn.state().phase == Phase::Handshake
            && let Err(error) = opening::check(conn.state_mut(), chunk)
        {
            self.user.failed(error);
            Self::close(&mut conn);
            return Outcome::CloseAfter;
        }

        if !Frames::new(&self.user, self.limit).append(conn.state_mut(), chunk)
            && conn.state().phase == Phase::Closed
        {
            Self::close(&mut conn);
            return Outcome::CloseAfter;
        }

        self.pump(&mut conn, driver);
        Outcome::Continue
    }

    fn try_direct<'d, ID: Identity>(
        &mut self,
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        chunk: &[u8],
        accept: Option<(server::Opening<'_>, &[u8; 28])>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Commit {
        let limit = self.limit;
        let user = &self.user;
        let temp = &mut self.temp;
        let result = conn.write().try_prefix(driver, |state, mut prefix| {
            {
                let mut writer = prefix.writer();
                let mut split = match writer.try_split(0, writer.remaining()) {
                    Ok(split) => split,
                    Err(_) => unreachable!("prefix"),
                };
                let mut out = Output::new(split.body());
                if let Some((opening, accept)) = accept {
                    state.phase = match opening::upgrade(user, opening, &mut out.response, accept) {
                        Ok(phase) => phase,
                        Err(error) => {
                            user.failed(server::Error::Capacity(error));
                            Phase::Closed
                        }
                    };
                }
                if state.phase == Phase::Active {
                    state.phase = Frames::new(user, limit).chunk(temp, state, chunk, &mut out);
                }
                if out.finish(state, &self.user) && split.try_commit().is_err() {
                    unreachable!("prefix");
                }
            }
            let commit = prefix.commit(None, ());
            if state.phase == Phase::Closed && state.pending.is_none() {
                commit.close_after()
            } else {
                commit
            }
        });
        match Self::finish(conn, result) {
            Commit::Committed => match Self::send_pending(conn, driver) {
                Commit::Committed | Commit::Deferred => Commit::Committed,
                Commit::Closed => Commit::Closed,
            },
            next => next,
        }
    }

    fn try_reject<'d, ID: Identity>(
        &self,
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Commit {
        let result = conn.write().try_prefix(driver, |state, mut prefix| {
            let mut out = server::Response::new(prefix.writer());
            if let Err(error) = out.put_raw(server::Response::REJECTION) {
                self.user.failed(server::Error::Capacity(error));
            }
            state.phase = Phase::Closed;
            prefix.commit(None, ()).close_after()
        });
        Self::finish(conn, result)
    }

    fn pump<'d, ID: Identity>(
        &self,
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if Self::send_pending(conn, driver) != Commit::Committed {
            return;
        }
        if conn.state().phase == Phase::OpeningRejected {
            self.try_reject(conn, driver);
            return;
        }
        let opening = matches!(conn.state().phase, Phase::Handshake | Phase::Opening);
        let result = conn.write().try_prefix(driver, |state, mut prefix| {
            {
                let mut writer = prefix.writer();
                let mut split = match writer.try_split(0, writer.remaining()) {
                    Ok(split) => split,
                    Err(_) => unreachable!("prefix"),
                };
                let mut out = Output::new(split.body());
                if state.phase == Phase::Opening
                    && let Err(error) =
                        opening::drive(&self.path, state, &mut out.response, &self.user)
                {
                    self.user.failed(server::Error::Capacity(error));
                    state.phase = Phase::Closed;
                }
                Frames::new(&self.user, self.limit).pump(state, &mut out);
                if out.finish(state, &self.user) && split.try_commit().is_err() {
                    unreachable!("prefix");
                }
            }
            let commit = prefix.commit(None, ());
            if state.phase == Phase::Closed && state.pending.is_none() {
                commit.close_after()
            } else {
                commit
            }
        });
        let result = Self::finish(conn, result);
        if opening && !matches!(conn.state().phase, Phase::Handshake | Phase::Opening) {
            conn.deadline().cancel();
        }
        if result == Commit::Committed {
            Self::send_pending(conn, driver);
        }
    }

    fn finish<'d, ID: Identity>(
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        result: Result<write::Outcome<()>, error::Error<storage::Shared, (), convert::Infallible>>,
    ) -> Commit {
        let cause = match result {
            Ok(write::Outcome::Committed(())) => return Commit::Committed,
            Ok(write::Outcome::Deferred(_)) => return Commit::Deferred,
            Ok(write::Outcome::Closed) => None,
            Err(error::Error::Capacity(error)) => Some(body::Failure::Capacity(error)),
            Err(error::Error::Mismatch(error)) => {
                let (error, ()) = error.into_parts();
                Some(body::Failure::Mismatch(error))
            }
            Err(error::Error::Build(error)) => match error {},
        };
        conn.state_mut().phase = Phase::Closed;
        if let Some(cause) = cause {
            conn.write().reject(cause);
        } else {
            conn.close_after();
        }
        Commit::Closed
    }

    fn close<'d, ID: Identity>(conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>) {
        conn.state_mut().phase = Phase::Closed;
        conn.close_after();
    }

    fn send_pending<'d, ID: Identity>(
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Commit {
        if let Some(load) = conn.state_mut().pending.take() {
            match conn.write().admit(load, driver) {
                body::Admit::Committed => {}
                body::Admit::Deferred { load, .. } => {
                    conn.state_mut().pending = Some(load);
                    return Commit::Deferred;
                }
                body::Admit::Closed(_) => {
                    Self::close(conn);
                    return Commit::Closed;
                }
                body::Admit::Rejected { cause, .. } => {
                    conn.write().reject(body::Failure::Capacity(cause));
                    Self::close(conn);
                    return Commit::Closed;
                }
                body::Admit::Unavailable(rejected) => {
                    let (_, cause) = rejected.into_parts();
                    conn.write().reject(body::Failure::Reserve(cause));
                    Self::close(conn);
                    return Commit::Closed;
                }
            }
        }
        if conn.state().eof
            && conn.state().acc.is_empty()
            && !matches!(
                conn.state().phase,
                Phase::OpeningRejected | Phase::MessageTooBig | Phase::Rejected(_)
            )
        {
            conn.state_mut().phase = Phase::Closed;
        }
        if conn.state().phase == Phase::Closed {
            conn.close_after();
            Commit::Closed
        } else {
            Commit::Committed
        }
    }
}
