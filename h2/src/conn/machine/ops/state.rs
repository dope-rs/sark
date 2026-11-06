use core::{marker, mem};

use sark_core::{life::session, manifold::schedule};

use crate::{
    Endpoint,
    conn::{
        self, config, event,
        ingress::events,
        machine::ops,
        outputs::{self, payloads, transfer::Tail},
        registry,
    },
    flow,
    frame::numbers,
    stream,
};

pub(crate) struct State<R: crate::Role> {
    pub(in crate::conn::machine) settings: Settings,
    pub(in crate::conn::machine) flow: Flow,
    pub(in crate::conn::machine) lifecycle: Lifecycle<R>,
    pub(in crate::conn::machine) active_streams: registry::Registry<R>,
    pub(in crate::conn::machine) stream_history: registry::History<R>,
}

impl<R: crate::Role> State<R> {
    pub(in crate::conn::machine) fn new(
        local: config::Settings,
        recv: flow::Target,
        streams: registry::Registry<R>,
    ) -> Self {
        Self {
            settings: Settings::for_role::<R>(local),
            flow: Flow::new(recv),
            lifecycle: Lifecycle {
                role: marker::PhantomData,
                phase: Phase::Open,
                queued: false,
                acked: false,
                goaway_received: None,
                peer_reset_count: Reset::ZERO,
            },
            active_streams: streams,
            stream_history: registry::History::new(),
        }
    }

    pub(in crate::conn) fn poll_wake(&mut self) -> Option<schedule::Wake<stream::Key>> {
        if matches!(self.flow.take_wake(), ops::Opened::Ready) {
            return Some(schedule::Wake::Connection);
        }
        self.active_streams.poll_wake().map(schedule::Wake::Stream)
    }

    pub(crate) fn commit_write(&mut self, (credit, mut tail): (Option<flow::Reserved>, Tail<'_>)) {
        if let Some(credit) = credit {
            self.accept_write(credit);
        }
        self.flush_resets(&mut tail);
    }

    pub(in crate::conn::machine) fn finish_write<C: session::Storage, T>(
        &mut self,
        write: payloads::Write<C>,
        evidence: impl FnOnce(stream::Key, usize, C) -> (C, T),
    ) -> event::Write<T, C> {
        use payloads::Write;

        match write {
            Write::Buffered { written, accepted } => {
                if let Some(credit) = accepted {
                    self.accept_write(credit);
                }
                event::Write::Buffered(written)
            }
            Write::Split {
                prefix_len: prefix,
                credit,
                body,
            } => {
                let (body, proof) = evidence(credit.stream(), credit.amount(), body);
                self.accept_write(credit);
                event::Write::Split {
                    write: proof,
                    prefix_len: prefix,
                    body,
                }
            }
            Write::Owned {
                stream,
                prefix_len: prefix,
                body,
            } => event::Write::Owned {
                stream_id: stream,
                prefix_len: prefix,
                body,
            },
        }
    }

    pub(crate) fn accept_write(&mut self, credit: flow::Reserved) {
        let key = credit.stream();
        let Self {
            flow,
            active_streams: active,
            ..
        } = self;
        let stream = active
            .store
            .active_mut(key)
            .map(|record| &mut record.send_window);
        credit.accept(&mut flow.send_window, stream);
    }

    fn cancel_flow(&mut self, credit: flow::Reserved) {
        let key = credit.stream();
        let Self {
            flow,
            active_streams: active,
            ..
        } = self;
        let blocked = flow.send_window.available() == 0;
        let stream = active
            .store
            .active_mut(key)
            .map(|record| &mut record.send_window);
        credit.cancel(&mut flow.send_window, stream);
        if blocked && flow.send_window.available() != 0 {
            flow.mark_open();
        }
    }

    pub(in crate::conn::machine) fn cancel_write(
        &mut self,
        write: event::Staging,
    ) -> event::Retirement {
        let (charge, credit) = write.into_parts();
        let stream = credit.stream();
        self.cancel_flow(credit);
        event::Retirement {
            stream_id: stream,
            charge,
        }
    }

    pub(in crate::conn::machine) fn retire_write<C, E>(
        &mut self,
        egress: &mut outputs::Egress<C>,
        id: stream::Id,
    ) -> Option<E::Retired>
    where
        C: session::Storage,
        E: event::Evidence,
    {
        if let Some(key) = egress.headers.pending()
            && key.id() == id
        {
            egress.headers.discard(key);
        }
        let outbound = &mut egress.outbound;
        if E::OBSERVED {
            let retired = outbound
                .retained()
                .discard(id, |credit| self.cancel_flow(credit));
            retired.and_then(E::project)
        } else {
            outbound
                .retained()
                .cancel(id, |credit| self.cancel_flow(credit));
            None
        }
    }

    /// Closes an active stream after its wire reset has committed.
    pub(in crate::conn::machine) fn retire<C: session::Storage, E: event::Evidence>(
        &mut self,
        egress: &mut outputs::Egress<C>,
        key: stream::Key,
    ) -> Option<E::Retired> {
        let retired = self.retire_write::<C, E>(egress, key.id());
        self.active_streams
            .remove_key(key, self.lifecycle.keeps_events());
        retired
    }

    /// Finishes a reset already committed through the original registry entry.
    pub(in crate::conn::machine) fn finish_reset<S, B, C: session::Storage, E: event::Evidence>(
        &mut self,
        egress: &mut outputs::Egress<C>,
        key: stream::Key,
        error: numbers::ErrorCode,
        notify: Option<events::Reset<'_, S, B, C, E>>,
    ) -> Result<(), conn::Error> {
        let retired = self.retire_write::<C, E>(egress, key.id());
        let outbound = &mut egress.outbound;
        outbound.set_barrier(outputs::Barrier::Blocked);
        self.flush_resets(outbound);
        if let Some(notify) = notify {
            notify.commit(key, error, retired);
        }
        outbound.check()?;
        Ok(())
    }

    pub(in crate::conn) fn flush_resets<T: outputs::Reset + ?Sized>(&mut self, outbound: &mut T) {
        while let Some(reset) = self.active_streams.reset() {
            let stream = reset.key();
            match outbound.write(stream, reset.error()) {
                Ok(()) => {}
                Err(
                    outputs::Full::Append(_)
                    | outputs::Full::Barrier
                    | outputs::Full::Payloads { .. }
                    | outputs::Full::Bytes { .. },
                ) => {
                    outbound.set_barrier(outputs::Barrier::Blocked);
                    return;
                }
            }
            reset.commit();
        }
        outbound.set_barrier(outputs::Barrier::Open);
    }

    pub(in crate::conn::machine) fn admit_push(&self) -> Result<(), conn::Push> {
        match R::ENDPOINT {
            Endpoint::Server => return Err(conn::Push::Server),
            Endpoint::Client => {}
        }
        match self.settings.local.enable_push {
            config::Push::Enabled => Ok(()),
            config::Push::Disabled if self.lifecycle.is_acked() => Err(conn::Push::Disabled),
            config::Push::Disabled => Ok(()),
        }
    }
}

pub(in crate::conn::machine) struct Settings {
    pub(in crate::conn::machine) local: config::Settings,
    pub(in crate::conn::machine) peer: config::Settings,
}

impl Settings {
    fn for_role<R: crate::Role>(local: config::Settings) -> Self {
        let mut peer = config::Settings::DEFAULT;
        match R::ENDPOINT {
            Endpoint::Client => peer.enable_push = config::Push::Disabled,
            Endpoint::Server => {}
        }
        Self { local, peer }
    }
}

pub(in crate::conn::machine) struct Flow {
    pub(in crate::conn::machine) send_window: flow::Window,
    pub(in crate::conn::machine) recv_window: flow::Window,
    pub(in crate::conn::machine) recv_window_target: flow::Target,
    pub(in crate::conn::machine) pending_release: u32,
    opened: ops::Opened,
}

impl Flow {
    fn new(recv: flow::Target) -> Self {
        Self {
            send_window: flow::Window::new(),
            recv_window: flow::Window::for_target(recv),
            recv_window_target: recv,
            pending_release: 0,
            opened: ops::Opened::Idle,
        }
    }

    pub(in crate::conn::machine) fn mark_open(&mut self) {
        self.opened = ops::Opened::Ready;
    }

    fn take_wake(&mut self) -> ops::Opened {
        mem::replace(&mut self.opened, ops::Opened::Idle)
    }
}

pub(in crate::conn::machine) struct Lifecycle<R: crate::Role> {
    role: marker::PhantomData<R>,
    phase: Phase,
    queued: bool,
    acked: bool,
    pub(in crate::conn::machine) goaway_received: Option<numbers::ErrorCode>,
    pub(in crate::conn::machine) peer_reset_count: Reset,
}

#[repr(u8)]
enum Phase {
    Closing,
    Draining,
    Open,
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::conn::machine) struct Reset(u32);

impl Reset {
    const ZERO: Self = Self(0);

    pub(in crate::conn::machine) fn next(self) -> Result<Self, conn::Error> {
        let actual = self.0 + 1;
        if actual > conn::MAX_PEER_RESETS_PER_CONNECTION {
            return Err(conn::Error::RapidReset {
                actual,
                limit: conn::MAX_PEER_RESETS_PER_CONNECTION,
            });
        }
        Ok(Self(actual))
    }
}

impl Lifecycle<crate::ServerRole> {
    pub(in crate::conn::machine) fn queue_events(&mut self) {
        self.queued = true;
    }
}

impl<R: crate::Role> Lifecycle<R> {
    pub(in crate::conn::machine) fn keeps_events(&self) -> bool {
        matches!(R::ENDPOINT, Endpoint::Server) && self.queued
    }

    pub(in crate::conn::machine) fn is_goaway(&self) -> bool {
        !matches!(self.phase, Phase::Open)
    }

    pub(in crate::conn::machine) fn mark_goaway(&mut self, shutdown: ops::Shutdown) {
        self.phase = match shutdown {
            ops::Shutdown::Immediate => Phase::Closing,
            ops::Shutdown::Drain => Phase::Draining,
        };
    }

    pub(in crate::conn::machine) fn is_draining(&self) -> bool {
        matches!(self.phase, Phase::Draining)
    }

    pub(in crate::conn::machine) fn is_closing(&self) -> bool {
        matches!(self.phase, Phase::Closing)
    }

    pub(in crate::conn::machine) fn finish_drain(&mut self) -> ops::Drain {
        if !self.is_draining() {
            return ops::Drain::Pending;
        }
        self.phase = Phase::Closing;
        ops::Drain::Complete
    }

    pub(in crate::conn::machine) fn is_acked(&self) -> bool {
        self.acked
    }

    pub(in crate::conn::machine) fn mark_acked(&mut self) {
        self.acked = true;
    }
}

const _: () = {
    assert!(mem::size_of::<Phase>() == mem::size_of::<bool>());
    assert!(mem::size_of::<Lifecycle<crate::ServerRole>>() == 3 * mem::size_of::<u32>());
};
