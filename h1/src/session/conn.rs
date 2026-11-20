//! HTTP/1 connection root.
use core::{convert::Infallible, mem};

use o3::buffer::storage;
use sark_http::{h1::switch::Kind, payload::fixed};

use crate::{
    pump,
    server::deadline,
    session::Ingress,
    switch,
    wire::{head, recv},
};

/// Protocol root with inline runtime and transition policy.
///
pub struct Conn<
    'd,
    Runtime = (),
    Pending = (),
    Transitions = switch::Disabled,
    Ready = Infallible,
    Heads = Infallible,
> {
    pub ingress: Ingress<'d, Pending, Ready, Heads>,
    #[doc(hidden)]
    pub async_state: Runtime,
    pub(crate) deferred_action: Option<fixed::Body<'d>>,
    pub(crate) deferred: head::Connection,
    pub(crate) transitions: Transitions,
}

impl<'d, Runtime: Default, Pending, Transitions: Default, Ready, Heads> Default
    for Conn<'d, Runtime, Pending, Transitions, Ready, Heads>
{
    fn default() -> Self {
        Self::prefixed(sark_http::preface::Prefix::default())
    }
}

impl<'d, Runtime: Default, Pending, Transitions: Default, Ready, Heads>
    Conn<'d, Runtime, Pending, Transitions, Ready, Heads>
{
    pub fn prefixed(prefix: sark_http::preface::Prefix) -> Self {
        let protocol = if prefix.as_ref().is_empty() {
            super::ingress::Protocol::Head
        } else {
            super::ingress::Protocol::Prefix(prefix)
        };
        Self {
            ingress: Ingress::new(protocol),
            async_state: Runtime::default(),
            deferred_action: None,
            deferred: head::Connection::KeepAlive,
            transitions: Transitions::default(),
        }
    }
}

impl<'d, Runtime, Pending, Transitions: switch::Engine, Ready, Heads>
    Conn<'d, Runtime, Pending, Transitions, Ready, Heads>
{
    /// Handles states which cannot acquire a transport write buffer.
    pub fn preflight(&mut self, input: &[u8]) -> pump::Preflight
    where
        Runtime: pump::Tasks,
    {
        match self.mode() {
            switch::Mode::Switching(kind) => {
                return pump::Preflight::from(
                    self.ingress
                        .recv
                        .backlog(pump::Operation::Switch(kind), input),
                );
            }
            switch::Mode::Raw(kind) => {
                return pump::Preflight::Rejected(pump::Reject::Raw(kind));
            }
            switch::Mode::Http => {}
        }
        match self.async_state.activity() {
            pump::Activity::Idle => {}
            pump::Activity::Task | pump::Activity::Stream => {
                return pump::Preflight::from(
                    self.ingress.recv.backlog(pump::Operation::Task, input),
                );
            }
        }
        if matches!(self.deferred, head::Connection::Close) || self.deferred_action.is_some() {
            self.ingress.discard_remaining -= input.len().min(self.ingress.discard_remaining);
            return pump::Preflight::Settled;
        }
        if self.ingress.discard_remaining > 0 {
            let take = input.len().min(self.ingress.discard_remaining);
            let (_, tail) = input.split_at(take);
            return match self.ingress.absorb(tail) {
                Ok(()) => {
                    self.ingress.discard_remaining -= take;
                    if !tail.is_empty() {
                        pump::Preflight::Resume
                    } else if self.ingress.discard_remaining == 0 {
                        pump::Preflight::Finished
                    } else {
                        pump::Preflight::Settled
                    }
                }
                Err(error) => pump::Preflight::Rejected(error),
            };
        }
        pump::Preflight::Drive
    }

    /// Retains ingress while transport write capacity is unavailable.
    pub fn retain(&mut self, input: &[u8]) -> pump::Retention {
        let window = self.ingress.protocol.window();
        let progress = match (input.is_empty(), window) {
            (false, recv::Window::Head) => {
                deadline::Progress::new(deadline::Head::Pending, deadline::Body::None)
            }
            (false, recv::Window::Body) => {
                deadline::Progress::new(deadline::Head::Body, deadline::Body::Progress)
            }
            (true, recv::Window::Head) if self.ingress.recv.is_ready() => {
                deadline::Progress::new(deadline::Head::Pending, deadline::Body::None)
            }
            (true, recv::Window::Head | recv::Window::Body) => deadline::Progress::default(),
        };
        let (state, progress) = match self.ingress.absorb(input) {
            Ok(()) => (Ok(()), progress),
            Err(error) => (
                Err(error),
                deadline::Progress::new(progress.head(), deadline::Body::None),
            ),
        };
        pump::Retention { state, progress }
    }

    pub const fn deferred(&self) -> head::Connection {
        self.deferred
    }

    /// Restores a close response for the next output-capable turn.
    #[doc(hidden)]
    pub fn defer(&mut self, reason: fixed::Body<'d>) {
        self.deferred_action = Some(reason);
    }

    /// Borrows the exact action an output-capable turn must settle.
    #[doc(hidden)]
    pub const fn action(&self) -> Option<&fixed::Body<'d>> {
        self.deferred_action.as_ref()
    }

    pub fn mode(&self) -> switch::Mode {
        self.transitions.mode()
    }

    /// Retains the response's settled connection intent before ingress can resume.
    #[doc(hidden)]
    pub fn settle(&mut self, intent: head::Connection) {
        self.deferred = self.deferred.join(intent);
    }

    /// Framed work or retained input before terminal close can resume.
    #[doc(hidden)]
    pub fn can_resume(&self) -> bool {
        self.ingress.is_ready()
            || (matches!(self.deferred, head::Connection::KeepAlive)
                && self.ingress.recv.is_ready())
    }

    /// A closing HTTP response owns no further application work.
    #[doc(hidden)]
    pub fn is_draining(&self) -> bool
    where
        Runtime: pump::Tasks,
    {
        matches!(self.mode(), switch::Mode::Http)
            && matches!(self.deferred, head::Connection::Close)
            && matches!(self.async_state.activity(), pump::Activity::Idle)
            && self.deferred_action.is_none()
    }

    /// Retires request ingress and deferred connection intents.
    #[doc(hidden)]
    pub fn retire(&mut self) {
        self.ingress.retire();
        self.deferred_action = None;
        self.deferred = head::Connection::KeepAlive;
    }
}

impl<'d, Runtime, Pending, D, Ready, Heads>
    Conn<'d, Runtime, Pending, switch::Enabled<D>, Ready, Heads>
{
    pub fn raw(&self) -> Result<&D, switch::Mode<Infallible>> {
        self.transitions.raw()
    }

    pub fn raw_mut(&mut self) -> Result<&mut D, switch::Mode<Infallible>> {
        self.transitions.raw_mut()
    }

    /// Accesses staged raw state before response drain only for ingress.
    #[doc(hidden)]
    pub fn staged_mut(&mut self) -> Result<&mut D, switch::Mode<Kind, Infallible>> {
        self.transitions.staged_mut()
    }

    /// Takes input received after the HTTP request boundary, or `None` for the
    /// HTTP and switching modes, which own no tail to hand over.
    pub fn take_input(&mut self) -> Option<storage::Shared> {
        match switch::Engine::mode(&self.transitions) {
            switch::Mode::Raw(_) => self.ingress.recv.take_ready(),
            switch::Mode::Http | switch::Mode::Switching(_) => None,
        }
    }
}

const _: () = {
    let fields = mem::size_of::<Ingress<'_, ()>>()
        + mem::size_of::<Option<fixed::Body<'_>>>()
        + mem::size_of::<head::Connection>();
    let align = mem::align_of::<Conn<'_>>();
    let min = (fields + align - 1) & !(align - 1);
    assert!(mem::size_of::<Conn<'_>>() == min);
};
