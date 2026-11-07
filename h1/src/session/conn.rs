//! HTTP/1 connection root.
use core::{convert::Infallible, mem};

use o3::buffer::storage;
use sark_http::{h1::switch::Kind, payload::fixed};

use crate::{pump, session::Ingress, switch, wire::head};

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
