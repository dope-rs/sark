//! Transport-drain transition boundary.
use crate::{pump, session, switch, wire::head};

/// Stateless owner of transport completion transitions.
pub enum Drain {}

impl Drain {
    /// Resolves work after the final-link owner reports a complete drain.
    pub fn complete<'d, Runtime, Pending, Transitions, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
    ) -> pump::Completion<'d>
    where
        Runtime: pump::Tasks,
        Transitions: switch::Engine,
    {
        match conn.transitions.commit() {
            switch::Commit::Ordinary => Self::ordinary(conn, pump::Next::Resume).into(),
            switch::Commit::Switch => pump::Completion::Switch,
        }
    }

    /// Resolves prefix progress without exposing a staged raw owner.
    pub fn progress<'d, Runtime, Pending, Transitions, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
    ) -> pump::Next<'d>
    where
        Runtime: pump::Tasks,
        Transitions: switch::Engine,
    {
        if matches!(conn.mode(), switch::Mode::Switching(_)) {
            return pump::Next::None;
        }
        Self::ordinary(conn, pump::Next::None)
    }

    fn ordinary<'d, Runtime, Pending, Transitions, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        stream: pump::Next<'d>,
    ) -> pump::Next<'d>
    where
        Runtime: pump::Tasks,
        Transitions: switch::Engine,
    {
        match conn.async_state.activity() {
            pump::Activity::Stream => return stream,
            pump::Activity::Idle | pump::Activity::Task => {}
        }
        if let Some(reason) = conn.deferred_action.take() {
            conn.deferred = head::Connection::Close;
            return pump::Next::Close(reason);
        }
        if matches!(conn.deferred, head::Connection::Close) {
            return pump::Next::None;
        }
        if conn.can_resume() {
            pump::Next::Resume
        } else {
            pump::Next::None
        }
    }
}
