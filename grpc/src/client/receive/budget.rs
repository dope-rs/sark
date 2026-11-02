use o3::buffer::write::consume;

use crate::{Reason, details};

pub(in crate::client) struct Charge {
    bytes: usize,
}

pub(super) enum Admit {
    Ready(Charge),
    Blocked(crate::Status),
}

pub(super) struct Budget {
    buffered_bytes: usize,
    buffered_messages: usize,
    max_bytes: usize,
    max_messages: usize,
}

impl Budget {
    pub(super) const fn new(byte_cap: usize, msg_cap: usize) -> Self {
        Self {
            buffered_bytes: 0,
            buffered_messages: 0,
            max_bytes: byte_cap,
            max_messages: msg_cap,
        }
    }

    /// Every other live charge is queued in Claims and can be polled independently
    /// of ingress. Unfinished unary responses cannot release their charge until
    /// their terminal event has passed through this same ingress turn.
    pub(super) fn can_release<'a>(&self, held: impl Iterator<Item = &'a Charge>) -> bool {
        held.size_hint()
            .1
            .is_some_and(|upper| self.buffered_messages > upper)
            || self.buffered_messages > held.take(self.buffered_messages).count()
    }

    pub(super) fn admit(&mut self, bytes: usize) -> Result<Admit, crate::Status> {
        if let Err(error) = consume::Error::check(bytes, self.max_bytes - self.buffered_bytes) {
            consume::Error::check(bytes, self.max_bytes)
                .map_err(|error| crate::Status::capacity(Reason::LiveMessage, error))?;
            return Ok(Admit::Blocked(crate::Status::capacity(
                Reason::LiveMessage,
                error,
            )));
        }
        if let Err(error) = consume::Error::check(1, self.max_messages - self.buffered_messages) {
            return Ok(Admit::Blocked(crate::Status::capacity(
                Reason::LiveMessages,
                error,
            )));
        }
        self.buffered_bytes += bytes;
        self.buffered_messages += 1;
        Ok(Admit::Ready(Charge { bytes }))
    }

    pub(super) fn release(&mut self, charge: Charge) -> Result<(), details::Budget> {
        let bytes =
            self.buffered_bytes
                .checked_sub(charge.bytes)
                .ok_or(details::Budget::Bytes {
                    buffered: self.buffered_bytes,
                    released: charge.bytes,
                })?;
        let messages = self
            .buffered_messages
            .checked_sub(1)
            .ok_or(details::Budget::Messages {
                buffered: self.buffered_messages,
                released: 1,
            })?;
        self.buffered_bytes = bytes;
        self.buffered_messages = messages;
        Ok(())
    }
}
