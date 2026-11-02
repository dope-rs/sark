use crate::{
    frame,
    inbound::{Fault, valid},
};

#[derive(Clone, Copy)]
#[repr(u16)]
pub(crate) enum Code {
    Protocol = 1002,
    InvalidPayload = 1007,
    TooLarge = 1009,
    Internal = 1011,
}

impl Code {
    pub(crate) const fn close(self, payload: &mut [u8; 2]) -> Close<'_> {
        *payload = (self as u16).to_be_bytes();
        Close {
            payload,
            code: Some(self as u16),
            reason: "",
        }
    }
}

/// One peer close frame's admitted payload, code, and reason.
#[derive(Clone, Copy, Debug)]
pub struct Close<'payload> {
    pub(super) payload: &'payload [u8],
    pub(super) code: Option<u16>,
    pub(super) reason: &'payload str,
}

impl<'payload> Close<'payload> {
    /// Admits one complete close payload for transmission.
    pub fn parse(payload: &'payload [u8]) -> Result<Self, Fault> {
        if payload.len() > frame::MAX_CONTROL_PAYLOAD {
            return Err(Fault::Control {
                fin: true,
                length: payload.len(),
            });
        }
        valid::decode(payload)
    }

    pub fn payload(&self) -> &'payload [u8] {
        self.payload
    }

    pub fn code(&self) -> Option<u16> {
        self.code
    }

    pub fn reason(&self) -> &'payload str {
        self.reason
    }
}
