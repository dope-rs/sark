//! Protocol conformance over the real server loop: close-code handling and
//! malformed-frame rejection.

use sark_ws::{fragment, frame, inbound};

type Frame = &'static [u8];

pub const TEXT_PREFIX: Frame = b"\x01\x01\xe2";
pub const TEXT_SUFFIX: Frame = b"\x80\x12\x82\xacabcdefghijklmnop";

pub const TERMINAL: [(Frame, inbound::Fault); 4] = [
    (
        b"\xc1\0",
        inbound::Fault::Frame(frame::Error::Reserved { bits: 0x40 }),
    ),
    (
        b"\x81\x80\0\0\0\0",
        inbound::Fault::Mask {
            expected: inbound::Mask::Clear,
            actual: inbound::Mask::Masked,
        },
    ),
    (b"\x8b\0", inbound::Fault::Opcode { actual: 11 }),
    (
        b"\x80\0",
        inbound::Fault::Fragment(fragment::Error::UnexpectedContinuation),
    ),
];

pub const SPLIT_TEXT: [(Frame, Frame, Result<&str, fragment::Utf8>); 4] = [
    (b"\x01\x02\xe2\x82", b"\x80\x01\xac", Ok("€")),
    (b"\x01\x01\xc2", b"\x80\x01\xa2", Ok("¢")),
    (b"\x01\x03\xf0\x90\x80", b"\x80\x01\x80", Ok("\u{10000}")),
    (
        b"\x01\x04x\xf0\x90\x80",
        b"\x80\x00",
        Err(fragment::Utf8 {
            valid: 1,
            error: None,
        }),
    ),
];

#[derive(Debug)]
pub enum Build {
    Plan(frame::PlanError),
    Bind(frame::BindError),
}

pub trait Payload {
    fn try_masked(&self, opcode: u8, fin: bool) -> Result<Vec<u8>, Build>;
    fn close_code(&self) -> Option<u16>;
}

impl Payload for [u8] {
    fn try_masked(&self, opcode: u8, fin: bool) -> Result<Vec<u8>, Build> {
        let mask = [0x11u8, 0x22, 0x33, 0x44];
        let plan = frame::Plan::new(opcode, fin, self.len(), true).map_err(Build::Plan)?;
        let mut wire = vec![0; plan.wire_len()];
        plan.bind(self, Some(mask))
            .map_err(Build::Bind)?
            .bind(&mut wire)
            .map_err(Build::Bind)?
            .encode();
        Ok(wire)
    }

    fn close_code(&self) -> Option<u16> {
        self.first_chunk().copied().map(u16::from_be_bytes)
    }
}

pub trait Prepare {
    fn try_prepare<'state, 'data>(
        &'state mut self,
        wire: &'data [u8],
    ) -> Result<Option<inbound::Prepared<'state, 'data, inbound::ServerPeer>>, inbound::Fault>;
}

impl Prepare for inbound::Inbound<inbound::ServerPeer> {
    fn try_prepare<'state, 'data>(
        &'state mut self,
        wire: &'data [u8],
    ) -> Result<Option<inbound::Prepared<'state, 'data, inbound::ServerPeer>>, inbound::Fault> {
        match self.decode(wire, usize::MAX)? {
            inbound::Decode::Ready(frame) => {
                let range = frame.range();
                frame.prepare(&wire[range]).map(Some)
            }
            inbound::Decode::NeedMore => Ok(None),
        }
    }
}
