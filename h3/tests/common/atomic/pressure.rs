use std::ptr;

use dope_quic::wire::stream::Id;
use sark_h3::{
    conn::output::WritePayload,
    link::{self, Outcome, Transport},
};

pub struct BlockOnce {
    pub blocked: bool,
    pub payload: *const u8,
    pub mismatch: Option<*const u8>,
}

#[derive(Default)]
pub struct BlockData {
    pub accepted: Vec<Id>,
}

impl<'d> Transport<'d> for BlockOnce {
    fn send_write<'id>(&mut self, write: link::Write<'id, 'd>) -> Outcome<'id, 'd> {
        let pointer = write.payload().map_or(ptr::null(), first_segment_ptr);
        if !self.blocked {
            self.blocked = true;
            self.payload = pointer;
            return Outcome::Blocked(write);
        }
        if pointer != self.payload {
            self.mismatch.get_or_insert(pointer);
        }
        Outcome::Accepted
    }
}

impl<'d> Transport<'d> for BlockData {
    fn send_write<'id>(&mut self, write: link::Write<'id, 'd>) -> Outcome<'id, 'd> {
        if write.stream_id() == Id::ZERO {
            Outcome::Blocked(write)
        } else {
            self.accepted.push(write.stream_id());
            Outcome::Accepted
        }
    }
}

fn first_segment_ptr(payload: &WritePayload<'_>) -> *const u8 {
    match payload {
        WritePayload::Owned(bytes) => bytes.as_ptr(),
        WritePayload::Shared(bytes) => bytes.as_slice().as_ptr(),
        WritePayload::Retained(bytes) => bytes.as_slice().as_ptr(),
        WritePayload::Segmented(body) => body.front().as_slice().as_ptr(),
    }
}
