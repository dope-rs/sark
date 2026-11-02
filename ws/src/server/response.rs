use std::mem;

use o3::buffer::{
    storage,
    write::{SpareExt as _, consume},
};

use crate::{frame, inbound};

enum Target<'a> {
    Inline(&'a mut [u8]),
    Owned(&'a mut storage::Owned),
}

/// The application may write only within its admitted response budget.
pub struct Response<'a> {
    target: Target<'a>,
    pub(super) written: usize,
    pub(super) user: usize,
}

impl<'a> Response<'a> {
    pub(super) fn new(write: &'a mut [u8]) -> Self {
        let user = write.len();
        Self {
            target: Target::Inline(write),
            written: 0,
            user,
        }
    }

    pub(super) fn owned(owner: &'a mut storage::Owned) -> Self {
        let user = owner.capacity() - owner.len();
        Self {
            target: Target::Owned(owner),
            written: 0,
            user,
        }
    }

    pub fn try_text(&mut self, text: &str) -> Result<(), consume::Error> {
        self.put(0x1, text.as_bytes(), self.user)
    }

    pub fn try_binary(&mut self, data: &[u8]) -> Result<(), consume::Error> {
        self.put(0x2, data, self.user)
    }

    pub(super) fn put_raw(&mut self, src: &[u8]) -> Result<(), consume::Error> {
        match &mut self.target {
            Target::Inline(write) => {
                consume::Error::check(src.len(), write.len())?;
                let (dst, tail) = mem::take(write).split_at_mut(src.len());
                dst.copy_from_slice(src);
                *write = tail;
            }
            Target::Owned(owner) => owner.try_extend(src)?,
        }
        self.written += src.len();
        self.user = self.user.saturating_sub(src.len());
        Ok(())
    }

    pub(super) fn upgrade(&mut self, accept: &[u8; 28]) -> Result<(), consume::Error> {
        const PREFIX: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: ";
        const SUFFIX: &[u8] = b"\r\n\r\n";
        consume::Error::check(PREFIX.len() + accept.len() + SUFFIX.len(), self.available())?;
        self.put_raw(PREFIX)?;
        self.put_raw(accept)?;
        self.put_raw(SUFFIX)
    }

    fn put(&mut self, opcode: u8, data: &[u8], room: usize) -> Result<(), consume::Error> {
        let frame = frame::Frame::server(opcode, data);
        let required = frame.wire_len();
        consume::Error::check(required, room)?;
        match &mut self.target {
            Target::Inline(write) => {
                // SAFETY: the admitted frame and exclusive response remain here.
                // Room is either available capacity or the user budget kept
                // within that capacity; no target mutation follows admission.
                let (dst, tail) = unsafe { mem::take(write).split_at_mut_unchecked(required) };
                // SAFETY: this split retains exactly the admitted frame's wire length.
                unsafe { frame::raw::write(frame, dst) };
                *write = tail;
            }
            Target::Owned(owner) => {
                let mut spare = owner.spare();
                let mut writer = spare.bounded();
                frame.write_into(&mut writer)?;
                writer.commit();
                spare.commit();
            }
        }
        self.written += required;
        self.user = self.user.saturating_sub(required);
        Ok(())
    }

    fn available(&self) -> usize {
        match &self.target {
            Target::Inline(write) => write.len(),
            Target::Owned(owner) => owner.capacity() - owner.len(),
        }
    }

    pub(super) fn reserve(&mut self) {
        self.user = self
            .available()
            .saturating_sub(frame::MAX_SERVER_CONTROL_WIRE);
    }

    pub(super) fn reply<'reply, 'payload>(
        &mut self,
        reply: inbound::Request<'reply, 'payload>,
    ) -> Result<inbound::Receipt<'reply, 'payload>, consume::Error> {
        self.put(reply.opcode(), reply.payload(), self.available())?;
        Ok(reply.admit())
    }

    pub(super) fn close_code(&mut self, code: u16) -> Result<(), consume::Error> {
        self.put(0x8, &code.to_be_bytes(), self.available())
    }
}
