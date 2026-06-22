use crate::error::{Error, Result};

pub trait SeenHeaderHandler {
    const SEEN_BIT: u16;
    const DUPLICATE_ERR: &'static str;
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Flags(u16);

impl Flags {
    pub const SEEN_CONTENT_LENGTH: u16 = 1 << 0;
    pub const SEEN_TRANSFER_ENCODING: u16 = 1 << 1;
    pub const SEEN_EXPECT: u16 = 1 << 2;
    pub const SEEN_HOST: u16 = 1 << 3;
    pub const SEEN_CONNECTION: u16 = 1 << 4;

    pub const HAS_HOST: u16 = 1 << 8;
    pub const CONNECTION_CLOSE: u16 = 1 << 9;
    pub const CONNECTION_KEEP_ALIVE: u16 = 1 << 10;

    pub fn has(self, bit: u16) -> bool {
        (self.0 & bit) != 0
    }

    pub fn set(&mut self, bit: u16) {
        self.0 |= bit;
    }

    pub fn mark_seen<H: SeenHeaderHandler>(&mut self) -> Result<()> {
        if self.has(H::SEEN_BIT) {
            return Err(Error::BadRequest(H::DUPLICATE_ERR.into()));
        }
        self.set(H::SEEN_BIT);
        Ok(())
    }

    pub fn implies_close(self, version: &[u8]) -> bool {
        if self.has(Self::CONNECTION_CLOSE) {
            return true;
        }
        if self.has(Self::CONNECTION_KEEP_ALIVE) {
            return false;
        }
        version == b"HTTP/1.0"
    }
}
