use o3::alloc;
use sark_h2::{
    frame::{
        Ack, controls,
        numbers::{ErrorCode, Increment},
        payload,
    },
    stream::Id,
};
use sark_http::field::metadata::section::rules::Push;

use crate::common::protocol::rules::Build;

pub struct Authority;

impl Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = None;
    const AUTHORITIES: Option<&[&[u8]]> = None;
}

pub struct Controls {
    ack: Ack,
}

impl Controls {
    pub const fn new(ack: Ack) -> Self {
        Self { ack }
    }

    pub fn try_settings(self, params: &[(u16, u32)]) -> Result<Vec<u8>, Build> {
        let mut payload = Vec::new();
        for (id, val) in params {
            payload.extend_from_slice(&id.to_be_bytes());
            payload.extend_from_slice(&val.to_be_bytes());
        }
        let mut out = Vec::new();
        let frame = controls::Settings::new(self.ack, &payload).ok_or(Build::Settings {
            ack: self.ack,
            bytes: payload.len(),
        })?;
        frame.encode(&mut out).map_err(Build::Encode)?;
        Ok(out)
    }

    pub fn try_ping(self, opaque: [u8; 8]) -> Result<Vec<u8>, alloc::Error> {
        let mut out = Vec::new();
        payload::Ping {
            ack: self.ack,
            opaque,
        }
        .encode(&mut out)?;
        Ok(out)
    }
}

pub struct Control {
    stream: u32,
}

impl Control {
    pub const fn new(stream: u32) -> Self {
        Self { stream }
    }

    pub fn try_goaway(self, error: ErrorCode, debug: &[u8]) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        let stream = Id::new(self.stream).ok_or(Build::Stream(self.stream))?;
        payload::GoAway::new(stream, error, debug)
            .ok_or(Build::GoAway {
                stream,
                bytes: debug.len(),
            })?
            .encode(&mut out)
            .map_err(Build::Encode)?;
        Ok(out)
    }

    pub fn try_window(self, delta: u32) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        payload::Payload::WindowUpdate(payload::WindowUpdate {
            stream_id: Id::new(self.stream).ok_or(Build::Stream(self.stream))?,
            increment: Increment::new(delta).ok_or(Build::Increment(delta))?,
        })
        .encode(&mut out)
        .map_err(Build::Encode)?;
        Ok(out)
    }
    pub fn try_reset(self, error: ErrorCode) -> Result<Vec<u8>, Build> {
        let mut out = Vec::new();
        payload::Payload::RstStream(
            controls::RstStream::new(
                Id::new(self.stream).ok_or(Build::Stream(self.stream))?,
                error,
            )
            .ok_or(Build::Stream(self.stream))?,
        )
        .encode(&mut out)
        .map_err(Build::Encode)?;
        Ok(out)
    }
}
