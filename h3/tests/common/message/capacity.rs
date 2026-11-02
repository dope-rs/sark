use sark_h3::{
    conn::options::{Config, Decode, Egress, Ingress, Qpack, Streams},
    frame::{Payload, TYPE_DATA},
};
use sark_http::field::{literal, metadata::section::rules::Push};

use crate::common::{
    conn::build::{Build, Fields as _},
    field::entry,
};

pub struct ZstPush;

impl Push for ZstPush {
    const SCHEMES: Option<&[&[u8]]> = None;
    const AUTHORITIES: Option<&[&[u8]]> = None;
}

pub trait Capacity: Default {
    fn single() -> Self;
    fn qpack() -> Self;
    fn edited(edit: impl FnOnce(&mut Self)) -> Self {
        let mut config = Self::default();
        edit(&mut config);
        config
    }
}
impl Capacity for Config {
    fn qpack() -> Self {
        Self {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Qpack::default()
            },
            ..Self::default()
        }
    }
    fn single() -> Self {
        Self {
            streams: Streams { capacity: 1 },
            ingress: Ingress {
                event_capacity: 1,
                ..Ingress::default()
            },
            egress: Egress {
                write_capacity: 2,
                ..Egress::default()
            },
            ..Self::default()
        }
    }
}

pub struct Request<'a>(pub &'a [&'a [u8]]);

impl Request<'_> {
    pub fn try_encode(self) -> Result<Vec<u8>, Build> {
        let mut wire = [
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":authority", b"example.test")),
            literal!(entry(b":path", b"/")),
        ]
        .try_headers()?;
        for part in self.0 {
            Payload::encode(TYPE_DATA, part, &mut wire).map_err(Build::Frame)?;
        }
        Ok(wire)
    }
}
