use sark_h2::{
    ServerRole,
    conn::{
        config::{self, Config, capacity},
        machine::Connection,
    },
};
use sark_http::field::metadata::map::record::Entry;

pub const REQUEST: [Entry<'static>; 4] = [
    Entry::new(b":method", b"GET"),
    Entry::new(b":scheme", b"http"),
    Entry::new(b":path", b"/"),
    Entry::new(b"x-warming-sync", b"retained"),
];

pub const FIELDS: [Entry<'static>; 1] = [Entry::new(b"x-discard-sync", b"retained")];

pub fn try_server() -> Result<Connection<ServerRole>, config::Error> {
    Connection::with_config(Config {
        stream_capacity: 4,
        event_capacity: 2,
        payload_capacity: capacity::Payload::new(1, 1).map_err(config::Error::PayloadCapacity)?,
        ..Config::default()
    })
}
