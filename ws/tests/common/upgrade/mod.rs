pub mod client;
pub mod server;

pub const OWNED_HEADERS: [&str; 7] = [
    "host",
    "user-agent",
    "accept",
    "upgrade",
    "sec-websocket-version",
    "sec-websocket-key",
    "connection",
];
