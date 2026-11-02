use std::num::NonZero;

use sark::client::protocol::retry::Policy;

pub const AGGRESSIVE: Policy = Policy {
    idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
    non_idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
    reconnect_on_stale_non_idempotent: true,
};

pub const INVALID_GZIP: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\nConnection: close\r\n\r\nbad";

pub const MISSING_LOCATION: &[u8] =
    b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
