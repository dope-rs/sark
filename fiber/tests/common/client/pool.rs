use std::time::Duration;

pub const SATURATION_INFLIGHT: usize = 2;

pub const SATURATION_WAVES: usize = 2;

pub const SATURATION_MAX_CONNECTIONS: usize = 5;

pub const SATURATION_PATH: &str = "/lane-saturation";

pub const SATURATION_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

pub const SATURATION_LAST_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok";

pub const BURST: usize = 4;
pub const WINDOW: Duration = Duration::from_millis(1_500);
