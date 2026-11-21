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

pub fn excess_response() -> Vec<u8> {
    let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    // The head, each chunk and the terminator each consume one decode permit.
    for _ in 0..dope::core::driver::schedule::LIMIT - 2 {
        wire.extend_from_slice(b"1\r\nx\r\n");
    }
    wire.extend_from_slice(b"0\r\n\r\nHTTP/1.1 299 Unsolicited\r\nContent-Length: 0\r\n\r\n");
    wire
}
