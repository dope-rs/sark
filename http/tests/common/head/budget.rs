pub const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\nX: oversized\r\n\r\n";
pub const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nX: x\r\nX: oversized\r\n\r\n";
pub const LIMIT: usize = 28;
