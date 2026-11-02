pub const HEAD_205: &[u8] = b"HTTP/1.1 205 Reset Content\r\n\r\n";
pub const RESPONSE_TE_10: &[u8] =
    b"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n";
pub const REQUEST_TE_10: &[u8] =
    b"POST / HTTP/1.0\r\nHost: test.invalid\r\nTransfer-Encoding: chunked\r\n\r\n";
