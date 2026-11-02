pub const BODY_POLICY: &[(&[u8], u16)] = &[
    (
        b"GET /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n",
        400,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\nx",
        400,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n0\r\n\r\n",
        400,
    ),
    (
        b"GET /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1048577\r\nExpect: 100-continue\r\n\r\n",
        413,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n",
        200,
    ),
];
