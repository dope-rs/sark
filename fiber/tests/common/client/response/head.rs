pub const PIPELINED_RESPONSES: &[u8] = concat!(
    "HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n",
    "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
    "HTTP/1.1 201 Created\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
)
.as_bytes();
