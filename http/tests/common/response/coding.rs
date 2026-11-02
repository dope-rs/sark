use sark_http::h1::head::response::Coding;

pub const CASES: [(&[u8], Coding); 4] = [
    (
        b"HTTP/1.1 200 OK\r\nContent-Encoding: GZip\r\n\r\n",
        Coding::Gzip,
    ),
    (
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Encoding: gzip\r\n\r\n",
        Coding::Unsupported,
    ),
    (
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip, br\r\n\r\n",
        Coding::Unsupported,
    ),
    (b"HTTP/1.1 200 OK\r\n\r\n", Coding::None),
];
