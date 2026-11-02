use std::io::{self, Write};

use dope_test::harness::peer::Stream;
use sark::client::protocol::codec::output;

pub const SEGMENT: usize = output::DATA_LIMIT;
pub const CHUNK: usize = 16 * 1024;
pub const CHUNKS: usize = 160;
pub const BODY: usize = 16 * 1024 * 1024;

pub fn chunked(stream: &mut &Stream<'_>) -> io::Result<()> {
    let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
     5\r\nhello\r\n7\r\n, world\r\n0\r\n\r\n";
    Write::write_all(stream, resp)
}

pub fn segmented(stream: &mut &Stream<'_>) -> io::Result<()> {
    let mut response = Vec::with_capacity(SEGMENT * 2 + 128);
    response.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    );
    response.extend_from_slice(format!("{SEGMENT:x}\r\n").as_bytes());
    response.resize(response.len() + SEGMENT, b'a');
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(format!("{SEGMENT:x}\r\n").as_bytes());
    response.resize(response.len() + SEGMENT, b'b');
    response.extend_from_slice(b"\r\n0\r\n\r\n");
    Write::write_all(stream, &response)
}

pub fn large(stream: &mut &Stream<'_>) -> io::Result<()> {
    let mut resp = Vec::with_capacity(CHUNK * CHUNKS + 4096);
    resp.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    );
    let payload = vec![b'x'; CHUNK];
    for _ in 0..CHUNKS {
        resp.extend_from_slice(format!("{CHUNK:x}\r\n").as_bytes());
        resp.extend_from_slice(&payload);
        resp.extend_from_slice(b"\r\n");
    }
    resp.extend_from_slice(b"0\r\n\r\n");
    Write::write_all(stream, &resp)
}

pub fn single(stream: &mut &Stream<'_>) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{BODY:x}\r\n"
    );
    Write::write_all(stream, head.as_bytes())?;
    let payload = vec![b'z'; BODY];
    Write::write_all(stream, &payload)?;
    Write::write_all(stream, b"\r\n0\r\n\r\n")
}

pub fn trailers(stream: &mut &Stream<'_>) -> io::Result<()> {
    let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
     3\r\nabc\r\n3\r\ndef\r\n0\r\nX-Checksum: 42\r\n\r\n";
    Write::write_all(stream, resp)
}

pub fn buffered_eof(stream: &mut &Stream<'_>) -> io::Result<()> {
    let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody until EOF";
    Write::write_all(stream, response)
}

pub fn stream_eof(stream: &mut &Stream<'_>) -> io::Result<()> {
    let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nstreamed body";
    Write::write_all(stream, response)
}

pub fn info(stream: &mut &Stream<'_>) -> io::Result<()> {
    let response = b"HTTP/1.1 100 Continue\r\n\r\n\
         HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    Write::write_all(stream, response)
}
