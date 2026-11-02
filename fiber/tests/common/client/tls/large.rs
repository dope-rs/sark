use std::num::NonZero;

pub const BODY_LEN: usize = 504_651;

pub const BODY_CHUNK: usize = 0xC000;

pub const MARKETS_PATHS: [&str; 4] = [
    "/v1/descriptions/ko/markets.xml",
    "/v1/descriptions/ja/markets.xml",
    "/v1/descriptions/zh/markets.xml",
    "/v1/descriptions/en/markets.xml",
];

pub trait Large {
    fn response(self) -> Vec<Vec<u8>>;
}

impl Large for NonZero<usize> {
    fn response(self) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        chunks.push(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\nConnection: keep-alive\r\n\r\n".to_vec(),
        );
        let body = [b'x'; BODY_CHUNK];
        let mut left = self.get();
        while left != 0 {
            let len = left.min(body.len());
            chunks.push(format!("{len:08X}\r\n").into_bytes());
            chunks.push(body[..len].to_vec());
            chunks.push(b"\r\n".to_vec());
            left -= len;
        }
        chunks.push(b"00000000\r\n\r\n".to_vec());
        chunks
    }
}
