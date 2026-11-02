pub const UPLOAD: &[u8] = &[0xa5; 1024 * 1024];
pub const LARGE_BODY_LEN: usize = 512 * 1024;

pub struct Request<'a> {
    path: &'a str,
}

impl<'a> Request<'a> {
    pub const fn new(path: &'a str) -> Self {
        Self { path }
    }

    pub fn get(self) -> Vec<u8> {
        let path = self.path;
        format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").into_bytes()
    }

    pub fn close_head(self, length: usize) -> Vec<u8> {
        let path = self.path;
        format!(
            "POST {path} HTTP/1.1\r\nHost: local\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes()
    }
    pub fn chunked(self, chunks: &[&[u8]], trailer: bool) -> Vec<u8> {
        let path = self.path;
        let mut request =
            format!("POST {path} HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n")
                .into_bytes();
        for chunk in chunks {
            request.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            request.extend_from_slice(chunk);
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(if trailer {
            b"0\r\nx-proof: yes\r\n\r\n"
        } else {
            b"0\r\n\r\n"
        });
        request
    }

    pub fn fixed(self, body: &[u8]) -> Vec<u8> {
        let path = self.path;
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(body);
        request
    }
}
