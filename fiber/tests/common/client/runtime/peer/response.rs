/// Raw response fixtures retain intentionally malformed status and field bytes.
pub trait Response {
    fn response(&self, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8>;

    fn location(&self) -> Vec<u8>;
}

impl Response for str {
    fn response(&self, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        let capacity = headers.iter().fold(
            self.len().saturating_add(body.len()).saturating_add(4),
            |total, (name, value)| {
                total
                    .saturating_add(name.len())
                    .saturating_add(value.len())
                    .saturating_add(4)
            },
        );
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(self.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        for (name, value) in headers {
            bytes.extend_from_slice(name.as_bytes());
            bytes.extend_from_slice(b": ");
            bytes.extend_from_slice(value.as_bytes());
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(b"\r\n");
        bytes.extend_from_slice(body);
        bytes
    }

    fn location(&self) -> Vec<u8> {
        format!("HTTP/1.1 302 Found\r\nLocation: {self}\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").into_bytes()
    }
}
