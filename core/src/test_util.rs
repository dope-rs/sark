pub struct RawHttpResponse;

impl RawHttpResponse {
    pub fn build(status_line: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(status_line.as_bytes());
        out.extend_from_slice(b"\r\n");
        for (name, value) in headers {
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(body);
        out
    }
}
