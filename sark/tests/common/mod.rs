use std::io::{Error, ErrorKind, Read};
use std::net::TcpStream;

#[derive(Debug)]
pub(crate) struct ParsedResponse {
    pub(crate) status: u16,
    #[allow(dead_code)]
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl ParsedResponse {
    #[allow(dead_code)]
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    pub(crate) fn body_str(&self) -> &str {
        std::str::from_utf8(&self.body).expect("response body is utf8")
    }
}

pub(crate) fn parse_response_from_buffer(
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<ParsedResponse>> {
    let header_end = match buf.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(pos) => pos + 4,
        None => return Ok(None),
    };

    let head = &buf[..header_end];
    let head_str = std::str::from_utf8(head)
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid utf8 in header"))?;

    let mut lines = head_str.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status line"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status code"))?
        .parse::<u16>()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid status code"))?;

    let mut content_length = 0usize;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name_lc = name.trim().to_ascii_lowercase();
            let value_trimmed = value.trim().to_string();
            if name_lc == "content-length" {
                content_length = value_trimmed.parse().map_err(|_| {
                    Error::new(ErrorKind::InvalidData, "invalid content-length header")
                })?;
            }
            headers.push((name_lc, value_trimmed));
        }
    }

    let total = header_end + content_length;
    if buf.len() < total {
        return Ok(None);
    }

    let body = buf[header_end..total].to_vec();
    buf.drain(..total);

    Ok(Some(ParsedResponse {
        status,
        headers,
        body,
    }))
}

#[allow(dead_code)]
pub(crate) fn parse_status_head_from_buffer(buf: &mut Vec<u8>) -> std::io::Result<Option<u16>> {
    let header_end = match buf.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(pos) => pos + 4,
        None => return Ok(None),
    };

    let head_str = std::str::from_utf8(&buf[..header_end])
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid utf8 in header"))?;
    let status_line = head_str
        .split("\r\n")
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status line"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status code"))?
        .parse::<u16>()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid status code"))?;

    buf.drain(..header_end);
    Ok(Some(status))
}

pub(crate) fn read_response_std_stream(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<ParsedResponse> {
    loop {
        if let Some(parsed) = parse_response_from_buffer(buf)? {
            return Ok(parsed);
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).map_err(|err| {
            let sample_len = buf.len().min(256);
            let sample = String::from_utf8_lossy(&buf[..sample_len]);
            Error::new(
                err.kind(),
                format!(
                    "{} (partial_len={}, partial_sample={sample:?})",
                    err,
                    buf.len(),
                ),
            )
        })?;
        if n == 0 {
            let sample_len = buf.len().min(256);
            let sample = String::from_utf8_lossy(&buf[..sample_len]);
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("eof (partial_len={}, partial_sample={sample:?})", buf.len()),
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[allow(dead_code)]
pub(crate) fn read_status_head_std_stream(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<u16> {
    loop {
        if let Some(status) = parse_status_head_from_buffer(buf)? {
            return Ok(status);
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[allow(dead_code)]
pub(crate) async fn read_response_dope_stream(
    stream: &mut dope::net::TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<ParsedResponse> {
    loop {
        if let Some(parsed) = parse_response_from_buffer(buf)? {
            return Ok(parsed);
        }

        let (res, chunk) = stream.read(vec![0u8; 4096]).await;
        let n = res?;
        if n == 0 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}
