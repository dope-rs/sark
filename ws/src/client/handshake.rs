pub(super) fn header_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
                    | b'0'..=b'9'
                    | b'A'..=b'Z'
                    | b'a'..=b'z'
            )
        })
}

pub(super) fn header_value(value: &str) -> bool {
    !value.bytes().any(|byte| matches!(byte, b'\r' | b'\n'))
}

pub(super) fn request(
    host: &str,
    path: &str,
    user_agent: &str,
    key: &[u8; 24],
    headers: &[(String, String)],
) -> Vec<u8> {
    let extra_headers: usize = headers
        .iter()
        .map(|(name, value)| name.len() + value.len() + 4)
        .sum();
    let mut request = Vec::with_capacity(192 + host.len() + path.len() + extra_headers);
    request.extend_from_slice(b"GET ");
    request.extend_from_slice(path.as_bytes());
    request.extend_from_slice(b" HTTP/1.1\r\nHost: ");
    request.extend_from_slice(host.as_bytes());
    request.extend_from_slice(b"\r\nUser-Agent: ");
    request.extend_from_slice(user_agent.as_bytes());
    request.extend_from_slice(b"\r\nAccept: */*");
    request.extend_from_slice(
        b"\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
    );
    request.extend_from_slice(key);
    request.extend_from_slice(b"\r\nConnection: Upgrade\r\n");
    for (name, value) in headers {
        request.extend_from_slice(name.as_bytes());
        request.extend_from_slice(b": ");
        request.extend_from_slice(value.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    request
}
