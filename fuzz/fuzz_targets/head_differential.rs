#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use sark::framer::Http;

#[derive(Arbitrary, Debug)]
enum Method {
    Get,
    Post,
    Put,
    Delete,
    Head,
    Other(Vec<u8>),
}

#[derive(Arbitrary, Debug)]
enum Path {
    Json,
    Db,
    Queries,
    User(u32),
    Random(Vec<u8>),
}

#[derive(Arbitrary, Debug)]
enum Version {
    V11,
    V10,
    Bogus(Vec<u8>),
}

#[derive(Arbitrary, Debug)]
enum Header {
    ContentLength(u64),
    TransferEncodingChunked,
    DoubleContentLength(u64, u64),
    ContentLengthAndTe(u64),
    ConnectionClose,
    ConnectionKeepAlive,
    Host(Vec<u8>),
    ObsFold,
    NullInName,
    NullInValue,
    Raw(Vec<u8>, Vec<u8>),
}

#[derive(Arbitrary, Debug)]
enum Input {
    Structured {
        method: Method,
        path: Path,
        version: Version,
        headers: Vec<Header>,
        body: Vec<u8>,
    },
    Raw(Vec<u8>),
}

fn method_bytes(m: &Method) -> Vec<u8> {
    match m {
        Method::Get => b"GET".to_vec(),
        Method::Post => b"POST".to_vec(),
        Method::Put => b"PUT".to_vec(),
        Method::Delete => b"DELETE".to_vec(),
        Method::Head => b"HEAD".to_vec(),
        Method::Other(v) => v.clone(),
    }
}

fn path_bytes(p: &Path) -> Vec<u8> {
    match p {
        Path::Json => b"/json".to_vec(),
        Path::Db => b"/db".to_vec(),
        Path::Queries => b"/queries".to_vec(),
        Path::User(id) => format!("/user/{id}").into_bytes(),
        Path::Random(v) => v.clone(),
    }
}

fn version_bytes(v: &Version) -> Vec<u8> {
    match v {
        Version::V11 => b"HTTP/1.1".to_vec(),
        Version::V10 => b"HTTP/1.0".to_vec(),
        Version::Bogus(v) => v.clone(),
    }
}

fn header_line(h: &Header) -> Vec<u8> {
    match h {
        Header::ContentLength(n) => format!("Content-Length: {n}").into_bytes(),
        Header::TransferEncodingChunked => b"Transfer-Encoding: chunked".to_vec(),
        Header::DoubleContentLength(a, b) => {
            format!("Content-Length: {a}\r\nContent-Length: {b}").into_bytes()
        }
        Header::ContentLengthAndTe(n) => {
            format!("Content-Length: {n}\r\nTransfer-Encoding: chunked").into_bytes()
        }
        Header::ConnectionClose => b"Connection: close".to_vec(),
        Header::ConnectionKeepAlive => b"Connection: keep-alive".to_vec(),
        Header::Host(v) => {
            let mut out = b"Host: ".to_vec();
            out.extend_from_slice(v);
            out
        }
        Header::ObsFold => b"X-Fold: a\r\n b".to_vec(),
        Header::NullInName => b"X-N\x00ull: v".to_vec(),
        Header::NullInValue => b"X-Val: v\x00x".to_vec(),
        Header::Raw(n, v) => {
            let mut out = n.clone();
            out.push(b':');
            out.push(b' ');
            out.extend_from_slice(v);
            out
        }
    }
}

fn build_request(
    method: &Method,
    path: &Path,
    version: &Version,
    headers: &[Header],
    body: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&method_bytes(method));
    buf.push(b' ');
    buf.extend_from_slice(&path_bytes(path));
    buf.push(b' ');
    buf.extend_from_slice(&version_bytes(version));
    buf.extend_from_slice(b"\r\n");
    for h in headers {
        buf.extend_from_slice(&header_line(h));
        buf.extend_from_slice(b"\r\n");
    }
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(body);
    buf
}

fn is_token(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes.iter().all(|&b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
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
                )
        })
}

fn httparse_request_line(buf: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u8)> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    match req.parse(buf) {
        Ok(_) => {
            let m = req.method?.as_bytes().to_vec();
            let p = req.path?.as_bytes().to_vec();
            let v = req.version?;
            Some((m, p, v))
        }
        Err(_) => None,
    }
}

fn check(buf: &[u8]) {
    let parsed = Http::parse_head(buf);
    let oracle = httparse_request_line(buf);

    let fused = Http::parse_head_fused(buf);
    match (&parsed, &fused) {
        (Some(a), Some(f)) => {
            assert_eq!(a.method, f.head.method, "fused method drift");
            assert_eq!(a.target, f.head.target, "fused target drift");
            assert_eq!(a.version, f.head.version, "fused version drift");
            assert_eq!(
                a.headers_start, f.head.headers_start,
                "fused headers_start drift"
            );
            assert_eq!(
                sark::service::Key::from_bytes(a.method) as u8,
                f.method_key as u8,
                "fused method_key drift"
            );
        }
        (None, None) => {}
        (a, f) => panic!("fused accept/reject drift: parse_head={a:?} fused={:?}", f.is_some()),
    }

    if let Some(head) = parsed {
        let version_ok = head.version == b"HTTP/1.1" || head.version == b"HTTP/1.0";
        assert!(version_ok, "accepted bad version {:?}", head.version);

        assert!(!head.method.is_empty(), "accepted empty method");
        assert!(!head.target.is_empty(), "accepted empty target");
        assert!(
            !head.target.iter().any(|&b| b <= 0x20 || b == 0x7f),
            "accepted unprintable target {:?}",
            head.target
        );

        assert!(head.headers_start >= 2, "headers_start too small");
        assert!(head.headers_start <= buf.len(), "headers_start out of range");
        assert_eq!(
            &buf[head.headers_start - 2..head.headers_start],
            b"\r\n",
            "headers_start not after CRLF"
        );

        let m_start = 0usize;
        let m_end = m_start + head.method.len();
        assert_eq!(&buf[m_start..m_end], head.method, "method slice mismatch");

        if let Some((om, op, ov)) = oracle
            && is_token(head.method)
        {
            assert_eq!(head.method, &om[..], "method disagrees with httparse");
            let expect_11 = head.version == b"HTTP/1.1";
            let oracle_11 = ov == 1;
            assert_eq!(
                expect_11, oracle_11,
                "version 1.1/1.0 disagrees with httparse: {:?} vs {ov}",
                head.version
            );
            let target_no_query: &[u8] = match head.target.iter().position(|&b| b == b'?') {
                Some(q) => &head.target[..q],
                None => head.target,
            };
            let op_no_query: &[u8] = match op.iter().position(|&b| b == b'?') {
                Some(q) => &op[..q],
                None => &op[..],
            };
            if !op.is_empty() && op[0] == b'/' {
                assert_eq!(
                    target_no_query, op_no_query,
                    "path disagrees with httparse"
                );
            }
        }
    }
}

fuzz_target!(|input: Input| {
    match input {
        Input::Structured {
            method,
            path,
            version,
            headers,
            body,
        } => {
            let buf = build_request(&method, &path, &version, &headers, &body);
            check(&buf);
        }
        Input::Raw(bytes) => {
            check(&bytes);
        }
    }
});
