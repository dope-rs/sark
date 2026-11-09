use sark_http::h1::{head, head::request::Parts};

#[test]
fn span_ownership() {
    let request = b"GET /chat?q=1 HTTP/1.1\r\nHost: example.test\r\nX-A:  value \t\r\n\r\nbody";
    let mut fields = Vec::new();
    let parsed = (Parts::parse(request, head::Limit::DEFAULT.with_bytes(1024), |field| {
        fields.push(field);
        Ok::<(), ()>(())
    }))
    .expect("parser operation")
    .expect("complete input");
    assert_eq!(
        parsed.wire_len().get() as usize,
        request.len() - b"body".len()
    );
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), b"Host");
    assert_eq!(
        fields[0].value_span().get(request),
        Some(b"example.test".as_slice())
    );
    assert_eq!(fields[1].name(), b"X-A");
    assert_eq!(fields[1].value(), b"value");
}
