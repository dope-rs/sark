use http::{HeaderValue, StatusCode};
use sark_core::http::Response;

#[test]
fn test_response_creation() {
    let resp = Response::ok();

    assert_eq!(resp.status(), StatusCode::OK);

    let mut resp = Response::new(StatusCode::CREATED);
    resp.set_body_str("Created resource");

    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(resp.body().to_vec(), b"Created resource");

    let mut resp = Response::ok();
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain"),
    );

    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");

    let resp = Response::text("hello");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
    assert_eq!(std::str::from_utf8(resp.body()).ok(), Some("hello"));

    let resp = Response::json(&serde_json::json!({"a": 1})).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
}
