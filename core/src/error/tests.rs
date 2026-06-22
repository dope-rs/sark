use http::StatusCode;

use super::*;

#[test]
fn status_code_not_found() {
    assert_eq!(Error::NotFound.status_code(), StatusCode::NOT_FOUND);
}

#[test]
fn status_code_method_not_allowed() {
    assert_eq!(
        Error::MethodNotAllowed.status_code(),
        StatusCode::METHOD_NOT_ALLOWED
    );
}

#[test]
fn status_code_bad_request() {
    assert_eq!(
        Error::BadRequest("x".into()).status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn status_code_unauthorized() {
    assert_eq!(
        Error::Unauthorized("x".into()).status_code(),
        StatusCode::UNAUTHORIZED
    );
}

#[test]
fn status_code_forbidden() {
    assert_eq!(
        Error::Forbidden("x".into()).status_code(),
        StatusCode::FORBIDDEN
    );
}

#[test]
fn status_code_internal_server_error() {
    assert_eq!(
        Error::InternalServerError("x".into()).status_code(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn status_code_internal() {
    assert_eq!(
        Error::Internal("x".into()).status_code(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn status_code_io() {
    let err = Error::Io(io::Error::other("test"));
    assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn status_code_json() {
    let json_err: serde_json::Error = serde_json::from_str::<i32>("bad").unwrap_err();
    assert_eq!(Error::Json(json_err).status_code(), StatusCode::BAD_REQUEST);
}

#[test]
fn status_code_http_parse() {
    assert_eq!(
        Error::HttpParse(httparse::Error::TooManyHeaders).status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn to_response_not_found_plain() {
    let resp = Error::NotFound.to_response();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(resp.body_str(), Some("Not found"));
}

#[test]
fn to_response_method_not_allowed_plain() {
    let resp = Error::MethodNotAllowed.to_response();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(resp.body_str(), Some("Method not allowed"));
}

#[test]
fn to_response_bad_request_plain() {
    let resp = Error::BadRequest("missing field".into()).to_response();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(resp.body_str(), Some("Bad request: missing field"));
}

#[test]
fn to_response_unauthorized_plain() {
    let resp = Error::Unauthorized("no token".into()).to_response();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(resp.body_str(), Some("Unauthorized: no token"));
}

#[test]
fn to_response_forbidden_plain() {
    let resp = Error::Forbidden("denied".into()).to_response();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(resp.body_str(), Some("Forbidden: denied"));
}

#[test]
fn to_response_internal_server_error_plain() {
    let resp = Error::InternalServerError("db down".into()).to_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.body_str(), Some("Internal server error: db down"));
}

#[test]
fn to_response_internal_plain() {
    let resp = Error::Internal("oops".into()).to_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.body_str(), Some("Internal error: oops"));
}

#[test]
fn to_response_plain_has_text_content_type() {
    let resp = Error::NotFound.to_response();
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
}

#[test]
fn to_response_json_not_found() {
    let resp = Error::NotFound.to_response_with_format(ErrorBodyFormat::Json);
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_str(resp.body_str().unwrap()).unwrap();
    assert_eq!(body["status"], 404);
    assert_eq!(body["error"], "Not found");
}

#[test]
fn to_response_json_bad_request() {
    let resp = Error::BadRequest("invalid".into()).to_response_with_format(ErrorBodyFormat::Json);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(resp.body_str().unwrap()).unwrap();
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"], "Bad request: invalid");
}

#[test]
fn to_response_json_internal() {
    let resp = Error::Internal("fail".into()).to_response_with_format(ErrorBodyFormat::Json);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: serde_json::Value = serde_json::from_str(resp.body_str().unwrap()).unwrap();
    assert_eq!(body["status"], 500);
    assert_eq!(body["error"], "Internal error: fail");
}

#[test]
fn to_response_json_has_json_content_type() {
    let resp = Error::NotFound.to_response_with_format(ErrorBodyFormat::Json);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
}

#[test]
fn from_error_for_response() {
    let err = Error::NotFound;
    let resp: Response = err.into();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(resp.body_str(), Some("Not found"));
}

#[test]
fn from_error_ref_for_response() {
    let err = Error::BadRequest("oops".into());
    let resp: Response = (&err).into();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(resp.body_str(), Some("Bad request: oops"));
}

#[test]
fn from_error_matches_to_response() {
    let err = Error::Forbidden("nope".into());
    let resp_direct = err.to_response();
    let err2 = Error::Forbidden("nope".into());
    let resp_from: Response = err2.into();
    assert_eq!(resp_direct.status(), resp_from.status());
    assert_eq!(resp_direct.body_str(), resp_from.body_str());
}

#[test]
fn error_body_format_default_is_plain_text() {
    assert_eq!(ErrorBodyFormat::default(), ErrorBodyFormat::PlainText);
}

#[test]
fn from_io_error() {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
    let err: Error = io_err.into();
    assert!(matches!(err, Error::Io(_)));
    assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn from_json_error() {
    let json_err = serde_json::from_str::<i32>("bad").unwrap_err();
    let err: Error = json_err.into();
    assert!(matches!(err, Error::Json(_)));
    assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
}

#[test]
fn from_httparse_error() {
    let err: Error = httparse::Error::TooManyHeaders.into();
    assert!(matches!(err, Error::HttpParse(_)));
    assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
}
