use http::{Method, Uri};
use o3::buffer::{Owned, Shared};

use super::*;

#[test]
fn path_param_returns_none_when_empty() {
    let req = Request::default();
    assert_eq!(req.path_param("id"), None);
}

#[test]
fn insert_path_param_adds_and_retrieves_value() {
    let mut req = Request::default();
    req.insert_path_param("id", "42");
    assert_eq!(req.path_param("id"), Some("42"));
}

#[test]
fn insert_path_param_overwrites_existing_key() {
    let mut req = Request::default();
    req.insert_path_param("id", "42");
    req.insert_path_param("id", "84");
    assert_eq!(req.path_param("id"), Some("84"));
    assert_eq!(req.path_params_len(), 1);
}

#[test]
fn path_param_uses_reverse_lookup_last_match_wins() {
    let mut req = Request::default();
    req.set_path_params(vec![
        (Box::<str>::from("id"), Shared::from_static(b"1")),
        (Box::<str>::from("name"), Shared::from_static(b"alice")),
        (Box::<str>::from("id"), Shared::from_static(b"2")),
    ]);
    assert_eq!(req.path_param("id"), Some("2"));
}

#[test]
fn set_path_params_replaces_existing_params() {
    let mut req = Request::default();
    req.insert_path_param("old", "x");
    req.set_path_params(vec![(Box::<str>::from("new"), Shared::from_static(b"y"))]);
    assert_eq!(req.path_param("old"), None);
    assert_eq!(req.path_param("new"), Some("y"));
    assert_eq!(req.path_params_len(), 1);
}

#[test]
fn set_body_str_and_body_str_round_trip() {
    let mut req = Request::default();
    req.set_body_str("hello");
    assert_eq!(req.body_str(), Some("hello"));
}

#[test]
fn body_str_returns_none_for_non_utf8_body() {
    let mut req = Request::default();
    req.set_body(Owned::from(&[0xff_u8, 0xfe_u8][..]));
    assert_eq!(req.body_str(), None);
}

#[test]
fn clear_body_empties_body() {
    let mut req = Request::default();
    req.set_body_str("hello");
    req.clear_body();
    assert!(req.body().is_empty());
}

#[test]
fn set_body_accepts_bytes_mut() {
    let mut req = Request::default();
    let body = Owned::from(&b"payload"[..]);
    req.set_body(body.clone());
    assert_eq!(req.body(), &body);
}

#[derive(serde::Serialize)]
struct Q {
    key: String,
}

#[test]
fn with_uri_clears_query_cache_and_reparses() {
    let mut req = Request::new(Method::GET, Uri::from_static("/?a=1"));
    assert_eq!(req.query("a"), Some("1".to_string()));

    req.with_uri(Uri::from_static("/?b=2"));

    assert_eq!(req.query("a"), None);
    assert_eq!(req.query("b"), Some("2".to_string()));
}

#[test]
fn with_query_builds_correct_uri_and_preserves_path() {
    let mut req = Request::new(Method::GET, Uri::from_static("/users/list?old=1"));
    req.with_query(&Q {
        key: "value".to_string(),
    })
    .unwrap();

    assert_eq!(req.uri().path(), "/users/list");
    assert_eq!(req.uri().query(), Some("key=value"));
}

#[test]
fn with_query_adds_query_to_path_without_existing_query() {
    let mut req = Request::new(Method::GET, Uri::from_static("/items"));
    req.with_query(&Q {
        key: "value".to_string(),
    })
    .unwrap();

    assert_eq!(req.uri().path(), "/items");
    assert_eq!(req.uri().query(), Some("key=value"));
}

#[test]
fn clone_is_independent_when_modified() {
    let mut original = Request::new(Method::GET, Uri::from_static("/?a=1"));
    original.set_body_str("original");
    original.insert_path_param("id", "1");

    let mut cloned = original.clone();
    cloned.set_body_str("clone");
    cloned.insert_path_param("id", "2");

    assert_eq!(original.body_str(), Some("original"));
    assert_eq!(cloned.body_str(), Some("clone"));
    assert_eq!(original.path_param("id"), Some("1"));
    assert_eq!(cloned.path_param("id"), Some("2"));
}

#[test]
fn clone_query_cache_is_independent() {
    let original = Request::new(Method::GET, Uri::from_static("/?a=1"));
    assert_eq!(original.query("a"), Some("1".to_string()));

    let mut cloned = original.clone();
    cloned.with_uri(Uri::from_static("/?b=2"));

    assert_eq!(original.query("a"), Some("1".to_string()));
    assert_eq!(original.query("b"), None);
    assert_eq!(cloned.query("a"), None);
    assert_eq!(cloned.query("b"), Some("2".to_string()));
}

#[test]
fn query_params_ref_returns_cached_map_without_clone() {
    let req = Request::new(Method::GET, Uri::from_static("/?a=1&b=2"));
    let params = req.query_params_ref().unwrap();
    assert_eq!(params.get("a").map(String::as_str), Some("1"));
    assert_eq!(params.get("b").map(String::as_str), Some("2"));
}

#[test]
fn path_param_bytes_returns_raw_bytes() {
    let mut req = Request::default();
    req.insert_path_param("id", Shared::from_static(b"42"));
    assert_eq!(req.path_param_bytes("id"), Some(&b"42"[..]));
}

#[test]
fn query_bytes_returns_raw_query_value() {
    let req = Request::new(Method::GET, Uri::from_static("/?a=1&b=hello"));
    assert_eq!(req.query_bytes(b"a"), Some(&b"1"[..]));
    assert_eq!(req.query_bytes(b"b"), Some(&b"hello"[..]));
    assert_eq!(req.query_bytes(b"missing"), None);
}

#[test]
fn debug_includes_body_len_not_body_content() {
    let mut req = Request::default();
    req.set_body_str("super-secret-body");
    let out = format!("{:?}", req);

    assert!(out.contains("body_len"));
    assert!(!out.contains("super-secret-body"));
}
