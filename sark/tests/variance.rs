use http::Method;
use sark::request::Ref;

#[test]
fn request_ref_is_covariant_over_req_lifetime() {
    let shorten = |x: Ref<'static, ()>| -> Ref<'_, ()> { x };
    let head = b"GET / HTTP/1.1\r\n\r\n";
    let long: Ref<'static, ()> = Ref::from_slice(Method::GET, 4..5, head, b"");
    let short = shorten(long);
    assert_eq!(short.method(), &Method::GET);
}
