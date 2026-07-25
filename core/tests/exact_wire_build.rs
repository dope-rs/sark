use http::StatusCode;
use o3::buffer::Shared;
use sark_core::http::{ResponsePlan, TextBody, TextItem, codec::Wire};

#[test]
fn static_headers_remain_static_when_materialized() {
    static HEADERS: &[u8] = b"x-static: yes\r\n";
    let headers = ResponsePlan::from_static(StatusCode::OK, HEADERS).wire_headers();

    assert_eq!(headers.as_ptr(), HEADERS.as_ptr());
    assert_eq!(headers.as_ref(), HEADERS);
}

#[test]
fn dynamic_headers_fill_the_exact_wire_buffer() {
    let mut plan = ResponsePlan::from_static(StatusCode::OK, b"x-static: yes\r\n");
    plan.push_static("x-dynamic", "also");

    assert_eq!(
        plan.wire_headers().as_ref(),
        b"x-static: yes\r\nx-dynamic: also\r\n"
    );
}

#[test]
fn a_single_shared_text_item_keeps_its_allocation() {
    let shared = Shared::copy_from_slice(b"already shared");
    let allocation = shared.as_ptr();
    let body = TextBody::from_items([TextItem::Shared(shared)]).into_bytes();

    assert_eq!(body.as_ptr(), allocation);
    assert_eq!(body.as_ref(), b"already shared");
}

#[test]
fn multiple_text_items_use_exact_concatenation() {
    let body = TextBody::from_items([
        TextItem::Static(b"hello "),
        TextItem::Shared(Shared::copy_from_slice(b"world")),
    ])
    .into_bytes();

    assert_eq!(body.as_ref(), b"hello world");
}

#[test]
fn chunk_framing_fills_the_exact_wire_buffer() {
    let framed = Wire::chunk_frame(Shared::from_static(b"hello"));

    assert_eq!(framed.as_ref(), b"5\r\nhello\r\n");
}
