use http::StatusCode;
use o3::buffer::Shared;
use sark_core::http::{
    Body, Field, FixedResponse, HeaderItem, HeaderNameToken, HeaderTemplate, Headers, ResponseSink,
    Shape,
};

const STATIC_FIELDS: &[Field<'static>] = &[
    Field::new(b"content-type", b"text/plain"),
    Field::new(b"x-static", b"yes"),
];
const STATIC_HEADERS: HeaderTemplate = HeaderTemplate::new(
    b"content-type: text/plain\r\nx-static: yes\r\n",
    STATIC_FIELDS,
);

#[derive(Default)]
struct Capture {
    status: Option<StatusCode>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
}

impl ResponseSink for Capture {
    fn emit<'a, 'body, I>(&mut self, status: StatusCode, headers: I, body: Body<'body>)
    where
        I: Iterator<Item = Field<'a>>,
    {
        self.status = Some(status);
        self.fields
            .extend(headers.map(|field| (field.name.to_vec(), field.value.to_vec())));
        self.body.extend_from_slice(body.as_bytes());
    }
}

#[test]
fn structured_and_dynamic_headers_flow_without_wire_reparse() {
    let dynamic = Headers::from_items([HeaderItem::from_value(
        HeaderNameToken::new("x-dynamic"),
        "value",
    )]);
    let response = FixedResponse::structured(
        StatusCode::CREATED,
        &STATIC_HEADERS,
        dynamic,
        Shared::from_static(b"body"),
    );
    let mut capture = Capture::default();

    assert!(response.emit(&mut capture));
    assert_eq!(capture.status, Some(StatusCode::CREATED));
    assert_eq!(
        capture.fields,
        [
            (b"content-type".to_vec(), b"text/plain".to_vec()),
            (b"x-static".to_vec(), b"yes".to_vec()),
            (b"x-dynamic".to_vec(), b"value".to_vec()),
        ]
    );
    assert_eq!(capture.body, b"body");
}

#[test]
fn legacy_wire_headers_remain_sink_compatible() {
    let response: FixedResponse<'static, 0> = FixedResponse::direct(
        StatusCode::OK,
        b"x-first: one\r\nx-second:  two\r\n",
        Headers::new(),
        Shared::from_static(b"legacy"),
    );
    let mut capture = Capture::default();

    assert!(response.emit(&mut capture));
    assert_eq!(
        capture.fields,
        [
            (b"x-first".to_vec(), b"one".to_vec()),
            (b"x-second".to_vec(), b"two".to_vec()),
        ]
    );
    assert_eq!(capture.body, b"legacy");
}
