use http::StatusCode;
use o3::buffer::{Bytes, Retained, Shared};
use sark::json::JsonEncode;
use sark::service::RouteSpec;

#[sark_gen::json(encode)]
struct Item {
    id: u64,
    name: String,
    score: f64,
}

#[sark_gen::json(encode)]
struct Output {
    ok: bool,
    delta: i64,
    #[field(seq, nested)]
    items: Vec<Item>,
}

#[sark_gen::json(encode)]
struct OwnedText {
    shared: Shared,
    retained: Bytes<Retained>,
}

#[sark_gen::request]
struct OwnedRequest {}

#[sark_gen::response(json)]
struct OwnedReply {
    status: StatusCode,
    body: OwnedText,
}

#[sark_gen::handler]
async fn owned_json(_request: OwnedRequest, _state: &()) -> OwnedReply {
    let shared = Shared::from_static(b"shared");
    OwnedReply {
        status: StatusCode::OK,
        body: OwnedText {
            shared: shared.clone(),
            retained: Bytes::<Retained>::from(shared),
        },
    }
}

#[test]
fn encode_only_supports_owned_output_shapes() {
    let output = Output {
        ok: true,
        delta: -7,
        items: vec![Item {
            id: 3,
            name: String::from("a\"b"),
            score: 1.25,
        }],
    };
    let encoded = output.encode_json();
    assert_eq!(
        encoded.as_slice(),
        br#"{"ok":true,"delta":-7,"items":[{"id":3,"name":"a\"b","score":1.25}]}"#
    );
    assert_eq!(encoded.len(), output.json_len());
}

#[test]
fn encode_only_maps_nonfinite_numbers_to_null() {
    let output = Item {
        id: 0,
        name: String::new(),
        score: f64::INFINITY,
    };
    let encoded = output.encode_json();
    assert_eq!(encoded.as_slice(), br#"{"id":0,"name":"","score":null}"#);
    assert_eq!(encoded.len(), output.json_len());
}

#[test]
fn shared_and_retained_bytes_form_owned_async_json() {
    fn require_route<T: RouteSpec>() {}
    require_route::<owned_json>();

    let shared = Shared::from_static(b"a\"b");
    let output = OwnedText {
        shared: shared.clone(),
        retained: Bytes::<Retained>::from(shared),
    };
    assert_eq!(
        output.encode_json().as_slice(),
        br#"{"shared":"a\"b","retained":"a\"b"}"#
    );
}
