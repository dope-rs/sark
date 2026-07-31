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

#[sark_gen::json(encode)]
struct OptionalOutput {
    count: Option<u64>,
    delta: Option<i64>,
    score: Option<f64>,
    ok: Option<bool>,
    name: Option<String>,
    shared: Option<Shared>,
    retained: Option<Bytes<Retained>>,
    #[field(raw)]
    raw: Option<Shared>,
    #[field(plain)]
    plain: Option<Shared>,
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

#[test]
fn optional_fields_share_the_scalar_encoding_plan() {
    let shared = Shared::from_static(b"a\"b");
    let present = OptionalOutput {
        count: Some(7),
        delta: Some(-3),
        score: Some(1.25),
        ok: Some(true),
        name: Some(String::from("x\"y")),
        shared: Some(shared.clone()),
        retained: Some(Bytes::<Retained>::from(shared)),
        raw: Some(Shared::from_static(b"17")),
        plain: Some(Shared::from_static(b"plain")),
    };
    let encoded = present.encode_json();
    assert_eq!(
        encoded.as_slice(),
        br#"{"count":7,"delta":-3,"score":1.25,"ok":true,"name":"x\"y","shared":"a\"b","retained":"a\"b","raw":17,"plain":"plain"}"#
    );
    assert_eq!(encoded.len(), present.json_len());

    let absent = OptionalOutput {
        count: None,
        delta: None,
        score: None,
        ok: None,
        name: None,
        shared: None,
        retained: None,
        raw: None,
        plain: None,
    };
    let encoded = absent.encode_json();
    assert_eq!(
        encoded.as_slice(),
        br#"{"count":null,"delta":null,"score":null,"ok":null,"name":null,"shared":null,"retained":null,"raw":null,"plain":null}"#
    );
    assert_eq!(encoded.len(), absent.json_len());
}
