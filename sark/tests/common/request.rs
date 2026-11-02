use o3::buffer::storage;
use sark::request;
#[sark::request]
pub struct Raw {
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::request]
pub struct Len {
    #[body_len]
    pub body_len: request::BodyLen,
}

#[sark_json::json(ordered)]
pub struct Payload {
    pub value: u64,
}

#[sark::request]
#[json_body(Payload)]
pub struct Json {}

#[sark_json::json(ordered, plain)]
pub struct PlainBody {
    pub text: storage::Shared,
    #[field(raw)]
    pub number: storage::Shared,
}

#[sark::request(ordered)]
#[json_body(PlainBody)]
pub struct Plain {}
