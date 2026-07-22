#[sark_gen::response(raw)]
struct InvalidDynamicName {
    status: http::StatusCode,
    body: &'static [u8],
    #[header("x bad")]
    value: &'static str,
}

#[sark_gen::response(raw)]
#[header("Server", "mine")]
struct ManagedStaticName {
    status: http::StatusCode,
    body: &'static [u8],
}

#[sark_gen::response(raw)]
#[header("x-safe", "first\r\nsecond")]
struct MultilineStaticValue {
    status: http::StatusCode,
    body: &'static [u8],
}

fn main() {}
