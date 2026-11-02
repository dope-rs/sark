use o3::buffer::storage::Shared;

#[sark_json::json(ordered, plain)]
pub struct PlainBody {
    pub text: Shared,
    #[field(raw)]
    pub number: Shared,
}

#[sark_json::json(ordered, plain)]
pub struct Number {
    pub r#value: u64,
}

#[sark_json::json(ordered)]
pub struct General {
    pub text: Shared,
    #[field(raw)]
    pub number: Shared,
}

#[sark_json::json(ordered, plain)]
pub struct Plain {
    pub text: Shared,
}

#[sark_json::json(unordered, plain)]
pub struct Selective {
    pub text: Shared,
}

pub const SELECTED_PREFIX: &[u8] = br#"{"text":"alpha","#;
pub const IGNORED_VALUES: [&[u8]; 2] = [br#""ignored":""#, br#""\u0074ext":""#];
pub const VALUE_END: &[u8] = br#""}"#;

#[sark_json::json(ordered)]
pub struct Strings {
    #[field(seq)]
    pub values: Vec<Shared>,
}

pub const RAW: &[(&[u8], &[u8])] = &[
    (br#"{"text":"x","number":"a,b]}"}"#, br#""a,b]}""#),
    (
        br#"{"text":"x","number":[1,{"a":true}]}"#,
        br#"[1,{"a":true}]"#,
    ),
    (
        br#"{"text":"x","number":{"a":[null,false]}}"#,
        br#"{"a":[null,false]}"#,
    ),
];

pub const INVALID: &[&[u8]] = &[
    b"{\"text\":\"a\nb\"}",
    b"{\"text\":\"\0\"}",
    b"{\"text\":\"\xff\"}",
    b"{\"text\":\"\xc0\xaf\"}",
    b"{\"text\":\"\xed\xa0\x80\"}",
    b"{\"text\":\"\xf4\x90\x80\x80\"}",
    b"{\"text\":\"\xe2\x82\"}",
    br#"{"ignored":"\q","text":"x"}"#,
    br#"{"text":"alpha","\u0074ext":"\q"}"#,
    br#"{"ignored":1e+,"text":"x"}"#,
    br#"{"text":"alpha","\u0074ext":1e+}"#,
    br#"{"ignored":"\uD800","text":"x"}"#,
    br#"{"ignored":"\uD800\u0041","text":"x"}"#,
    br#"{"ignored":"\uDC00","text":"x"}"#,
    br#"{"ignored":{garbage},"text":"x"}"#,
    br#"{"ignored":[1 2],"text":"x"}"#,
    br#"{"ignored":[true,],"text":"x"}"#,
    br#"{"ignored":{"a":},"text":"x"}"#,
    br#"{"ignored":{"a":null,},"text":"x"}"#,
    br#"{"ignored":{"a" 1},"text":"x"}"#,
    b"{\x0b\"text\":\"x\"}",
    b"{\"text\":\"x\"}\x0c",
];

pub const UNICODE: &[u8] = br#"{"v\u0061lues":["\u0000\u007F\u0080\u07ff\u0800\uD7ff\uE000\uFFFF\uD800\uDC00\uDBFF\uDFFF","plain"]}"#;
pub const DECODED: &[u8] =
    "\u{0}\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}".as_bytes();

pub const INTERRUPTED_UTF8: &[u8] = b"\"\xe2aaaaaaaa\x82\xac\"";
