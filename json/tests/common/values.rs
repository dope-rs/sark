use o3::buffer::storage::{self, Shared};

pub const INVALID_TEXT: &[u8] = b"\xff";
pub const INVALID_PLAIN: &[u8] = b"a\"b";
pub const INVALID_RAW: &[u8] = b"[true,]";
pub const ESCAPED_TEXT: &[u8] = br#""a\"b""#;
pub const RAW_TEXT: &str = r#"{"é":["한","\uD83D\uDE00"]}"#;

#[sark_json::json(encode)]
pub struct Item {
    pub score: f64,
    pub error: sark_json::Token<0>,
}

#[sark_json::json(encode)]
pub struct BorrowedText<'a> {
    pub value: &'a [u8],
    #[field(raw)]
    pub raw: &'a [u8],
}

#[sark_json::json(encode)]
pub struct Sparse {
    pub count: Option<u64>,
    pub delta: Option<i64>,
    pub score: Option<f64>,
    pub ok: Option<bool>,
    #[field(nested)]
    pub text: Text,
}

#[sark_json::json(encode)]
pub struct Text {
    pub name: Option<String>,
    pub shared: Option<Shared>,
    pub retained: Option<storage::Shared>,
    #[field(raw)]
    pub raw: Option<Shared>,
    #[field(plain)]
    pub plain: Option<Shared>,
}
