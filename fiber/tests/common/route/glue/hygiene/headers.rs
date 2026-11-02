use core::primitive;

use sark_http::payload::{fixed, response::status};

use self::{Boolean as bool, Byte as u8, Half as u16, Wide as u64, Word as u32};

struct Byte;
struct Half;
struct Word;
struct Wide;
struct Boolean;
struct Option<T>(T);
struct Some<T>(T);
struct None;
struct Ok<T>(T);
struct Err<T>(T);

const _: (u8, u16, u32, u64, Some<()>, None) = (u8, u16, u32, u64, Some(()), None);
const _: (bool, Option<primitive::u8>, Ok<()>, Err<()>) = (bool, Option(0), Ok(()), Err(()));

#[sark_fiber::request]
pub struct Request {
    #[header("x-token-a", default = "")]
    pub __sark_m: Shared,
    #[header("x-token-abc", default = "")]
    pub __sark_mode: Shared,
    #[header("x-token-abcdefg", default = "")]
    pub eight: Shared,
}

#[sark_fiber::response(raw)]
#[header_static("x-hygiene", "kept")]
pub struct Reply<'d> {
    pub(super) r#status: status::Code,
    pub(super) r#body: r#fixed::r#Body<'d>,
}
