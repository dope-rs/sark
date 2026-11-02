use core::{option, primitive};
use std::{num::NonZero, vec};

use o3::buffer::storage::Shared;

use self::{Boolean as bool, Byte as u8, Size as usize};

struct Byte;
struct Size;
struct Boolean;
struct Option<T>(T);
struct Vec<T>(T);
struct Some<T>(T);
struct None;
struct Ok<T>(T);
struct Err<T>(T);

const _: (u8, usize, bool, Option<primitive::u8>, Vec<()>) = (u8, usize, bool, Option(0), Vec(()));
const _: (Some<()>, None, Ok<()>, Err<()>) = (Some(()), None, Ok(()), Err(()));

#[sark_json::json]
pub struct Scalars {
    pub value: option::Option<u64>,
    pub flag: primitive::bool,
    #[field(raw)]
    pub token: sark_json::Token<8>,
}

pub const FIELD_VALUES: [(&[primitive::u8], option::Option<u64>); 3] = [
    (br#"{"flag":true,"token":null}"#, option::Option::None),
    (
        br#"{"value":null,"flag":true,"token":null}"#,
        option::Option::None,
    ),
    (
        br#"{"value":7,"flag":true,"token":null}"#,
        option::Option::Some(7),
    ),
];
pub const MISSING_FIELDS: &[primitive::u8] = b"{}";
pub const NULL_FLAG: &[primitive::u8] = br#"{"flag":null,"token":null}"#;
pub const NULL_CHILDREN: &[primitive::u8] = br#"{"children":null}"#;
pub const SAME_LENGTH_NAMES: &[primitive::u8] =
    br#"{"\u0076alue":7,"extra":[true],"\u0074oken":[1],"value":false,"flag":true}"#;
pub const INVALID_IGNORED_VALUES: [&[primitive::u8]; 2] = [
    br#"{"\u0076alue":7,"\u0076alue":[1,],"token":[1],"flag":true}"#,
    br#"{"\u0076alue":7,"extra":[1,],"token":[1],"flag":true}"#,
];

#[sark_json::json]
pub struct Optional {
    pub value: option::Option<u64>,
}

#[sark_json::json(ordered)]
pub struct Ordered {
    pub value: option::Option<u64>,
}

#[sark_json::json(ordered, exact)]
pub struct Exact {
    pub value: u64,
}

#[sark_json::json(ordered, plain)]
pub struct Sequence {
    #[field(seq)]
    pub values: vec::Vec<Shared>,
}

#[sark_json::json]
pub struct RawSequence {
    #[field(seq, raw)]
    pub values: vec::Vec<Shared>,
}

#[sark_json::json]
pub struct DepthNode {
    #[field(seq, nested)]
    pub children: vec::Vec<DepthNode>,
    #[field(raw)]
    pub extra: option::Option<Shared>,
}

impl DepthNode {
    pub fn json(nodes: NonZero<primitive::usize>, field: &str, arrays: primitive::usize) -> String {
        let mut input = String::new();
        for _ in 0..nodes.get() {
            input.push_str("{\"children\":[");
        }
        input.push(']');
        if arrays != 0 {
            input.push_str(",\"");
            input.push_str(field);
            input.push_str("\":");
            for _ in 0..arrays {
                input.push('[');
            }
            for _ in 0..arrays {
                input.push(']');
            }
        }
        input.push('}');
        for _ in 1..nodes.get() {
            input.push_str("]}");
        }
        input
    }
}

pub const SEQUENCE: &[primitive::u8] = br#"{"values":["a","b","c","d","e","f","g","h","i"]}"#;
pub const PLAIN_SEQUENCE_ESCAPED: &[primitive::u8] = br#"{"values":["a\nb"]}"#;
pub const RAW_SEQUENCE: &[primitive::u8] = br#"{"values":["a\nb",17,{"nested":[true,null]}]}"#;

#[sark_json::json]
pub struct Escaped {
    #[field(name = "quote\"slash\\line\n\u{1f}")]
    pub __sark_json_marker: u64,
}

#[sark_json::json]
pub struct RenamedSequence {
    #[field(seq, name = "wire-values")]
    pub values: vec::Vec<Shared>,
}
