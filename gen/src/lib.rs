//! `#[sark_gen::json]` derives compact JSON encode/decode for named structs.
//! Fields default to scalars (`u64`, `bool`, `LocalFrameBytes`, `Option<T>`). Two
//! `#[field(...)]` attributes compose nested documents:
//!
//! - `#[field(nested)]` on a field whose type is another `#[sark_gen::json]` struct
//!   emits `"key":{...}` using that type's own encoder.
//! - `#[field(seq)]` on a `Vec<LocalFrameBytes>` emits a JSON string array
//!   `"key":["a","b"]`; combine with `nested` (`#[field(seq, nested)]`) on a
//!   `Vec<T>` of json structs to emit an array of objects `"key":[{...},{...}]`.
//!
//! Empty vectors encode as `[]`. `nested`/`seq` are not supported with `exact`.
//!
//! ```ignore
//! #[sark_gen::json(ordered)]
//! struct Rating { score: u64, count: u64 }
//!
//! #[sark_gen::json(ordered)]
//! struct Item {
//!     id: u64,
//!     #[field(seq)] tags: Vec<LocalFrameBytes>,
//!     #[field(nested)] rating: Rating,
//! }
//!
//! #[sark_gen::json(ordered)]
//! struct ItemsResponse {
//!     #[field(seq, nested)] items: Vec<Item>,
//!     count: u64,
//! }
//! ```

use proc_macro::TokenStream;
use syn::{Item, ItemStruct, parse_macro_input};

mod app;
mod attr;
mod codegen;
mod fixed;
mod json;
mod model;
mod parse;
mod request;
mod response;
mod route_compiler;
mod util;

#[proc_macro]
pub fn body(input: TokenStream) -> TokenStream {
    fixed::TextInput::body(input)
}

#[proc_macro]
pub fn define_route(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as model::DefineRouteInput);
    app::define_route(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn handler(attr_args: TokenStream, item: TokenStream) -> TokenStream {
    if !attr_args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[sark_gen::handler] takes no arguments; path and method belong to define_route!",
        )
        .to_compile_error()
        .into();
    }
    let item = parse_macro_input!(item as Item);
    match item {
        Item::Fn(fun) => attr::attr_fn(fun)
            .unwrap_or_else(|err| err.to_compile_error())
            .into(),
        other => syn::Error::new_spanned(other, "#[sark_gen::handler] supports only fn")
            .to_compile_error()
            .into(),
    }
}

#[proc_macro_attribute]
pub fn json(attr: TokenStream, item: TokenStream) -> TokenStream {
    let st = parse_macro_input!(item as ItemStruct);
    let mode = parse_macro_input!(attr as json::JsonMode);
    json::attr(mode, st)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn request(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mode = parse_macro_input!(attr as request::Mode);
    let st = parse_macro_input!(item as ItemStruct);
    mode.expand(st)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn response(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mode = parse_macro_input!(attr as response::Mode);
    let st = parse_macro_input!(item as ItemStruct);
    mode.expand(st)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
