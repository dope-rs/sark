//! `#[sark_gen::json]` derives compact JSON encode/decode for named structs.
//! Fields default to scalars (`u64`, `bool`, `Bytes<Retained>`, `Option<T>`). Two
//! `#[field(...)]` attributes compose nested documents:
//!
//! - `#[field(nested)]` on a field whose type is another `#[sark_gen::json]` struct
//!   emits `"key":{...}` using that type's own encoder.
//! - `#[field(seq)]` on a `Vec<Bytes<Retained>>` emits a JSON string array
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
//!     #[field(seq)] tags: Vec<Bytes<Retained>>,
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
mod body_input;
mod codegen;
mod define_route_input;
mod handler;
mod json;
mod lifetimes;
mod model;
mod request;
mod response;
mod route_compiler;
mod util;

#[proc_macro]
pub fn body(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as body_input::BodyInput);
    input.expand().into()
}

fn emit(result: syn::Result<proc_macro2::TokenStream>) -> TokenStream {
    match result {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro]
pub fn define_route(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as define_route_input::DefineRouteInput);
    emit(app::define_route(input))
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
    emit(match item {
        Item::Fn(fun) => handler::Handler::new(fun).and_then(handler::Handler::expand),
        other => Err(syn::Error::new_spanned(
            other,
            "#[sark_gen::handler] supports only fn",
        )),
    })
}

#[proc_macro_attribute]
pub fn json(attr: TokenStream, item: TokenStream) -> TokenStream {
    let st = parse_macro_input!(item as ItemStruct);
    let mode = parse_macro_input!(attr as json::JsonMode);
    emit(mode.expand(st))
}

#[proc_macro_attribute]
pub fn request(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mode = parse_macro_input!(attr as request::Mode);
    let st = parse_macro_input!(item as ItemStruct);
    emit(mode.expand(st))
}

#[proc_macro_attribute]
pub fn response(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mode = parse_macro_input!(attr as response::Mode);
    let st = parse_macro_input!(item as ItemStruct);
    emit(mode.expand(st))
}
