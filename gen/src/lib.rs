//! Borrow-preserving JSON and protobuf derives.
//!
//! JSON fields: scalar, `nested`, `seq`, and `seq + nested`.

mod app;
mod handler;
mod json;
mod macros;
mod request;
mod response;
mod runtime;
mod types;

use macros::fibers;
use runtime::paths;

/// Derives borrowed protobuf encoding for one declared bytes field.
#[proc_macro_derive(Message, attributes(prost))]
pub fn message(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    use macros::message::Message;
    match Message::new(input).and_then(Message::expand) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Produces a statically validated direct response header name.
#[proc_macro]
pub fn header_name(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let value = syn::parse_macro_input!(input as syn::LitStr);
    emit(
        |paths| Ok(response::name(paths, &value)),
        runtime::Requirement::Http,
    )
}

fn emit(
    build: impl FnOnce(&paths::Paths) -> syn::Result<proc_macro2::TokenStream>,
    need: runtime::Requirement,
) -> proc_macro::TokenStream {
    let result = paths::Paths::resolve(need)
        .and_then(|paths| build(&paths).and_then(|tokens| paths.finish(tokens)));
    match result {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Lowers an `async move` block through the consuming crate's `sark-fiber` facade.
#[proc_macro]
pub fn fiber(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as fibers::Body);
    emit(|paths| Ok(input.expand(paths)), runtime::Requirement::Fiber)
}

/// Defines statically dispatched routes at module scope; constructors infer state type and const parameters.
/// `'_` selects the driver lifetime; scoped constructors retain state and route bounds.
/// Capacity expressions and type/const arguments cannot contain macros, local bindings, or item declarations.
/// The current pin-project expansion rejects const parameters named `core` (including `r#core`); use another name.
#[proc_macro]
pub fn define_route(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    use self::app::App;
    let input = syn::parse_macro_input!(input as app::inputs::Input);
    emit(
        |paths| {
            paths.h1.require()?;
            App::new(paths, input).map(|app| app.expand(paths, runtime::H1::Available))
        },
        runtime::Requirement::Fiber,
    )
}

/// Defines a protocol-neutral multiplexed task application at module scope.
/// The input and const-parameter naming restrictions of [`define_route`] also apply here.
#[proc_macro]
pub fn define_task_route(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    use self::app::App;
    let input = syn::parse_macro_input!(input as app::inputs::Input);
    emit(
        |paths| App::new(paths, input).map(|app| app.expand(paths, runtime::H1::Missing)),
        runtime::Requirement::Fiber,
    )
}

/// Defines a fiber-free, statically dispatched router at module scope.
/// Omitting `: State` emits a zero-sized router backed by `sark::EmptyState`.
/// The type and const argument restrictions of [`define_route`] also apply here.
#[proc_macro]
pub fn define_router(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    use self::app::App;
    let input = syn::parse_macro_input!(input as app::inputs::Input);
    emit(
        |paths| App::new_router(paths, input).map(|app| app.router(paths, paths.h1())),
        runtime::Requirement::Portable,
    )
}

/// Leading visibility affects the marker; function visibility and state-family type/const bounds remain intact.
/// Async state bounds use `'_` for the driver; responses may retain state-associated types. Signature `for<...>` binders cannot declare `'req` or `'d`.
/// `request = Schema` disambiguates parsing/timer aliases; no-arg, `&State`, and async `Timer<'_>` handlers infer empty requests.
#[proc_macro_attribute]
pub fn handler(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use self::handler::{Entry, Handler, args};
    let args = syn::parse_macro_input!(attr as args::Args);
    let fun = syn::parse_macro_input!(item as syn::ItemFn);
    let need = if fun.sig.asyncness.is_some() {
        runtime::Requirement::Fiber
    } else {
        runtime::Requirement::Portable
    };
    emit(
        |paths| {
            Handler::new(args, fun, Entry::Handler)
                .and_then(|handler| handler.expand(paths, paths.h1()))
        },
        need,
    )
}

/// Defines a synchronous CONNECT session constructor using the same request
/// projection as an HTTP handler while preserving state-borrowing sessions.
/// Signature `for<...>` binders follow the lifetime naming requirement of [`handler`].
#[proc_macro_attribute]
pub fn duplex(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use syn::{Error, Item};

    use self::handler::{Entry, Handler};

    let args = syn::parse_macro_input!(attr as handler::args::Args);
    let item = syn::parse_macro_input!(item as Item);
    emit(
        |paths| match item {
            Item::Fn(fun) => Handler::new(args, fun, Entry::Duplex)
                .and_then(|handler| handler.expand(paths, paths.h1())),
            other => Err(Error::new_spanned(
                other,
                "#[sark_gen::duplex] supports only fn",
            )),
        },
        runtime::Requirement::Fiber,
    )
}

/// Derives JSON decoding and encoding for structs declared at module scope.
/// Decoding requires nongeneric structs; `encode` and `try_encode` also support generics.
/// Type and const arguments cannot contain macros, local bindings, or item declarations.
#[proc_macro_attribute]
pub fn json(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let st = syn::parse_macro_input!(item as syn::ItemStruct);
    let mode = syn::parse_macro_input!(attr as json::mode::Mode);
    emit(
        |paths| json::build(paths, mode, st),
        runtime::Requirement::Json,
    )
}

/// Generates a request projection for a module-scope struct whose fields declare their wire source.
/// `#[authority]` accepts retained bytes; H1 uses Host/absolute targets while
/// H2 and H3 use `:authority` with Host fallback, retaining it only on demand.
#[proc_macro_attribute]
pub fn request(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let mode = syn::parse_macro_input!(attr as request::mode::Mode);
    let st = syn::parse_macro_input!(item as syn::ItemStruct);
    emit(
        |paths| {
            if st
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("json_body"))
            {
                paths.json.require()?;
            }
            request::build(paths, mode, st)
        },
        runtime::Requirement::Portable,
    )
}

#[proc_macro_attribute]
pub fn response(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use self::response::code;
    let mode = syn::parse_macro_input!(attr as response::Mode);
    let st = syn::parse_macro_input!(item as syn::ItemStruct);
    emit(
        |paths| {
            if matches!(mode, response::Mode::Json) {
                paths.json.require()?;
            }
            code::build(paths, mode, st)
        },
        runtime::Requirement::Portable,
    )
}
