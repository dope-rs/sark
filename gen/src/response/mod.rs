use crate::runtime::paths::Paths;

pub(super) mod code;
mod headers;
mod mode;

pub(super) use mode::Mode;

pub(super) fn name(paths: &Paths, value: &syn::LitStr) -> proc_macro2::TokenStream {
    let ns_http = &paths.http;
    let value = syn::LitByteStr::new(value.value().to_ascii_lowercase().as_bytes(), value.span());
    quote::quote_spanned! {value.span()=> {
        const __SARK_NAME: #ns_http::payload::response::headers::Name<&'static [::core::primitive::u8]> =
            #ns_http::field::literal!(#ns_http::payload::response::headers::Name::try_bytes(#value));
        __SARK_NAME
    }}
}
