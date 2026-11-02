use crate::{macros::tokens::local, runtime::paths::Paths};

pub(in crate::request) fn build<'a>(
    paths: &Paths,
    raw: &'a proc_macro2::Ident,
    error: &'a proc_macro2::TokenStream,
    slot: &'a proc_macro2::TokenStream,
    span: &'a proc_macro2::TokenStream,
    set: &'a proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let [v_name, v_heads, v_slot, v_value] = ["name", "headers", "slot", "value"].map(local);
    quote::quote! {
            fn slice(#v_name: &[::core::primitive::u8]) -> ::core::option::Option<Self> {
                #slot
            }

            fn span(
                #v_name: #ns_http::field::metadata::map::record::Span<'_>,
            ) -> ::core::option::Option<Self> {
                if let ::core::option::Option::Some(#v_name) = #v_name.as_slice() {
                    return Self::slice(#v_name);
                }
                #span
            }

            fn set<__V: #ns_core::request::field::HeaderValue>(
                #v_heads: &mut #raw,
                #v_slot: Self,
                #v_value: &__V,
            ) -> ::core::result::Result<(), #error> {
                #set
            }

    }
}
