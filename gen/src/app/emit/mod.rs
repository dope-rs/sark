mod serve;
mod service;

use proc_macro2::TokenStream;
use quote::quote;

use super::spec::Gen;

pub(super) fn render(spec: &Gen) -> TokenStream {
    let vis = &spec.vis;
    let key_ident = &spec.key_ident;
    let parts_ident = &spec.parts_ident;
    let key_vars = &spec.key_vars;
    let parts_vars = &spec.parts_vars;
    let parts_aliases = &spec.parts_aliases;
    let service_tokens = service::tokens(spec);

    quote! {
        #( #parts_aliases )*

        #[derive(Clone, Copy, PartialEq, Eq)]
        #vis enum #key_ident {
            #( #key_vars, )*
            Miss,
        }

        #vis enum #parts_ident {
            #( #parts_vars )*
            Miss { method_key: Option<sark::service::Key>, path_hit: bool },
        }

        #service_tokens
    }
}
