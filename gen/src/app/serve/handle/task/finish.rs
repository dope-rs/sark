use crate::{app::serve, runtime::paths::Paths};

pub(super) fn define(
    paths: &Paths,
    generics: &crate::types::Generics,
    fibers: &[(serve::Task<'_, '_>, &serve::Fiber<'_>)],
    ident: &syn::Ident,
) -> proc_macro2::TokenStream {
    if fibers.is_empty() {
        return proc_macro2::TokenStream::new();
    }
    let g_sink = generics.ident("__S");
    let [v_sink, v_output] = generics.locals(["__sink", "__output"]);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    use quote::quote;

    let variants: Vec<_> = fibers.iter().map(|pair| pair.0.variant()).collect();
    let types: Vec<_> = (0..fibers.len())
        .map(|index| generics.ident(&format!("__C{index}")))
        .collect();
    quote! {
        #[doc(hidden)]
        enum #ident<#( #types ),*> {
            #( #variants(#types), )*
        }

        impl<'d, #( #types ),*> #ns_core::manifold::task::response::Completion<'d> for #ident<#( #types ),*>
        where #( #types: #ns_core::manifold::task::response::Completion<'d>, )*
        {
            fn emit<#g_sink: #ns_core::decode::Encode<'d, 'd>>(
                self,
                #v_sink: impl ::core::ops::FnOnce(#ns_http::request::Id<'d>) -> #g_sink,
            ) -> #ns_core::manifold::task::response::Complete<'d, #g_sink> {
                match self {
                    #(Self::#variants(#v_output) => #ns_core::manifold::task::response::Completion::emit(#v_output, #v_sink),)*
                }
            }
        }
    }
}
