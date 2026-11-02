use crate::{app::serve, runtime::paths::Paths};

pub(super) fn define(
    paths: &Paths,
    generics: &crate::types::Generics,
    fibers: &[(serve::Task<'_, '_>, &serve::Fiber<'_>)],
    ident: &syn::Ident,
) -> proc_macro2::TokenStream {
    let [g_state, g_sink] = generics.idents(["S", "__S"]);
    let [v_sink, v_output] = generics.locals(["__sink", "__output"]);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    use quote::{format_ident, quote};

    let routes: Vec<_> = fibers.iter().map(|pair| pair.0.route()).collect();
    let variants: Vec<_> = fibers
        .iter()
        .map(|pair| {
            let task = pair.0;
            let route = task.route();
            let variant = format_ident!("__Route{:04}", task.slot());
            quote! {
                #variant(#ns_core::manifold::task::response::Output<'d, #route, #g_state>),
            }
        })
        .collect();
    let id_arms = fibers.iter().map(|pair| {
        let task = pair.0;
        let variant = format_ident!("__Route{:04}", task.slot());
        quote! { Self::#variant(#v_output) => #v_output.id(), }
    });
    let emits: Vec<_> = fibers
        .iter()
        .map(|pair| {
            let task = pair.0;
            let variant = format_ident!("__Route{:04}", task.slot());
            quote! {
                Self::#variant(#v_output) => #v_output.emit(#v_sink),
            }
        })
        .collect();
    if variants.is_empty() {
        proc_macro2::TokenStream::new()
    } else {
        let packers = fibers.iter().map(|pair| {
            let task = pair.0;
            let route = task.route();
            let variant = format_ident!("__Route{:04}", task.slot());
            let packer = task.request();
            quote! {
                impl<'d, #g_state> #ns_core::manifold::task::response::Pack<
                    'd,
                    #route,
                    #g_state,
                    #ident<'d, #g_state>,
                > for #packer
                where #( #routes: #ns_core::route::Task<'d, #g_state>, )*
                {
                    fn pack(
                        #v_output: #ns_core::manifold::task::response::Output<'d, #route, #g_state>,
                    ) -> #ident<'d, #g_state> {
                        #ident::#variant(#v_output)
                    }
                }
            }
        });
        quote! {
            #[doc(hidden)]
            enum #ident<'d, #g_state>
            where #( #routes: #ns_core::route::Task<'d, #g_state>, )*
            {
                #( #variants )*
            }

            impl<'d, #g_state> #ns_core::manifold::task::response::Completion<'d> for #ident<'d, #g_state>
            where #( #routes: #ns_core::route::Task<'d, #g_state>, )*
            {
                fn id(&self) -> #ns_http::request::Id<'d> {
                    match self {
                        #( #id_arms )*
                    }
                }

                fn emit<#g_sink: #ns_core::decode::Encode<'d, 'd>>(
                    self,
                    #v_sink: #g_sink,
                ) -> ::core::result::Result<
                    (
                        <#g_sink as #ns_core::decode::Encode<'d, 'd>>::Prepared,
                        #ns_core::life::Outcome,
                    ),
                    (
                        <#g_sink as #ns_core::decode::Encode<'d, 'd>>::Error,
                        #g_sink::Target,
                    ),
                > {
                    match self {
                        #( #emits )*
                    }
                }
            }

            #( #packers )*
        }
    }
}
