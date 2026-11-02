use crate::{
    app::{serve, spec},
    runtime::paths::Paths,
};

pub(super) fn define<M>(
    paths: &Paths,
    vis: &syn::Visibility,
    entries: &[serve::RouteSpec<'_, M>],
) -> Vec<proc_macro2::TokenStream> {
    use quote::quote;

    let ns_core = &paths.core;
    let ns_http = &paths.http;

    let heads = entries.iter().map(|item| {
        let slot = &item.head;
        let entry = item.entry;
        let route = &entry.route;
        quote! {
            #[doc(hidden)]
            #vis struct #slot {
                head: #ns_core::request::raw::Parsed<
                    #ns_core::request::Head<
                        #ns_http::field::metadata::section::storage::Bytes,
                        <<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Frame,
                    >,
                    <#route as #ns_core::route::Spec>::Request,
                >,
                method: #ns_http::method::Method,
                content_length: ::core::option::Option<::core::primitive::usize>,
            }
        }
    });
    let requests = entries.iter().map(|item| {
        let slot = &item.request;
        let entry = item.entry;
        let route = &entry.route;
        let length = match item.kind {
            spec::Kind::Duplex => {
                quote! { content_length: ::core::option::Option<::core::primitive::usize>, }
            }
            spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                proc_macro2::TokenStream::new()
            }
        };
        quote! {
            #[doc(hidden)]
            #vis struct #slot {
                head: #ns_core::request::raw::Parsed<
                    #ns_core::request::Head<
                        #ns_http::field::metadata::section::storage::Bytes,
                        <<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Frame,
                    >,
                    <#route as #ns_core::route::Spec>::Request,
                >,
                #length
            }
        }
    });
    heads.chain(requests).collect()
}

pub(super) fn implement<M>(
    paths: &Paths,
    prepared: &syn::Ident,
    entries: &[serve::RouteSpec<'_, M>],
) -> Vec<proc_macro2::TokenStream> {
    use quote::quote;

    let ns_core = &paths.core;

    entries
        .iter()
        .map(|item| {
            let slot = &item.request;
            let variant = &item.prepared;
            quote! {
                impl #ns_core::decode::state::Slot<#prepared> for #slot {
                    fn prepare(self) -> #prepared { #prepared::#variant(self) }
                }
            }
        })
        .collect()
}
