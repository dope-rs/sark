use crate::{
    app::{serve, spec},
    runtime::paths::Paths,
};

pub(super) fn negotiate(
    paths: &Paths,
    entry: &serve::RouteSpec<'_, serve::Mode<'_>>,
) -> proc_macro2::TokenStream {
    let ns_h1 = &paths.h1;
    let route = &entry.entry.route;
    if matches!(entry.mode, serve::Mode::Sync) {
        quote::quote!(::core::matches!(
            <#route as #ns_h1::route::PolicyOf>::POLICY.mode,
            #ns_h1::route::Mode::Gzip | #ns_h1::route::Mode::RequiredGzip,
        ))
    } else {
        quote::quote!(false)
    }
}

/// Closed generated ownership of every routed H1 continuation.
pub(in crate::app::serve) struct Plan<'source, 'spec> {
    generics: &'spec crate::types::Generics,
    entries: &'source [serve::RouteSpec<'spec, serve::Mode<'spec>>],
    name: syn::Ident,
    route: syn::Ident,
    ready: syn::Ident,
    ready_route: syn::Ident,
    head: syn::Ident,
}

impl<'source, 'spec> Plan<'source, 'spec> {
    pub(in crate::app::serve) fn full(plan: &'source serve::ProtocolPlan<'spec>) -> Self {
        Self {
            generics: &plan.spec.state_generics,
            entries: &plan.entries,
            name: quote::format_ident!("__{}H1Pending", plan.spec.name),
            route: quote::format_ident!("__{}H1Route", plan.spec.name),
            ready: quote::format_ident!("__{}H1Ready", plan.spec.name),
            ready_route: quote::format_ident!("__{}H1ReadyRoute", plan.spec.name),
            head: quote::format_ident!("__{}H1Head", plan.spec.name),
        }
    }

    pub(in crate::app::serve::handle) fn name(&self) -> &syn::Ident {
        &self.name
    }

    pub(in crate::app::serve::handle) fn route(&self) -> &syn::Ident {
        &self.route
    }

    pub(in crate::app::serve::handle) fn bodies(
        &self,
    ) -> impl Iterator<Item = &serve::RouteSpec<'spec, serve::Mode<'spec>>> {
        self.entries.iter().filter(|item| !item.entry.upgrade)
    }

    pub(in crate::app::serve) fn head(&self) -> &syn::Ident {
        &self.head
    }

    pub(in crate::app::serve) fn ty(&self) -> proc_macro2::TokenStream {
        let g_id = self.generics.ident("__ID");
        let name = &self.name;
        quote::quote!(#name<'d, #g_id>)
    }

    pub(in crate::app::serve::handle) fn ready(&self) -> &syn::Ident {
        &self.ready
    }

    pub(in crate::app::serve::handle) fn ready_route(&self) -> &syn::Ident {
        &self.ready_route
    }

    pub(in crate::app::serve) fn ready_ty(&self) -> proc_macro2::TokenStream {
        let g_id = self.generics.ident("__ID");
        if !self
            .entries
            .iter()
            .any(|item| item.kind == spec::Kind::Fiber)
        {
            return quote::quote!(::core::convert::Infallible);
        }
        let ready = &self.ready;
        quote::quote!(#ready<'d, #g_id>)
    }

    /// Emits the exact continuation sum stored by each connection.
    pub(in crate::app::serve) fn definitions(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        let life = self.bodies().next().map(|_| quote::quote!(<'d>));
        let entries = self.bodies().map(|item| {
            let variant = &item.pending_variant;
            let route = &item.entry.route;
            let pending = match item.kind {
                spec::Kind::Fiber => quote::quote!(#ns_h1::route::task::Pending<'d, #route>),
                spec::Kind::Sync | spec::Kind::Stream | spec::Kind::Duplex => {
                    quote::quote!(#ns_h1::route::Pending<'d, #route>)
                }
            };
            quote::quote!(#variant(#pending),)
        });
        let name = &self.name;
        let route = &self.route;
        let ready = &self.ready;
        let built = &self.ready_route;
        let head = &self.head;
        let headers = self.entries.iter().map(|item| {
            let variant = &item.pending_variant;
            let route = &item.entry.route;
            let admission = if item.entry.upgrade {
                quote::quote!(#ns_h1::route::duplex::Upgrade)
            } else {
                quote::quote!(())
            };
            let negotiate = negotiate(paths, item);
            quote::quote!(#variant(#ns_h1::wire::parsing::framing::PendingHead<#route, #admission, { #negotiate }>),)
        });
        let builders: Vec<_> = self
            .entries
            .iter()
            .filter_map(|item| {
                if item.kind != spec::Kind::Fiber {
                    return None;
                }
                let variant = &item.pending_variant;
                let route = &item.entry.route;
                Some(quote::quote!(#variant(#ns_h1::route::task::Ready<'d, #route>),))
            })
            .collect();
        let builds = (!builders.is_empty()).then(|| {
            quote::quote! {
                struct #ready<'d, ID: #ns_fiber::Identity> {
                    exchange: #ns_core::life::exchange::Id<
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, ID>,
                        ::core::primitive::u64,
                    >,
                    route: #built<'d>,
                }

                enum #built<'d> {
                    #( #builders )*
                }
            }
        });
        quote::quote! {
            struct #name<'d, ID: #ns_fiber::Identity> {
                exchange: #ns_core::life::exchange::Id<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<
                        'd,
                         ID,
                    >,
                    ::core::primitive::u64,
                >,
                route: #route #life,
            }

            enum #route #life {
                #( #entries )*
            }

            #builds

            enum #head {
                #( #headers )*
            }
        }
    }
}
