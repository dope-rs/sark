use crate::{
    app::{serve, serve::duplex, spec},
    runtime::paths::Paths,
};

impl serve::RouteSpec<'_, serve::Mode<'_>> {
    pub(super) fn empty_sync(&self) -> bool {
        use crate::app::route::Method;
        matches!(self.mode, serve::Mode::Sync)
            && matches!(self.entry.method, Method::Get | Method::Head)
    }
}

/// H1-only route state. Task-only expansion has no value of this type, making
/// continuation, guard, and duplex planning unreachable there.
pub(in crate::app::serve) struct H1Plan<'source, 'spec> {
    pub(in crate::app::serve) cleartext: Option<&'source proc_macro2::TokenStream>,
    pub(in crate::app::serve) duplex: &'source duplex::Plan<'spec>,
    entries: &'source [serve::RouteSpec<'spec, serve::Mode<'spec>>],
    pub(super) operations: Vec<usize>,
    pub(in crate::app::serve::handle) continuation: syn::Ident,
    pub(in crate::app::serve::handle) route: syn::Ident,
    pub(in crate::app::serve::handle) ready_route: syn::Ident,
    pub(in crate::app::serve::handle) head: syn::Ident,
}

impl<'source, 'spec> H1Plan<'source, 'spec> {
    pub(in crate::app::serve) fn new(
        plan: &'source serve::ProtocolPlan<'spec>,
        cleartext: Option<&'source proc_macro2::TokenStream>,
    ) -> Self {
        let mut sync = std::collections::BTreeMap::new();
        let mut operations = Vec::with_capacity(plan.entries.len());
        for (index, route) in plan.entries.iter().enumerate() {
            let entry = route.entry;
            let operation = if matches!(route.mode, serve::Mode::Sync)
                && entry.guards.is_empty()
                && entry.route.qself.is_none()
                && let Some(handler) = entry.route.path.get_ident()
            {
                *sync.entry((entry.method, handler)).or_insert(index)
            } else {
                index
            };
            operations.push(operation);
        }
        Self {
            cleartext,
            duplex: &plan.duplex,
            entries: &plan.entries,
            operations,
            continuation: quote::format_ident!("__{}H1Continuation", plan.spec.name),
            route: quote::format_ident!("__{}H1Route", plan.spec.name),
            ready_route: quote::format_ident!("__{}H1ReadyRoute", plan.spec.name),
            head: quote::format_ident!("__{}H1Head", plan.spec.name),
        }
    }

    pub(super) fn heads(
        &self,
    ) -> impl Iterator<Item = (usize, &serve::RouteSpec<'spec, serve::Mode<'spec>>)> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(index, _)| self.operations[*index] == *index)
    }

    pub(in crate::app::serve::handle) fn bodies(
        &self,
    ) -> impl Iterator<Item = &serve::RouteSpec<'spec, serve::Mode<'spec>>> {
        self.heads()
            .map(|(_, item)| item)
            .filter(|item| !item.entry.upgrade && !item.empty_sync())
    }

    /// Emits the exact continuation sum stored by each connection.
    pub(in crate::app::serve) fn definitions(
        &self,
        paths: &Paths,
        exec: &serve::Execution<'source, 'spec>,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        let life = self.bodies().next().map(|_| quote::quote!(<'d>));
        let entries = self.bodies().map(|item| {
            let variant = &item.prepared;
            let route = &item.entry.route;
            let pending = match item.kind {
                spec::Kind::Fiber => quote::quote!(#ns_h1::route::task::Pending<'d, #route>),
                spec::Kind::Sync | spec::Kind::Stream | spec::Kind::Duplex => {
                    quote::quote!(#ns_h1::route::Pending<'d, #route>)
                }
            };
            quote::quote!(#variant(#pending),)
        });
        let continuation = &self.continuation;
        let route = &self.route;
        let built = &self.ready_route;
        let head = &self.head;
        let headers = self.heads().map(|(_, item)| {
            let variant = &item.prepared;
            let route = &item.entry.route;
            let admission = if item.entry.upgrade {
                quote::quote!(#ns_h1::route::duplex::Upgrade)
            } else {
                quote::quote!(())
            };
            let negotiate = if matches!(item.mode, serve::Mode::Sync) {
                quote::quote!(::core::matches!(<#route as #ns_h1::route::PolicyOf>::POLICY.mode,
                    #ns_h1::route::Mode::Gzip | #ns_h1::route::Mode::RequiredGzip))
            } else {
                quote::quote!(false)
            };
            quote::quote!(#variant(#ns_h1::wire::parsing::framing::PendingHead<#route, #admission, { #negotiate }>),)
        });
        let builds = (!exec.fibers().is_empty()).then(|| {
            let builders = exec.fibers().iter().map(|(task, _)| {
                let variant = &task.route.prepared;
                let route = task.route();
                quote::quote!(#variant(#ns_h1::route::task::Ready<'d, #route>),)
            });
            quote::quote!(enum #built<'d> { #( #builders )* })
        });
        quote::quote! {
            struct #continuation<'d, ID: #ns_fiber::Identity, R = #route #life> {
                exchange: #ns_core::life::exchange::Id<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<
                        'd,
                         ID,
                    >,
                    ::core::primitive::u64,
                >,
                route: R,
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
