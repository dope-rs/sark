use crate::{app::serve, runtime::paths::Paths};

#[derive(Clone, Copy)]
pub(in crate::app::serve) struct Task<'a, 'spec> {
    pub(super) route: &'a serve::RouteSpec<'spec, serve::Mode<'spec>>,
    spec: &'a serve::TaskSpec<'spec>,
}

impl<'a, 'spec> Task<'a, 'spec> {
    pub(in crate::app::serve) const fn new(
        route: &'a serve::RouteSpec<'spec, serve::Mode<'spec>>,
        spec: &'a serve::TaskSpec<'spec>,
    ) -> Self {
        Self { route, spec }
    }

    pub(in crate::app::serve) fn route(self) -> &'spec syn::TypePath {
        &self.route.entry.route
    }

    pub(in crate::app::serve) fn capacity(self) -> Option<&'spec syn::Expr> {
        self.spec.capacity.as_ref()
    }

    pub(in crate::app::serve) fn variant(self) -> syn::Ident {
        self.spec.variant()
    }

    pub(in crate::app::serve) const fn prepared(self) -> &'a syn::Ident {
        &self.route.prepared
    }

    pub(in crate::app::serve) const fn request(self) -> &'a syn::Ident {
        &self.route.request
    }

    pub(in crate::app::serve) fn quota(self) -> Option<usize> {
        self.spec.quota
    }

    pub(in crate::app::serve) fn phase(self, phase: &str) -> syn::Ident {
        quote::format_ident!("{}{}", self.variant(), phase)
    }

    pub(in crate::app::serve) fn scheduled_admission(
        self,
        paths: &Paths,
        symbols: &crate::types::Generics,
        fiber: &serve::Fiber<'_>,
    ) -> proc_macro2::TokenStream {
        let ns = &paths.fiber;
        let core = &paths.core;
        let [this, scope, admit, owner, parent] =
            symbols.locals(["this", "__task_route", "__insert", "__owner", "__parent"]);
        let tasks = crate::macros::tokens::local("tasks");
        let quotas = crate::macros::tokens::local("quotas");
        let check = fiber.quota.map(|index| {
            quote::quote! {
                if #this.#quotas[#index] == 0 {
                    return Err(#core::manifold::task::Error::Admission(
                        #ns::dope_fiber::task::storage::fixed::schedule::queue::Error::Task(
                            #ns::dope::core::driver::schedule::ready::task::Error::Capacity {
                                requested: ::core::num::NonZero::<usize>::MIN,
                                available: 0,
                            },
                        ),
                    ));
                }
            }
        });
        quote::quote! {
            #check
            let mut #scope = #this.#tasks.as_mut().scope();
            let #admit = #scope.try_admit(#owner, #parent)
                .map_err(#core::manifold::task::Error::Admission)?;
        }
    }

    pub(in crate::app::serve) fn fiber_bound(
        self,
        paths: &Paths,
        fiber: &serve::Fiber<'_>,
        state: &syn::Type,
    ) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        let future = &fiber.future;
        let output = self.fiber_output(paths, state);
        quote::quote! {
            #future: #ns_fiber::dope_fiber::abi::Fiber<'d, Output = #output>,
        }
    }

    pub(in crate::app::serve) fn is_stream(self) -> bool {
        matches!(&self.route.mode, serve::Mode::Fiber { fiber, .. } if fiber.stream)
    }

    pub(in crate::app::serve) fn store_stream(
        self,
        paths: &Paths,
    ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_http = &paths.http;
        let route = self.route();
        let ty = quote::quote!(<#route as #ns_core::route::Spec>::Stream<'d>);
        let bound = quote::quote! {
            #ty: #ns_fiber::dope_fiber::abi::Stream<
                'd,
                Item: ::core::convert::TryInto<
                    #ns_http::payload::response::dispatch::chunk::Chunk<'d>,
                    Error: ::core::fmt::Debug,
                >,
            >,
        };
        (ty, bound)
    }

    pub(in crate::app::serve) fn fiber_output(
        self,
        paths: &Paths,
        state: &syn::Type,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let route = self.route();
        quote::quote!(
            #ns_core::manifold::task::response::Output<'d, #route, #state>
        )
    }
}
