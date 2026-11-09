use crate::{
    app::serve::{self, layout},
    runtime::paths::Paths,
};

#[derive(Clone, Copy)]
pub(in crate::app::serve) struct Task<'a, 'spec> {
    route: &'a serve::RouteSpec<'spec, serve::Mode<'spec>>,
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

    pub(in crate::app::serve) fn capacity(self) -> &'spec syn::Expr {
        self.spec.capacity
    }

    pub(in crate::app::serve) fn tag(self, owner: &syn::Ident) -> syn::Ident {
        self.spec.tag(owner)
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

    pub(in crate::app::serve) fn path(self) -> &'a layout::Path {
        self.spec.path()
    }

    pub(in crate::app::serve) fn store_fiber(
        self,
        paths: &Paths,
        fiber: &serve::Fiber<'_>,
        state: &syn::Type,
    ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let future = &fiber.future;
        let route = self.route();
        let output = self.fiber_output(paths, state);
        let ty = if fiber.stream {
            quote::quote!(
                #ns_fiber::manifold::Phase<
                    #future,
                    #output,
                    #ns_core::manifold::task::response::Stream<'d, #route, #state>,
                >
            )
        } else {
            quote::quote!(#future)
        };
        let bound = quote::quote! {
            #future: #ns_fiber::dope_fiber::abi::Fiber<'d, Output = #output>,
        };
        (ty, bound)
    }

    pub(in crate::app::serve) fn is_stream(self) -> bool {
        matches!(&self.route.mode, serve::Mode::Fiber { fiber, .. } if fiber.stream)
    }

    pub(in crate::app::serve) fn make(
        self,
        paths: &Paths,
        task: &syn::Ident,
    ) -> proc_macro2::TokenStream {
        let ns_fiber = &paths.fiber;
        if self.is_stream() {
            quote::quote!(#ns_fiber::manifold::Phase::new(#task))
        } else {
            quote::quote!(#task)
        }
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
