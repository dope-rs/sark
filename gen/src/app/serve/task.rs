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

    pub(in crate::app::serve) fn slot(self) -> usize {
        self.spec.slot
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
        let ns_fiber = &paths.fiber;
        let future = &fiber.future;
        let ty = quote::quote!(#future);
        let output = self.fiber_output(paths, state);
        let bound = quote::quote! {
            #ty: #ns_fiber::dope_fiber::abi::Fiber<'d, Output = #output>,
        };
        (ty, bound)
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
                Item = #ns_http::payload::response::dispatch::chunk::Chunk<'d>,
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

/// Owns the generated task types and bounds.
pub(in crate::app::serve) struct TaskPlan {
    pub(in crate::app::serve) identity: syn::Ident,
    pub(in crate::app::serve) fiber: syn::Ident,
    pub(in crate::app::serve) direct: Vec<proc_macro2::TokenStream>,
    pub(in crate::app::serve) queued: Vec<proc_macro2::TokenStream>,
    pub(in crate::app::serve) producer_bounds: Vec<proc_macro2::TokenStream>,
    pub(in crate::app::serve) slab_bounds: Vec<proc_macro2::TokenStream>,
}

impl TaskPlan {
    pub(in crate::app::serve) fn new(
        paths: &Paths,
        spec: &crate::app::spec::Spec,
        fibers: &[(Task<'_, '_>, &serve::Fiber<'_>)],
        direct: Vec<proc_macro2::TokenStream>,
        queued: Vec<proc_macro2::TokenStream>,
        slabs: Vec<proc_macro2::TokenStream>,
    ) -> Self {
        let generics = &spec.state_generics;
        let owner = &spec.name;
        let state = spec.state.ty();
        let make = producer_bounds(paths, generics, fibers, state);
        Self {
            identity: quote::format_ident!("__{}Task", owner),
            fiber: quote::format_ident!("__{}Fiber", owner),
            direct,
            queued,
            producer_bounds: make,
            slab_bounds: slabs,
        }
    }
}

fn producer_bounds(
    paths: &Paths,
    generics: &crate::types::Generics,
    fibers: &[(Task<'_, '_>, &serve::Fiber<'_>)],
    state_ty: &syn::Type,
) -> Vec<proc_macro2::TokenStream> {
    let l_env = generics.lifetime("env");
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let mut bounds = Vec::new();
    for (task, fiber) in fibers.iter().copied() {
        let route = task.route();
        let future = &fiber.future;
        let maker = &fiber.maker;
        let output = task.fiber_output(paths, state_ty);
        let builder = fiber.body_bound(paths, route);
        bounds.push(quote::quote! {
            #builder
            #future: #ns_fiber::Fiber<
                    'd,
                    Output = #output,
                > + #l_env,
            #maker: ::core::marker::Copy
                + #l_env
                + ::core::ops::FnOnce((
                    #ns_core::manifold::task::request::Input<'d, #route>,
                    &#l_env #state_ty,
                    #ns_fiber::driver::schedule::clock::timer::Timer<'d>,
                )) -> #future,
        });
    }
    bounds
}
