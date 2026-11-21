//! Protocol-neutral dispatch generation.

use crate::{
    app::{route, serve::handle, spec},
    runtime::paths::Paths,
};

pub(in crate::app::serve::handle) struct Dispatch {
    pub(in crate::app::serve::handle) methods: Methods,
    pub(in crate::app::serve::handle) shape: Shape,
}

pub(in crate::app::serve::handle) struct Methods {
    pub(in crate::app::serve::handle) method_mask: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle) head_method_key: proc_macro2::TokenStream,
}

pub(in crate::app::serve) struct Shape {
    pub(in crate::app::serve) core: CoreArgs,
    pub(in crate::app::serve) base_bounds: proc_macro2::TokenStream,
    pub(in crate::app::serve) generic_def: proc_macro2::TokenStream,
    pub(in crate::app::serve) core_generic_def: proc_macro2::TokenStream,
    pub(in crate::app::serve) protocol_generic_def: proc_macro2::TokenStream,
    pub(in crate::app::serve) generic_use: proc_macro2::TokenStream,
    pub(in crate::app::serve) multiplexed_generic_use: proc_macro2::TokenStream,
}

pub(in crate::app::serve) struct CoreArgs {
    inputs: proc_macro2::TokenStream,
    pub(in crate::app::serve) tasks: proc_macro2::TokenStream,
}

impl CoreArgs {
    pub(in crate::app::serve) fn with(
        &self,
        storage: impl quote::ToTokens,
        mode: impl quote::ToTokens,
    ) -> proc_macro2::TokenStream {
        let Self { inputs, tasks } = self;
        quote::quote!(<#inputs #storage, #mode, #tasks>)
    }
}

impl Methods {
    /// Builds only the method facts shared by direct, H1, H2, and H3 routing.
    pub(in crate::app::serve::handle) fn build(paths: &Paths, spec: &spec::Spec) -> Self {
        use quote::quote;
        let symbols = &spec.state_generics;
        let [v_mbytes, v_mkey] = symbols.locals(["__method_bytes", "__method_key"]);

        let mut enabled = [false; route::Method::ALL.len()];
        for entry in spec.route_specs.iter() {
            enabled[entry.method.ord() as usize] = true;
        }
        let methods = || {
            let enabled = enabled;
            route::Method::ALL
                .into_iter()
                .filter(move |method| enabled[method.ord() as usize])
        };
        let bits = methods().map(|method| method.bit_token(paths));
        let mask = quote!(0u8 #( | #bits )*);
        let hchecks = methods().map(|method| {
            let bytes = method.bytes_token();
            let key = method.key_token(paths);
            quote! {
                if #v_mbytes.is_equal(#bytes) {
                    ::core::option::Option::Some(#key)
                }
            }
        });
        let head = quote! {
            let #v_mkey = #( #hchecks else )*
            {
                ::core::option::Option::None
            };
        };

        Self {
            method_mask: mask,
            head_method_key: head,
        }
    }
}

impl Shape {
    fn build(paths: &Paths, context: &handle::Context<'_, '_, '_>) -> Self {
        let [l_env, g_brand] = context.plan.spec.state_generics.lives(["env", "brand"]);
        let [g_target, g_id, g_mode, g_store, g_stamp, g_wire] = context
            .plan
            .spec
            .state_generics
            .idents(["__C", "__ID", "__M", "__P", "__S", "__W"]);
        use quote::quote;

        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let spec = context.plan.spec;
        let lives = &spec.state_generics.lifetimes;
        let decls = &spec.state_generics.parameters;
        let values = &spec.state_generics.arguments;
        let clauses = &spec.state_generics.predicates;
        let routes = &spec.route_bounds;
        let core = &context.core_ident;
        let slabs = &context.exec.slab_bounds;
        let make_req = &context.exec.producer_bounds;
        let (futures, makers) = context.exec.producers();
        let args = CoreArgs {
            inputs: quote!(#g_brand, #l_env, #( #lives, )* 'd, #( #values, )* #g_id, #g_wire,),
            tasks: quote!(#( #futures, )* #( #makers, )*),
        };
        let tasks = &args.tasks;
        let parameters = quote! {
            #g_brand: #l_env, #l_env, #( #lives: #l_env, )* 'd: #l_env,
            #( #decls, )* #g_id: #ns_fiber::Identity,
            #g_wire: #ns_fiber::dope::net::wire::Spec + #l_env,
        };
        let stored = quote!(#parameters #g_store: #ns_fiber::manifold::runtime::Storage, #g_mode,);
        let app = args.with(g_store, g_mode);
        let multiplexed = args.with(
            quote!(#ns_fiber::manifold::runtime::Prepared<#g_target>),
            quote!(#ns_core::life::mode::Disabled),
        );
        Self {
            base_bounds: quote!(#( #clauses, )* #( #routes )* #( #make_req )* #( #slabs )*),
            generic_def: quote!(<#stored #g_stamp, #tasks>),
            core_generic_def: quote!(<#stored #tasks>),
            protocol_generic_def: quote!(<#parameters #g_target: #ns_core::manifold::task::Target + 'd, #tasks>),
            generic_use: quote!(<#core #app, #g_stamp>),
            multiplexed_generic_use: quote!(<#core #multiplexed, ()>),
            core: args,
        }
    }
}

impl Dispatch {
    /// Composes method facts with application-only task and storage shape.
    pub(in crate::app::serve::handle) fn build(
        paths: &Paths,
        context: &handle::Context<'_, '_, '_>,
    ) -> Self {
        Self {
            methods: Methods::build(paths, context.plan.spec),
            shape: Shape::build(paths, context),
        }
    }
}
