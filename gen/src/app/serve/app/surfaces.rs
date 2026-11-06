use crate::{app::serve, runtime::paths::Paths};

pub(super) struct Binding<'emit> {
    pub generic: proc_macro2::TokenStream,
    pub cold_direct: proc_macro2::TokenStream,
    pub bound_direct: proc_macro2::TokenStream,
    pub bounds: &'emit proc_macro2::TokenStream,
    pub task_moves: &'emit proc_macro2::TokenStream,
    pub task_prepare: &'emit proc_macro2::TokenStream,
}

impl<'emit> Binding<'emit> {
    pub(super) fn new(
        paths: &Paths,
        source: &serve::ProtocolPlan<'_>,
        futures: &[&syn::Ident],
        makers: &[&syn::Ident],
        bounds: &'emit proc_macro2::TokenStream,
        (moves, prepare): (
            &'emit proc_macro2::TokenStream,
            &'emit proc_macro2::TokenStream,
        ),
    ) -> Self {
        let generics = &source.spec.state_generics;
        let duplex = source.duplex.state();
        let upgrade = source.entries.iter().any(|route| route.entry.upgrade);
        let [l_wire, l_env] = generics.lives(["__w", "env"]);
        let g_d = generics.ident("__D");
        let [g_brand, g_clock] = generics.lives(["brand", "clock"]);
        let [g_id, g_mode, g_wire] = generics.idents(["__ID", "__M", "__W"]);
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        let states = &generics.lifetimes;
        let decls = &generics.parameters;
        let values = &generics.arguments;
        use quote::quote;

        let generic = quote! {
            <#l_wire, #g_brand: #l_env, #l_env, #( #states: #l_env, )* 'd: #l_wire + #l_env,
                #( #decls, )* #g_id: #ns_fiber::Identity,
                #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd, #g_mode,
                #( #futures, )* #( #makers, )*>
        };
        let usage = |policy, mode, stamp| {
            quote! {
                <#g_brand, #l_env, #( #states, )* 'd, #( #values, )* #g_id, #g_wire, #policy, #mode, #stamp,
                    #( #futures, )* #( #makers, )*>
            }
        };
        Self {
            generic,
            cold_direct: usage(
                quote!(#ns_fiber::manifold::runtime::Cold),
                quote!(#g_mode),
                duplex.retention(paths),
            ),
            bound_direct: usage(
                quote!(#ns_fiber::manifold::runtime::Direct),
                quote!(<#g_d as #ns_h1::server::deadline::policies::Policy<'d, #g_id>>::Mode<#g_mode>),
                duplex.stamp(
                    quote!(&#g_clock #ns_h1::server::date::Clock),
                    duplex.retention(paths),
                    upgrade.then(|| quote!(::std::vec::Vec<::core::primitive::u8>)),
                ),
            ),
            bounds,
            task_moves: moves,
            task_prepare: prepare,
        }
    }
}

#[derive(Default)]
pub(super) struct Extras {
    pub definitions: proc_macro2::TokenStream,
    pub core_fields: proc_macro2::TokenStream,
    pub core_moves: proc_macro2::TokenStream,
    pub public_methods: proc_macro2::TokenStream,
    pub core_initializers: proc_macro2::TokenStream,
}
