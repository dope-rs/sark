use crate::{
    app::{
        serve::{
            self, duplex,
            handle::{decode, dispatch, head},
        },
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(in crate::app) fn build(paths: &Paths, spec: &spec::Spec) -> proc_macro2::TokenStream {
    let f_state = local("state");
    let symbols = &spec.state_generics;
    let [n_method, n_pend] = symbols.locals(["__method", "__path_end"]);
    let [n_rproto, n_target] = symbols.locals(["__route_protocol", "__target"]);
    let [v_index, v_key] = symbols.locals(["index", "key"]);
    let [v_state] = symbols.locals(["state"]);
    let [v_visit] = symbols.locals(["visit"]);
    let [l_r, l_state] = spec.state_generics.lives(["__req", "__state"]);
    let [g_v] = spec.state_generics.idents(["__V"]);
    use quote::{format_ident, quote};

    let ns_core = &paths.core;
    let name = &spec.name;
    let entries = spec
        .route_specs
        .iter()
        .enumerate()
        .map(|(index, entry)| serve::RouteSpec::new(name, index, entry, spec::Kind::Sync, ()))
        .collect::<Vec<_>>();
    let entries = &entries;
    let methods = dispatch::Methods::build(paths, spec);
    let connect = entries
        .iter()
        .fold(duplex::Connect::Disabled, |connect, item| {
            connect.register(item.entry)
        });
    let head = head::Head::build(paths, spec, &methods, entries, connect);
    let decode = decode::Decode::build(paths, spec, &methods, &head, entries, quote!(self));
    let vis = &spec.vis;
    let state_ty = spec.state.ty();
    let lives = &spec.state_generics.lifetimes;
    let decls = &spec.state_generics.parameters;
    let values = &spec.state_generics.arguments;
    let clauses = &spec.state_generics.predicates;
    let mask = &methods.method_mask;
    let count = entries.len();
    let table = format_ident!("{}Table", name);
    let params = if lives.is_empty() && decls.is_empty() {
        proc_macro2::TokenStream::new()
    } else {
        quote!(< #( #lives, )* #( #decls, )* >)
    };
    let impls = entries.iter().enumerate().map(|(index, item)| {
        let index = index as u32;
        let slot = &item.head;
        let entry = item.entry;
        let guards = &entry.guards;
        let route = &entry.route;
        quote! {
            impl #params #ns_core::route::table::Slot<#state_ty> for #slot
            where
                #( #clauses, )*
                #( #guards: #ns_core::route::guard::Guard<#state_ty>, )*
            {
                type Set = #table;
                type Route = #route;
                const INDEX: ::core::primitive::u32 = #index;
            }
        }
    });
    let matcher = spec
        .route_plan
        .compile(paths, &spec.state_generics, |index, captures| {
            let slot = &entries[index].head;
            quote! {
                return ::core::option::Option::Some(
                    #ns_core::route::table::Visit::visit::<#slot>(
                        #v_visit,
                        #ns_core::request::path::Target::new(#n_target),
                        #n_pend,
                        #captures,
                    ),
                );
            }
        });
    let each = entries.iter().enumerate().map(|(index, item)| {
        let index = index as u32;
        let slot = &item.head;
        quote! {
            #index => ::core::option::Option::Some(
                #ns_core::route::table::Each::each::<#slot>(#v_visit),
            ),
        }
    });
    let routes = &spec.route_bounds;
    let bounds = quote! {
        #( #clauses, )*
        #( #routes )*
    };
    let (decl, args, router, state) = match &spec.state {
        spec::State::Empty(_) => (
            quote!(),
            quote!(),
            quote! {
                #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::default::Default)]
                #vis struct #name;

                impl #name {
                    #vis const fn new() -> Self {
                        Self
                    }
                }

                const _: () = ::core::assert!(::core::mem::size_of::<#name>() == 0);
            },
            quote!(#ns_core::manifold::EmptyState::REF),
        ),
        spec::State::Bound(_) => {
            let decl = quote!(<#l_state, #( #lives: #l_state, )* #( #decls, )*>);
            let args = quote!(<#l_state, #( #lives, )* #( #values, )*>);
            let router = quote! {
                #[repr(transparent)]
                #vis struct #name #decl
                where
                    #( #clauses, )*
                {
                    #f_state: &#l_state #state_ty,
                }

                impl #decl #name #args
                where
                    #( #clauses, )*
                {
                    #[inline(always)]
                    #vis const fn new(#v_state: &#l_state #state_ty) -> Self {
                        Self { #f_state: #v_state }
                    }
                }
            };
            (decl, args, router, quote!(self.#f_state))
        }
    };
    let decoder = decode.implement(paths, &decl, quote!(#name #args where #bounds));

    quote! {
        #decoder
        #( #impls )*

        #[doc(hidden)]
        #vis enum #table {}

        // SAFETY: find, each, and Slot::INDEX use the same admitted route order.
        unsafe impl #params #ns_core::route::raw::Table<#state_ty> for #table
        where
            #bounds
        {
            type Set = Self;

            const LEN: ::core::primitive::usize = #count;
            const METHODS: ::core::primitive::u8 = #mask;

            #[inline(always)]
            fn find<#l_r, #g_v>(
                #v_key: #ns_core::route::table::Key<#l_r>,
                #v_visit: #g_v,
            ) -> ::core::option::Option<#g_v::Output>
            where
                #g_v: #ns_core::route::table::Visit<#state_ty, Self::Set>,
            {
                let (#n_method, #n_target, #n_rproto) = #v_key.into_parts();
                #matcher
                ::core::option::Option::None
            }

            #[inline(always)]
            fn each<#g_v>(
                #v_index: ::core::primitive::u32,
                #v_visit: #g_v,
            ) -> ::core::option::Option<#g_v::Output>
            where
                #g_v: #ns_core::route::table::Each<#state_ty, Self::Set>,
            {
                match #v_index {
                    #( #each )*
                    _ => ::core::option::Option::None,
                }
            }
        }

        #router

        impl #decl #ns_core::route::table::Router for #name #args
        where
            #bounds
        {
            type State = #state_ty;
            type Table = #table;

            #[inline(always)]
            fn state(&self) -> &Self::State {
                #state
            }
        }

    }
}
