use crate::{
    app::{
        serve::{
            self, duplex,
            handle::{self, decode, dispatch, head},
        },
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(in crate::app) fn coding(paths: &Paths, spec: &spec::Spec) -> proc_macro2::TokenStream {
    use quote::quote;

    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let routes = spec
        .route_specs
        .iter()
        .filter(|entry| matches!(entry.mode, spec::Mode::Sync))
        .map(|entry| &entry.route);
    quote! {
        <#ns_h1::wire::output::Select<{
            false #( || ::core::matches!(
                <#routes as #ns_h1::route::PolicyOf>::POLICY.coding,
                #ns_http::h1::head::request::Coding::Gzip,
            ) )*
        }> as #ns_h1::wire::output::Selected>::Compression
    }
}

pub(in crate::app) fn build(
    paths: &Paths,
    spec: &spec::Spec,
    h1: crate::runtime::H1,
) -> proc_macro2::TokenStream {
    let f_state = local("state");
    let symbols = &spec.state_generics;
    let [n_method, n_pend] = symbols.locals(["__method", "__path_end"]);
    let [n_rproto, n_target] = symbols.locals(["__route_protocol", "__target"]);
    let [v_edit, v_head, v_index, v_key] = symbols.locals(["edit", "head", "index", "key"]);
    let [v_params, v_wait, v_resume] = symbols.locals(["params", "pending", "resume"]);
    let [v_state, v_take, v_value] = symbols.locals(["state", "take", "value"]);
    let [v_visit] = symbols.locals(["visit"]);
    let [l_io, l_r, l_state] = spec.state_generics.lives(["__io", "__req", "__state"]);
    let [g_e, g_h, g_t, g_v] = spec.state_generics.idents(["__E", "__H", "__T", "__V"]);
    use quote::{format_ident, quote};

    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_h1 = &paths.h1;
    let name = &spec.name;
    let entries = spec
        .route_specs
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            serve::RouteSpec::new(name, index, entry, spec::Kind::Sync, (), serve::Flow::Push)
        })
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
    let plan = &head.plan;
    let selected = &head.names.prepared_ident;
    let variants = &decode.prepared.variant_defs;
    let structs = &decode.prepared.state.slots;
    let flow = &decode.prepared.state.definition;
    let error = &decode.prepared.type_defs;
    let method = &decode.method;
    let full = &decode.full;
    let mask = &methods.method_mask;
    let count = entries.len();
    let table = format_ident!("{}Table", name);
    let pending = format_ident!("{}Pending", name);
    let heads = format_ident!("{}PendingHead", name);
    let params = if lives.is_empty() && decls.is_empty() {
        proc_macro2::TokenStream::new()
    } else {
        quote!(< #( #lives, )* #( #decls, )* >)
    };
    let guards = handle::emit_guards(paths, spec, entries);
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
    let h1_impl = match h1 {
        crate::runtime::H1::Available(_) => {
            let coding = coding(paths, spec);
            let (variants, slots, routes): (Vec<_>, Vec<_>, Vec<_>) = entries
                .iter()
                .map(|item| (&item.pending_variant, &item.head, &item.entry.route))
                .collect();
            let methods = entries
                .iter()
                .map(|item| item.entry.method.http_token(paths));
            let io = quote!(<#l_io>);
            let find_h1 = [false, true].into_iter().map(|owned| {
                let (method, source, visitor, error, rejected) = if owned {
                    (
                        quote!(find_owned),
                        quote!(#ns_h1::wire::parsing::Owned),
                        quote!(#ns_h1::route::set::OwnedVisit),
                        quote!((#v_head, #v_visit)),
                        quote!((#ns_h1::wire::parsing::Owned, #g_v)),
                    )
                } else {
                    (
                        quote!(find),
                        quote!(&#ns_h1::wire::parsing::Head<'_>),
                        quote!(#ns_h1::route::set::Visit),
                        quote!(#v_visit),
                        quote!(#g_v),
                    )
                };
                let matcher = spec.route_plan.compile(paths, &spec.state_generics, |index, captures| {
                    let item = &entries[index];
                    let slot = &item.head;
                    let route = &item.entry.route;
                    let found = if spec.route_plan.has_params(index) {
                        quote! {{
                            let #v_params =
                                <<<#route as #ns_core::route::Spec>::Request
                                    as #ns_core::route::Schema>::RawParams
                                    as #ns_core::route::Params>::from_match(#captures);
                            // SAFETY: the matcher used this route and this unchanged head.
                            unsafe {
                                #ns_h1::wire::parsing::raw::Bound::<_, #route>::new(#v_head, #v_params)
                            }
                        }}
                    } else {
                        quote! {
                            #ns_h1::wire::parsing::raw::Bound::<_, #route>::empty(#v_head)
                        }
                    };
                    quote! {
                        return ::core::result::Result::Ok(
                            #visitor::visit::<#slot>(
                                #v_visit,
                                #found,
                            ),
                        );
                    }
                });
                quote! {
                    fn #method<#l_io, #g_v>(
                        #v_head: #source,
                        #v_visit: #g_v,
                    ) -> ::core::result::Result<#g_v::Output, #rejected>
                    where
                        #g_v: #visitor<
                            #l_io,
                            #state_ty,
                            Self::Set,
                            Self::Pending<#l_io>,
                            Self::Head,
                        >,
                    {
                        let ::core::option::Option::Some(#n_method) = #v_head.request().matched()
                        else {
                            return ::core::result::Result::Err(#error);
                        };
                        let ::core::option::Option::Some(#n_target) = #v_head.request().path() else {
                            return ::core::result::Result::Err(#error);
                        };
                        let #n_rproto: ::core::option::Option<&[::core::primitive::u8]> =
                            ::core::option::Option::None;
                        #matcher
                        ::core::result::Result::Err(#error)
                    }
                }
            });
            quote! {
                #vis enum #pending #io {
                    #( #variants(#ns_h1::route::sync::Pending<#l_io, #routes>), )*
                }

                #vis enum #heads {
                    #( #variants(#ns_h1::wire::parsing::framing::PendingHead<
                        #routes,
                        #ns_http::h1::head::request::Coding,
                    >), )*
                }

                #(
                    impl #params #ns_h1::route::set::Slot<#state_ty, #table> for #slots
                    where
                        #bounds
                    {
                        type Pending<#l_io> = #pending #io;
                        type Head = #heads;

                        const METHOD: #ns_http::method::Method = #methods;

                        fn pending<#l_io>(
                            #v_value: #ns_h1::route::sync::Pending<#l_io, #routes>,
                        ) -> Self::Pending<#l_io> {
                            #pending::#variants(#v_value)
                        }

                        fn head(
                            #v_value: #ns_h1::wire::parsing::framing::PendingHead<
                                #routes,
                                #ns_http::h1::head::request::Coding,
                            >,
                        ) -> Self::Head {
                            #heads::#variants(#v_value)
                        }
                    }
                )*

                impl #params #ns_h1::route::set::Table<#state_ty> for #table
                where
                    #bounds
                {
                    type Set = Self;
                    type Pending<#l_io> = #pending #io;
                    type Head = #heads;
                    type Compression = #coding;

                    const LEN: ::core::primitive::usize = #count;
                    const METHODS: ::core::primitive::u8 = #mask;
                    const HAS_CACHE: ::core::primitive::bool = false #(|| ::core::matches!(
                        <#routes as #ns_h1::route::PolicyOf>::POLICY.cache,
                        #ns_h1::route::Cache::Store,
                    ))*;

                    #( #find_h1 )*

                    fn resume_head<#l_io, #g_h>(
                        #v_wait: Self::Head,
                        #v_resume: #g_h,
                    ) -> #g_h::Output
                    where
                        #g_h: #ns_h1::route::set::ResumeHead<
                            #l_io,
                            #state_ty,
                            Self::Set,
                            Self::Pending<#l_io>,
                            Self::Head,
                        >,
                    {
                        match #v_wait {
                            #( #heads::#variants(#v_wait) =>
                                #ns_h1::route::set::ResumeHead::resume::<#slots>(#v_resume, #v_wait), )*
                        }
                    }

                    fn edit<#l_io, #g_e>(
                        #v_wait: &mut Self::Pending<#l_io>,
                        #v_edit: #g_e,
                    ) -> #g_e::Output
                    where
                        #g_e: #ns_h1::route::set::Edit<#l_io, #state_ty, Self::Set>,
                    {
                        match #v_wait {
                            #( #pending::#variants(#v_wait) =>
                                #ns_h1::route::set::Edit::edit::<#slots>(#v_edit, #v_wait), )*
                        }
                    }

                    fn take<#l_io, #g_t>(
                        #v_wait: Self::Pending<#l_io>,
                        #v_take: #g_t,
                    ) -> #g_t::Output
                    where
                        #g_t: #ns_h1::route::set::Take<#l_io, #state_ty, Self::Set>,
                    {
                        match #v_wait {
                            #( #pending::#variants(#v_wait) =>
                                #ns_h1::route::set::Take::take::<#slots>(#v_take, #v_wait), )*
                        }
                    }
                }

                impl #decl #ns_h1::route::Router for #name #args
                where
                    #bounds
                {
                }
            }
        }
        crate::runtime::H1::Missing => proc_macro2::TokenStream::new(),
    };
    let guarded = decode.guard.as_ref().map(|guard| {
        quote! {
            impl #decl #ns_core::decode::Guarded for #name #args
            where
                #bounds
            {
                #guard
            }
        }
    });

    quote! {
        #plan

        #( #structs )*

        #(
            #impls
            #guards
        )*

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

        #[doc(hidden)]
        #vis enum #selected {
            #( #variants )*
        }

        #flow

        #router

        #error

        impl #decl #ns_core::decode::Decode for #name #args
        where
            #bounds
        {
            #method
        }

        impl #decl #ns_core::decode::Full for #name #args
        where
            #bounds
        {
            #full
        }

        #guarded

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

        #h1_impl
    }
}
