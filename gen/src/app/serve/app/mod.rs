use crate::{
    app::serve::{self, flat, handle},
    macros::tokens::local,
    runtime::paths::Paths,
};
mod h1;
mod surfaces;

pub(super) fn build<'source, 'spec>(
    paths: &Paths,
    source: &'source serve::ProtocolPlan<'spec>,
    exec: &serve::Execution<'source, 'spec>,
    surface: Option<&handle::H1Plan<'source, 'spec>>,
    contract: &proc_macro2::TokenStream,
    shape: &handle::dispatch::Shape,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let [f_tasks, f_makers, f_ids] = ["tasks", "task_producers", "ids"].map(local);
    let symbols = &source.spec.state_generics;
    let [n_bind, v_brand, v_ids] = symbols.locals(["__binding", "brand", "ids"]);
    let v_config = symbols.local("config");
    let [v_maker, v_tasks] = symbols.locals(["task_producers", "tasks"]);
    let [l_env, g_brand] = symbols.lives(["env", "brand"]);
    let [g_target, g_id, g_mode, g_store, g_stamp, g_wire] =
        symbols.idents(["__C", "__ID", "__M", "__P", "__S", "__W"]);
    let g_part = symbols.ident("__CORE");
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_fiber = &paths.fiber;
    use proc_macro2::TokenStream;
    use quote::{format_ident, quote};
    let spec = source.spec;
    let vis = &spec.vis;
    let api = &spec.name;
    let name = format_ident!("{}Inner", api);
    let core_id = format_ident!("{}Core", name);
    let state_ty = spec.state.ty();
    let generics = &spec.state_generics;
    let lives = &generics.lifetimes;
    let decls = &generics.parameters;
    let values = &generics.arguments;
    let clauses = &generics.predicates;
    let routes = &spec.route_bounds;
    let tasks = exec.tasks();
    let fibers = exec.fibers().len();
    let h1 = surface.is_some();
    let (futures, makers) = exec.producers();
    let futures: Vec<_> = futures.collect();
    let makers: Vec<_> = makers.collect();
    let slabs = &exec.slab_bounds;
    let makes: Vec<TokenStream> = exec
        .fibers()
        .iter()
        .map(|pair| {
            let route = pair.0.route();
            quote! {
                <#route as #ns_fiber::manifold::runtime::TaskRoute<'d, #state_ty>>::invoke_task::<#l_env>
            }
        })
        .collect();
    let identity = &exec.identity;
    let fiber = &exec.fiber;
    let dflat = flat::build(paths, spec, exec, false);
    let qflat = flat::build(paths, spec, exec, true);
    let dty = &dflat.ty;
    let qty = &qflat.ty;
    let mode = &qflat.mode;
    let direct_quota = tasks
        .iter()
        .filter(|task| task.capacity().is_some())
        .count();
    let queued_quota = exec
        .fibers()
        .iter()
        .filter(|(task, _)| task.capacity().is_some())
        .count();
    let f_quotas = local("quotas");
    let v_quotas = symbols.local("quotas");
    let quota_field = (direct_quota != 0).then(|| {
        quote! {
            #f_quotas: <#g_store as #ns_fiber::manifold::runtime::Storage>::Quota<
                [u32; #direct_quota], [u32; #queued_quota],
            >,
        }
    });
    let quota_init = (direct_quota != 0).then(|| quote!(#f_quotas: (),));
    let quota_move = (direct_quota != 0).then(|| quote!(#f_quotas: #v_quotas,));
    let core_def = &shape.core_generic_def;
    let prod_f = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { #f_makers: ( #( #makers, )* ), }
    };
    let prod_i = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { #f_makers: ( #( #makes, )* ), }
    };
    let store_f = if fibers == 0 {
        quote! {
            #[pin]
            #f_tasks:
                <#g_store as #ns_fiber::manifold::runtime::Storage>::DirectOnly<
                    #g_brand,
                    'd,
                    #dty,
                    #g_id,
                >,
        }
    } else {
        quote! {
            #[pin]
            #f_tasks:
                <#g_store as #ns_fiber::manifold::runtime::Storage>::Tasks<
                    #g_brand,
                    'd,
                    #dty,
                    #qty,
                    #g_id,
                    #mode,
                >,
        }
    };
    let direct = quote! {
        #f_tasks: (
            #v_brand,
            #ns_fiber::o3::depot::capacity::Count::new(
                #v_config.capacity().min(u32::MAX as usize) as u32,
            ),
        ),
    };
    let scheduled = if fibers == 0 {
        quote! { #f_tasks: (), }
    } else {
        direct.clone()
    };
    let ids_f = (fibers != 0).then(|| {
        quote! {
            #f_ids: <#g_store as #ns_fiber::manifold::runtime::Storage>::Resource<
                #ns_http::request::Seq<'d>,
            >,
        }
    });
    let ids_i = (fibers != 0).then(|| quote! { #f_ids: (), });
    let ids_mv = (fibers != 0).then(|| quote! { #f_ids: #v_ids, });
    let ids_prep = if fibers == 0 {
        TokenStream::new()
    } else {
        quote! {
            let () = #v_ids;
            let #v_ids = #ns_http::request::Seq::new(#n_bind.sequence());
        }
    };
    let fields = if tasks.is_empty() {
        TokenStream::new()
    } else {
        quote! {
            #store_f
            #quota_field
            #prod_f
            #ids_f
        }
    };
    let tinit = [direct, scheduled].map(|storage| {
        if tasks.is_empty() {
            TokenStream::new()
        } else {
            quote! { #prod_i #storage #ids_i #quota_init }
        }
    });
    let moves = if tasks.is_empty() {
        TokenStream::new()
    } else if makers.is_empty() {
        quote! { #f_tasks: #v_tasks, #ids_mv #quota_move }
    } else {
        quote! { #f_tasks: #v_tasks, #f_makers: #v_maker, #ids_mv #quota_move }
    };
    let id_def = if !h1 {
        TokenStream::new()
    } else if tasks.is_empty() {
        quote! {
            struct #identity<#g_brand, 'd, __TASK_ID: #ns_fiber::Identity> {
                driver: ::core::marker::PhantomData<
                    fn(&#g_brand (), &'d (), __TASK_ID) -> (&#g_brand (), &'d ()),
                >,
            }
        }
    } else {
        quote! {
            type #identity<#g_brand, 'd, __TASK_ID> =
                #ns_fiber::dope_fiber::task::storage::fixed::Id<#g_brand, 'd, __TASK_ID>;
        }
    };
    let fdef = if fibers == 0 {
        quote! {
            enum #fiber<#g_brand, 'd, __TASK_ID: #ns_fiber::Identity> {
                __Never(
                    ::core::convert::Infallible,
                    ::core::marker::PhantomData<
                        fn(&#g_brand (), &'d (), __TASK_ID) -> (&#g_brand (), &'d ()),
                    >,
                ),
            }
        }
    } else {
        quote! {
            type #fiber<#g_brand, 'd, __TASK_ID> =
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::Id<#g_brand, 'd, __TASK_ID>;
        }
    };
    let bounds = &shape.base_bounds;
    let extras = match surface {
        Some(plan) => h1::extras(
            paths,
            plan,
            source,
            exec,
            &core_id,
            &name,
            surfaces::Binding::new(
                paths,
                &core_id,
                source,
                &futures,
                &makers,
                bounds,
                (&moves, &ids_prep),
            ),
            &tinit[0],
        ),
        None => surfaces::Extras::default(),
    };
    let surfaces::Extras {
        definitions: defs,
        core_fields: core_f,
        core_moves: core_mv,
        public_methods: methods,
        core_initializers: core_i,
    } = extras;
    let proto = &shape.protocol_generic_def;
    let cold = quote! {
        <#core_id<
            #g_brand,
            #l_env,
            #( #lives, )*
            'd,
            #( #values, )*
            #g_id,
            #g_wire,
            #ns_fiber::manifold::runtime::Multi<#g_target>,
            #ns_core::life::mode::Disabled,
            #( #futures, )*
            #( #makers, )*
        >, ()>
    };
    let hot = &shape.multiplexed_generic_use;
    let counts = flat::counts(paths, spec, exec, true);
    let empty_quota = (direct_quota != 0 && queued_quota == 0).then(|| quote!(let #v_quotas = [];));
    let bind = if fibers == 0 {
        quote!(#empty_quota)
    } else {
        quote! {
            let (#v_brand, #v_tasks) = #v_tasks;
            #counts
            #empty_quota
            let #v_tasks = #ns_fiber::dope_fiber::task::storage::fixed::schedule::Storage::try_owned(#v_tasks, #n_bind.owners())?;
            let #v_tasks = #ns_fiber::dope_fiber::task::storage::scope::Owned::new(#v_tasks, #v_brand);
        }
    };
    let [f_time, f_life, f_state] = ["timer", "lifecycle", "state"].map(local);
    let [f_mark, f_core, f_stamp] = ["marker", "core", "stamp"].map(local);
    let [v_core, v_life] = generics.locals(["core", "lifecycle"]);
    let [v_stamp, v_state] = generics.locals(["stamp", "state"]);
    let ddefinition = &dflat.definition;
    let qdefinition = &qflat.definition;
    let sources = exec.fibers().iter().map(|(task, fiber)| {
        crate::app::spec::fiber_bound(paths, task.route(), state_ty, fiber.stream)
    });
    let brand = (!tasks.is_empty())
        .then(|| quote!(#v_brand: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,));
    let scheduled = &tinit[1];
    let app = quote! {
        #defs
        #ddefinition
        #qdefinition
        #id_def
        #fdef
        impl #proto
            #ns_core::manifold::task::app::Bind<'d>
            for #name #cold
        where
            #bounds
        {
            type Target = #g_target;
            type Runtime = #name #hot;
            type Error = #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::PrepareError;

            fn try_bind(
                self,
                #n_bind: #ns_fiber::dope::core::driver::Binding<'d>,
            ) -> ::core::result::Result<Self::Runtime, Self::Error> {
                let #name { #f_core: #v_core, #f_stamp: #v_stamp, .. } = self;
                let #core_id {
                    #core_mv
                    #moves
                    #f_time: (),
                    #f_life: #v_life,
                    #f_state: #v_state,
                    #f_mark: ::core::marker::PhantomData,
                } = #v_core;
                #bind
                #ids_prep
                ::core::result::Result::Ok(#name {
                    #f_core: #core_id {
                        #core_mv
                        #moves
                        #f_time: #n_bind.timer(),
                        #f_life: #v_life,
                        #f_state: #v_state,
                        #f_mark: ::core::marker::PhantomData,
                    },
                    #f_stamp: #v_stamp,
                })
            }
        }

        #[#ns_fiber::pin_project::pin_project(!Unpin)]
        struct #core_id #core_def
        where
            #( #clauses, )* #( #slabs )* #( #sources )*
        {
            #core_f
            #fields
            #f_time: <#g_store as
                #ns_fiber::manifold::runtime::Storage
            >::Resource<#ns_fiber::driver::schedule::clock::timer::Timer<'d>>,
            #f_life: #g_mode,
            #f_state: &#l_env #state_ty,
            #f_mark: ::core::marker::PhantomData<
                (#g_id, #g_wire, #g_store, fn(&#g_brand ()) -> &#g_brand ()),
            >,
        }

        #[#ns_fiber::pin_project::pin_project]
        struct #name<#g_part, #g_stamp> {
            #[pin]
            #f_core: #g_part,
            #[pin]
            #f_stamp: #g_stamp,
        }

        #vis struct #api;

        impl #api {
            #methods

            /// Constructs a statically branded multiplexed application.
            #[inline(always)]
            #vis fn multiplexed<
                #g_brand: #l_env,
                #l_env,
                #( #lives: #l_env, )*
                'd: #l_env,
                #( #decls, )*
                #g_wire: #ns_fiber::dope::net::wire::Spec + #l_env,
                #g_id: #ns_fiber::Identity + #l_env,
                #g_target: #ns_core::manifold::task::Target + 'd,
            >(
                #brand
                #v_state: &#l_env #state_ty,
                #v_config: #ns_fiber::Config,
            ) -> impl #ns_core::manifold::task::app::Bind<
                    'd,
                    Target = #g_target,
                    Error = #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::PrepareError,
                    Runtime: #contract,
                >
                + #l_env
            where
                #( #clauses, )* #( #routes )*
            {
                #name {
                    #f_core: #core_id {
                        #core_i
                        #scheduled
                        #f_time: (),
                        #f_life: #ns_core::life::mode::Disabled,
                        #f_state: #v_state,
                        #f_mark: ::core::marker::PhantomData::<(
                            #g_id,
                            #g_wire,
                            #ns_fiber::manifold::runtime::Multi<#g_target>,
                            fn(&#g_brand ()) -> &#g_brand (),
                        )>,
                    },
                    #f_stamp: (),
                }
            }
        }
    };
    (app, dflat.ty)
}
