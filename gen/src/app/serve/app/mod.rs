use crate::{
    app::serve::{self, handle, layout},
    macros::tokens::local,
    runtime::paths::Paths,
};
mod finish;
mod h1;
mod surfaces;
mod tokens;

pub(super) fn build<'source, 'spec>(
    paths: &Paths,
    source: &'source serve::ProtocolPlan<'spec>,
    exec: &serve::Execution<'source, 'spec>,
    surface: Option<&handle::H1Plan<'source, 'spec>>,
    contract: &proc_macro2::TokenStream,
    shape: &handle::dispatch::Shape,
) -> proc_macro2::TokenStream {
    let [f_tasks, f_makers, f_ids] = ["tasks", "task_producers", "ids"].map(local);
    let symbols = &source.spec.state_generics;
    let [n_bind, v_brand, v_ids] = symbols.locals(["__binding", "brand", "ids"]);
    let [v_makers, v_cap] = symbols.locals(["producers", "task_capacity"]);
    let [v_maker, v_tasks] = symbols.locals(["task_producers", "tasks"]);
    let [l_env, g_brand] = source.spec.state_generics.lives(["env", "brand"]);
    let [g_target, g_id, g_store, g_wire] = source
        .spec
        .state_generics
        .idents(["__C", "__ID", "__P", "__W"]);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_fiber = &paths.fiber;
    use proc_macro2::TokenStream;
    use quote::{format_ident, quote};
    let spec = source.spec;
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
    let (tags, variants): (Vec<_>, Vec<_>) = tasks
        .iter()
        .map(|task| (task.tag(api), task.variant()))
        .unzip();
    let fixed_id = format_ident!("__{}Fixed", api);
    let fixed_fibers: Vec<_> = exec
        .fibers()
        .iter()
        .copied()
        .filter(|(_, fiber)| !fiber.stream)
        .collect();
    let fixed = finish::define(paths, generics, &fixed_fibers, &fixed_id);
    let dtree = layout::Tree::new(&exec.direct).ty(paths);
    let qtree = layout::Tree::new(&exec.queued).ty(paths);
    let ctor = format_ident!(
        "{}",
        crate::handler::names::encoded("__SarkConstructor", api)
            .to_string()
            .to_lowercase()
    );
    let app_def = &shape.generic_def;
    let app_use = &shape.generic_use;
    let output = quote! { #name #app_use };
    let prod_f = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { #f_makers: ( #( #makers, )* ), }
    };
    let prod_i = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { #f_makers: #v_makers, }
    };
    let store_f = if fibers == 0 {
        quote! {
            #[pin]
            #f_tasks:
                <#g_store as #ns_fiber::manifold::runtime::Storage>::DirectOnly<
                    #g_brand,
                    'd,
                    #dtree,
                >,
        }
    } else {
        quote! {
            #[pin]
            #f_tasks:
                <#g_store as #ns_fiber::manifold::runtime::Storage>::Tasks<
                    #g_brand,
                    'd,
                    #dtree,
                    #qtree,
                >,
        }
    };
    let store_i = if fibers == 0 {
        quote! {
            #f_tasks: <#g_store as #ns_fiber::manifold::runtime::Initial>::direct_only::<
                #dtree,
            >(
                #ns_fiber::dope_fiber::task::storage::fixed::quota::Limit::new(#v_cap),
                #v_brand,
            ),
        }
    } else {
        quote! {
            #f_tasks: <#g_store as #ns_fiber::manifold::runtime::Initial>::tasks::<
                #dtree,
                #qtree,
            >(
                #ns_fiber::dope_fiber::task::storage::fixed::quota::Limit::new(#v_cap),
                #v_brand,
            ),
        }
    };
    let ids_f = (fibers != 0).then(|| {
        quote! {
            #f_ids: <#g_store as #ns_fiber::manifold::runtime::Storage>::Resource<
                #ns_http::request::Seq<'d>,
            >,
        }
    });
    let ids_i = (fibers != 0).then(|| {
        quote! {
            #f_ids: <#g_store as
                #ns_fiber::manifold::runtime::Initial
            >::resource::<#ns_http::request::Seq<'d>>(),
        }
    });
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
            #prod_f
            #ids_f
        }
    };
    let tinit = if tasks.is_empty() {
        TokenStream::new()
    } else {
        quote! {
            #prod_i
            #store_i
            #ids_i
        }
    };
    let moves = if tasks.is_empty() {
        TokenStream::new()
    } else if makers.is_empty() {
        quote! { #f_tasks: #v_tasks, #ids_mv }
    } else {
        quote! { #f_tasks: #v_tasks, #f_makers: #v_maker, #ids_mv }
    };
    let prod_in = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { #v_makers: ( #( #makers, )* ), }
    };
    let prod_arg = if makers.is_empty() {
        TokenStream::new()
    } else {
        quote! { ( #( #makes, )* ), }
    };
    let tag_defs = tags.iter().map(|tag| {
        quote! {
            struct #tag<__TASK_ID: #ns_fiber::Identity>(
                ::core::marker::PhantomData<fn() -> __TASK_ID>,
            );
        }
    });
    let var_defs = variants
        .iter()
        .zip(tags.iter())
        .map(|(variant, tag)| {
            quote! {
                #variant(
                    #ns_fiber::dope_fiber::task::storage::Id<'d, #ns_fiber::dope_fiber::task::storage::scope::Domain<#g_brand, #tag<__TASK_ID>>>,
                )
            }
        })
        .collect::<Vec<_>>();
    let fvars = exec
        .fibers()
        .iter()
        .map(|pair| {
            let task = pair.0;
            let variant = task.variant();
            let tag = task.tag(api);
            quote! {
                #variant(
                    #ns_fiber::dope_fiber::task::storage::Id<'d, #ns_fiber::dope_fiber::task::storage::scope::Domain<#g_brand, #tag<__TASK_ID>>>,
                )
            }
        })
        .collect::<Vec<_>>();
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
            #[must_use = "dropping a task identity does not cancel or remove its fiber"]
            enum #identity<#g_brand, 'd, __TASK_ID: #ns_fiber::Identity> {
                #( #var_defs, )*
            }
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
            #[must_use = "dropping a task identity does not cancel or remove its fiber"]
            enum #fiber<#g_brand, 'd, __TASK_ID: #ns_fiber::Identity> {
                #( #fvars, )*
            }
        }
    };
    let bounds = &shape.base_bounds;
    let names = tokens::Names {
        generics,
        core: &core_id,
        name: &name,
        public_name: api,
        constructor_module: &ctor,
    };
    let extras = match surface {
        Some(plan) => h1::extras(
            paths,
            plan,
            source,
            exec,
            &names,
            surfaces::Binding::new(
                paths,
                source,
                &futures,
                &makers,
                bounds,
                (&moves, &ids_prep),
            ),
            &prod_arg,
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
        <
            #g_brand,
            #l_env,
            #( #lives, )*
            'd,
            #( #values, )*
            #g_id,
            #g_wire,
            #ns_fiber::manifold::runtime::Multi<#g_target>,
            #ns_core::life::mode::Disabled,
            (),
            #( #futures, )*
            #( #makers, )*
        >
    };
    let hot = &shape.multiplexed_generic_use;
    let bind = if fibers == 0 {
        TokenStream::new()
    } else {
        quote! {
            let (#v_brand, #v_tasks) = #v_tasks;
            let #v_tasks = #v_tasks.try_prepare(#n_bind.owners())?;
            let #v_tasks = #ns_fiber::dope_fiber::task::storage::scope::Owned::new(#v_tasks, #v_brand);
        }
    };
    let prep = quote! {
        #bind
        #ids_prep
    };
    let factory = tokens::Factory {
        name: &name,
        core: &core_id,
        protocol_generic: proto,
        unprepared: &cold,
        prepared: hot,
        bounds,
        core_moves: &core_mv,
        task_moves: &moves,
        prepare: &prep,
    }
    .build(paths, generics);
    let tags = quote! {
        #( #tag_defs )*
        #id_def
        #fdef
    };
    let sources = exec.fibers().iter().map(|(task, fiber)| {
        crate::app::spec::fiber_bound(paths, task.route(), state_ty, fiber.stream)
    });
    let slab = quote! { #( #clauses, )* #( #slabs )* #( #sources )* };
    let public = quote! { #( #clauses, )* #( #routes )* };
    let state_b = quote! { #( #lives: #l_env, )* };
    let state_p = quote! { #( #decls, )* };
    let future_g = quote! { #( #futures, )* };
    let maker_g = quote! { #( #makers, )* };
    tokens::build(
        paths,
        tokens::Prelude {
            definitions: &defs,
            task_tags: &tags,
            fixed: &fixed,
            factory: &factory,
        },
        names,
        tokens::Generics {
            app: app_def,
            usage: app_use,
            state_bounds: &state_b,
            state_parameters: &state_p,
            futures: &future_g,
            makers: &maker_g,
            build_return: &output,
        },
        tokens::Bounds {
            runtime: contract,
            slab: &slab,
            base: bounds,
            public: &public,
        },
        tokens::Fields {
            core: &core_f,
            task: &fields,
            public_methods: &methods,
            producer_argument: &prod_arg,
            producer_parameter: &prod_in,
            core_initializers: &core_i,
            task_initializers: &tinit,
            branded: !tasks.is_empty(),
        },
        spec,
    )
}
