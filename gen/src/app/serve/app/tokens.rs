use crate::{macros::tokens::local, runtime::paths::Paths};

pub(super) struct Prelude<'a> {
    pub definitions: &'a proc_macro2::TokenStream,
    pub task_tags: &'a proc_macro2::TokenStream,
    pub factory: &'a proc_macro2::TokenStream,
}

pub(super) struct Names<'a> {
    pub generics: &'a crate::types::Generics,
    pub core: &'a syn::Ident,
    pub name: &'a syn::Ident,
    pub public_name: &'a syn::Ident,
}

pub(super) struct Generics<'a> {
    pub core_parameters: &'a proc_macro2::TokenStream,
    pub state_bounds: &'a proc_macro2::TokenStream,
    pub state_parameters: &'a proc_macro2::TokenStream,
}

pub(super) struct Bounds<'a> {
    pub runtime: &'a proc_macro2::TokenStream,
    pub slab: &'a proc_macro2::TokenStream,
    pub public: &'a proc_macro2::TokenStream,
}

pub(super) struct Fields<'a> {
    pub core: &'a proc_macro2::TokenStream,
    pub task: &'a proc_macro2::TokenStream,
    pub public_methods: &'a proc_macro2::TokenStream,
    pub core_initializers: &'a proc_macro2::TokenStream,
    pub task_initializers: &'a proc_macro2::TokenStream,
    pub branded: bool,
}

pub(super) fn build<'a>(
    paths: &Paths,
    prelude: Prelude<'a>,
    names: Names<'a>,
    generics: Generics<'a>,
    bounds: Bounds<'a>,
    fields: Fields<'a>,
    spec: &'a crate::app::spec::Spec,
) -> proc_macro2::TokenStream {
    let state = spec.state.ty();
    let vis = &spec.vis;
    let [f_time, f_life, f_state] = ["timer", "lifecycle", "state"].map(local);
    let [f_mark, f_core, f_stamp] = ["marker", "core", "stamp"].map(local);
    let [v_brand, v_config] = names.generics.locals(["brand", "config"]);
    let v_state = names.generics.local("state");
    let g_part = names.generics.ident("__CORE");
    let [l_env, g_brand] = names.generics.lives(["env", "brand"]);
    let [g_target, g_id, g_mode, g_store, g_stamp, g_wire] = names
        .generics
        .idents(["__C", "__ID", "__M", "__P", "__S", "__W"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let Prelude {
        definitions: defs,
        task_tags: tags,
        factory,
    } = prelude;
    let Names {
        generics: _,
        core,
        name,
        public_name: api,
    } = names;
    let Generics {
        core_parameters,
        state_bounds: states,
        state_parameters: state_p,
    } = generics;
    let Bounds {
        runtime,
        slab,
        public,
    } = bounds;
    let Fields {
        core: core_set,
        task: task_set,
        public_methods: methods,
        core_initializers: cinit,
        task_initializers: tinit,
        branded,
    } = fields;
    let brand = branded.then(
        || quote::quote!(#v_brand: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,),
    );
    quote::quote! {
            #defs
            #tags
            #factory

            #[#ns_fiber::pin_project::pin_project(!Unpin)]
            struct #core #core_parameters
            where
                #slab
            {
                #core_set
                #task_set
                #f_time: <#g_store as
                    #ns_fiber::manifold::runtime::Storage
                >::Resource<#ns_fiber::driver::schedule::clock::timer::Timer<'d>>,
                #f_life: #g_mode,
                #f_state: &#l_env #state,
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
                    #states
                    'd: #l_env,
                    #state_p
                    #g_wire: #ns_fiber::dope::net::wire::Spec + #l_env,
                    #g_id: #ns_fiber::Identity + #l_env,
                    #g_target: #ns_core::manifold::task::Target + 'd,
                >(
                    #brand
                    #v_state: &#l_env #state,
                    #v_config: #ns_fiber::Config,
                ) -> impl #ns_core::manifold::task::app::Bind<
                        'd,
                        Target = #g_target,
                        Error = #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::PrepareError,
                        Runtime: #runtime,
                    >
                    + #l_env
                where
                    #public
                {
                    #name {
                        #f_core: #core {
                            #cinit
                            #tinit
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

    }
}
