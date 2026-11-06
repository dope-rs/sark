use crate::{macros::tokens::local, runtime::paths::Paths};

pub(super) struct Factory<'a> {
    pub name: &'a syn::Ident,
    pub core: &'a syn::Ident,
    pub protocol_generic: &'a proc_macro2::TokenStream,
    pub unprepared: &'a proc_macro2::TokenStream,
    pub prepared: &'a proc_macro2::TokenStream,
    pub bounds: &'a proc_macro2::TokenStream,
    pub core_moves: &'a proc_macro2::TokenStream,
    pub task_moves: &'a proc_macro2::TokenStream,
    pub prepare: &'a proc_macro2::TokenStream,
}

impl Factory<'_> {
    pub(super) fn build(
        self,
        paths: &Paths,
        generics: &crate::types::Generics,
    ) -> proc_macro2::TokenStream {
        let [f_time, f_life, f_state] = ["timer", "lifecycle", "state"].map(local);
        let [f_mark, f_core, f_stamp] = ["marker", "core", "stamp"].map(local);
        let [n_bind, v_core] = generics.locals(["__binding", "core"]);
        let [v_life, v_stamp, v_state] = generics.locals(["lifecycle", "stamp", "state"]);
        let g_target = generics.ident("__C");
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let Self {
            name,
            core,
            protocol_generic: proto,
            unprepared: cold,
            prepared,
            bounds,
            core_moves: core_set,
            task_moves: task_set,
            prepare,
        } = self;
        let error = quote::quote! {
            #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::PrepareError
        };
        quote::quote! {
            impl #proto
                #ns_core::manifold::task::app::Bind<'d>
                for #name #cold
            where
                #bounds
            {
                type Target = #g_target;
                type Runtime = #name #prepared;
                type Error = #error;

                fn try_bind(
                    self,
                    #n_bind: #ns_fiber::dope::core::driver::Binding<'d>,
                ) -> ::core::result::Result<Self::Runtime, Self::Error> {
                    let #name { #f_core: #v_core, #f_stamp: #v_stamp, .. } = self;
                    let #core {
                        #core_set
                        #task_set
                        #f_time: (),
                        #f_life: #v_life,
                        #f_state: #v_state,
                        #f_mark: ::core::marker::PhantomData,
                    } = #v_core;
                    #prepare
                    ::core::result::Result::Ok(#name {
                        #f_core: #core {
                            #core_set
                            #task_set
                            #f_time: #n_bind.timer(),
                            #f_life: #v_life,
                            #f_state: #v_state,
                            #f_mark: ::core::marker::PhantomData,
                        },
                        #f_stamp: #v_stamp,
                    })
                }
            }
        }
    }
}

pub(super) struct Prelude<'a> {
    pub definitions: &'a proc_macro2::TokenStream,
    pub task_tags: &'a proc_macro2::TokenStream,
    pub fixed: &'a proc_macro2::TokenStream,
    pub factory: &'a proc_macro2::TokenStream,
}

pub(super) struct Names<'a> {
    pub generics: &'a crate::types::Generics,
    pub core: &'a syn::Ident,
    pub name: &'a syn::Ident,
    pub public_name: &'a syn::Ident,
    pub constructor_module: &'a syn::Ident,
}

pub(super) struct Generics<'a> {
    pub app: &'a proc_macro2::TokenStream,
    pub usage: &'a proc_macro2::TokenStream,
    pub state_bounds: &'a proc_macro2::TokenStream,
    pub state_parameters: &'a proc_macro2::TokenStream,
    pub futures: &'a proc_macro2::TokenStream,
    pub makers: &'a proc_macro2::TokenStream,
    pub build_return: &'a proc_macro2::TokenStream,
}

pub(super) struct Bounds<'a> {
    pub runtime: &'a proc_macro2::TokenStream,
    pub slab: &'a proc_macro2::TokenStream,
    pub base: &'a proc_macro2::TokenStream,
    pub public: &'a proc_macro2::TokenStream,
}

pub(super) struct Fields<'a> {
    pub core: &'a proc_macro2::TokenStream,
    pub task: &'a proc_macro2::TokenStream,
    pub public_methods: &'a proc_macro2::TokenStream,
    pub producer_argument: &'a proc_macro2::TokenStream,
    pub producer_parameter: &'a proc_macro2::TokenStream,
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
    let [v_life, v_stamp] = names.generics.locals(["lifecycle", "stamp"]);
    let [v_state, v_cap] = names.generics.locals(["state", "task_capacity"]);
    let [v_types] = names.generics.locals(["types"]);
    let [l_env, g_brand] = names.generics.lives(["env", "brand"]);
    let [g_target, g_id, g_mode, g_store, g_stamp, g_wire] = names
        .generics
        .idents(["__C", "__ID", "__M", "__P", "__S", "__W"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let Prelude {
        definitions: defs,
        task_tags: tags,
        fixed,
        factory,
    } = prelude;
    let Names {
        generics: _,
        core,
        name,
        public_name: api,
        constructor_module: ctor,
    } = names;
    let Generics {
        app,
        usage,
        state_bounds: states,
        state_parameters: state_p,
        futures,
        makers,
        build_return: built,
    } = generics;
    let Bounds {
        runtime,
        slab,
        base,
        public,
    } = bounds;
    let Fields {
        core: core_set,
        task: task_set,
        public_methods: methods,
        producer_argument: prod_arg,
        producer_parameter: prod_in,
        core_initializers: cinit,
        task_initializers: tinit,
        branded,
    } = fields;
    let brand = branded.then(
        || quote::quote!(#v_brand: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,),
    );
    let claim = branded.then(|| quote::quote!(#v_brand,));
    quote::quote! {
            #defs
            #tags
            #fixed
            #factory

            #[#ns_fiber::pin_project::pin_project(!Unpin)]
            struct #core #app
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
                    (#g_id, #g_wire, #g_store, #g_stamp, fn(&#g_brand ()) -> &#g_brand ()),
                >,
            }

            #[#ns_fiber::pin_project::pin_project]
            struct #name #app
            where
                #slab
            {
                #[pin]
                #f_core: #core #usage,
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
                    #ctor(
                        #claim
                        #v_state,
                        ::core::marker::PhantomData::<(
                            fn() -> #g_id,
                            #g_wire,
                            #ns_fiber::manifold::runtime::Multi<#g_target>,
                        )>,
                        #v_config,
                        #ns_core::life::mode::Disabled,
                        (),
                        #prod_arg
                    )
                }
            }

            #[inline(always)]
            fn #ctor<
                    #g_brand: #l_env,
                    #l_env,
                    #states
                    'd: #l_env,
                    #state_p
                    #g_id: #ns_fiber::Identity,
                    #g_wire: #ns_fiber::dope::net::wire::Spec + #l_env,
                    #g_store: #ns_fiber::manifold::runtime::Initial,
                    #g_mode,
                    #g_stamp,
                    #futures
                    #makers
                >(
                    #brand
                    #v_state: &#l_env #state,
                    #v_types: ::core::marker::PhantomData<(fn() -> #g_id, #g_wire, #g_store)>,
                    #v_config: #ns_fiber::Config,
                    #v_life: #g_mode,
                    #v_stamp: #g_stamp,
                    #prod_in
                ) -> #built
                where
                    #base
                {
                    let #v_cap = #v_config.capacity();
                    let ::core::marker::PhantomData = #v_types;
                    #name {
                        #f_core: #core {
                            #cinit
                            #tinit
                            #f_time: <#g_store as
                                #ns_fiber::manifold::runtime::Initial
                            >::resource::<#ns_fiber::driver::schedule::clock::timer::Timer<'d>>(),
                            #f_life: #v_life,
                            #f_state: #v_state,
                            #f_mark: ::core::marker::PhantomData,
                        },
                        #f_stamp: #v_stamp,
                    }
            }
    }
}
