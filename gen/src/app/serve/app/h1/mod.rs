use crate::{
    app::{
        serve::{
            self,
            app::{surfaces, tokens},
            duplex,
            handle::{self, router},
        },
        spec::Driver,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

mod lending;
mod runners;

pub(super) fn extras<'emit, 'source, 'spec>(
    paths: &Paths,
    plan: &handle::H1Plan<'source, 'spec>,
    source: &'source serve::ProtocolPlan<'spec>,
    exec: &'emit serve::Execution<'source, 'spec>,
    names: &tokens::Names<'emit>,
    binding: surfaces::Binding<'emit>,
    producer: &'emit proc_macro2::TokenStream,
) -> surfaces::Extras {
    let ctor = names.constructor_identity;
    let make = names.constructor_module;
    let [f_cache, f_gzip, f_time] = ["response_cache", "gzip", "timer"].map(local);
    let [f_life, f_state, f_mark] = ["lifecycle", "state", "marker"].map(local);
    let [f_core, f_stamp] = ["core", "stamp"].map(local);
    let symbols = &source.spec.state_generics;
    let [n_bind, n_clock] = symbols.locals(["__binding", "__clock"]);
    let [n_until, v_brand, v_config] = symbols.locals(["__deadline", "brand", "config"]);
    let [v_core, v_gzip, v_life] = symbols.locals(["core", "gzip", "lifecycle"]);
    let [v_obs, v_cache] = symbols.locals(["observer", "response_cache"]);
    let [v_retain, v_stamp, v_state] = symbols.locals(["retention", "stamp", "state"]);
    let [v_tasks] = symbols.locals(["tasks"]);
    let [l_wire, l_env] = source.spec.state_generics.lives(["__w", "env"]);
    let [g_d, g_o] = source.spec.state_generics.idents(["__D", "__O"]);
    let [g_brand, g_clock] = source.spec.state_generics.lives(["brand", "clock"]);
    let [g_id, g_mode, g_store, g_wire] = source
        .spec
        .state_generics
        .idents(["__ID", "__M", "__P", "__W"]);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;

    let branded = !exec.tasks().is_empty();
    let brand = branded
        .then(|| quote!(#v_brand: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,));
    let claim = branded.then(|| quote!(#v_brand,));
    let spec = source.spec;
    let vis = &spec.vis;
    let state_ty = spec.state.ty();
    let generics = &spec.state_generics;
    let lives = &generics.lifetimes;
    let decls = &generics.parameters;
    let values = &generics.arguments;
    let clauses = &generics.predicates;
    let routes = &spec.route_bounds;
    let inner = names.name;
    let core = names.core;
    let params = &binding.generic;
    let cold = &binding.cold_direct;
    let bound = &binding.bound_direct;
    let limits = binding.bounds;
    let moves = binding.task_moves;
    let prep = binding.task_prepare;
    let startup = branded.then(|| {
        quote! {
            let (#v_brand, #v_tasks) = #v_tasks;
            let #v_tasks = #v_tasks.try_prepare()
                .map_err(#ns_h1::server::listener::bind::Error::Allocation)?;
            let #v_tasks = #ns_fiber::dope_fiber::task::storage::scope::Owned::new(#v_tasks, #v_brand);
        }
    });
    let count = plan.cache_count;
    let pending = plan.continuations.definitions(paths);
    let duplex = plan.duplex.state();
    let input = duplex.input_type(paths);
    let config = duplex.config(paths);
    let stamp = duplex.stamp(quote!(#n_clock), quote!(#v_stamp));
    let (unpack, initial) = match duplex {
        duplex::State::Disabled => (quote!(), quote!(())),
        duplex::State::Enabled => (
            quote!(let (#v_config, #v_retain) = #v_config.into_parts();),
            quote!(#v_retain),
        ),
    };
    let decode = exec
        .fibers()
        .is_empty()
        .then(|| quote!(+ #ns_core::decode::Decode));
    let runners = if matches!(spec.driver, Driver::Present) {
        runners::Output::default()
    } else {
        runners::build(paths, spec, plan.duplex.state(), branded)
    };
    let runs = runners.definitions;
    let serve = runners.methods;
    let coding = router::coding(paths, spec);
    let gz_field = (count != 0).then(|| {
        quote! {
            #f_gzip: <#g_store as
                #ns_fiber::manifold::runtime::Storage
            >::Resource<#coding>,
        }
    });
    let gz_init = (count != 0).then(|| {
        quote! {
            #f_gzip: <#g_store as
                #ns_fiber::manifold::runtime::Initial
            >::resource::<#coding>(),
        }
    });
    let gz_move = (count != 0).then(|| quote!(#f_gzip: #v_gzip,));
    let make_gz = |error: proc_macro2::TokenStream| {
        (count != 0).then(|| {
            quote! {
                let #ns_fiber::manifold::runtime::Empty = #v_gzip;
                let #v_gzip = <#coding as #ns_http::payload::compress::Compression>::try_new()
                    .map_err(#error)?;
            }
        })
    };
    let fields = quote! {
        #f_cache: [
            ::core::option::Option<#ns_h1::wire::cache::Entry>;
            #count
        ],
        #gz_field
    };
    let init = quote! {
        #f_cache: [
            const { ::core::option::Option::None };
            #count
        ],
        #gz_init
    };
    let owned = quote! {
        #f_cache: #v_cache,
        #gz_move
    };
    let multi_gz = make_gz(quote!(#ns_core::manifold::task::app::BindError::Allocation));
    let bind_gz = make_gz(quote!(#ns_h1::server::listener::bind::Error::Allocation));
    let prepare = quote! {
        #multi_gz
    };
    let wire = quote! {
        <#input as #ns_fiber::dope::net::link::pool::recv::input::Mode<#g_wire>>::Reserve:
            #ns_fiber::dope::net::link::pool::recv::reserve::Choice<
                #g_wire,
                <#input as #ns_fiber::dope::net::link::pool::recv::input::Mode<#g_wire>>::Ingress,
            >,
    };
    let ctor_g = quote! {
        <
            #l_wire,
            #g_brand: #l_env,
            #l_env,
            #( #lives: #l_env, )*
            'd: #l_wire + #l_env,
            #( #decls, )*
            #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
            #g_id: #ns_fiber::Identity + #l_env,
        >
    };
    let ctor_out = quote! {
        impl #ns_h1::server::listener::bind::Bind<
            #l_wire,
            'd,
            #g_id,
            Wire = #g_wire,
            Input = #input,
            Error = #ns_h1::server::listener::bind::Error,
        >
        #decode
        + #l_env
    };
    let ctor_b = quote! {
        #( #clauses, )*
        #( #routes )*
        #state_ty: #ns_h1::server::listener::failure::Policy<#g_wire>,
        #wire
    };
    let bind_key = match branded {
        true => quote!(#v_brand),
        false => quote!(_),
    };
    let binding = quote! {
        impl #params #ns_h1::server::listener::bind::Bind<#l_wire, 'd, #g_id>
            for #inner #cold
        where
            #limits
            #state_ty: #ns_h1::server::listener::failure::Policy<#g_wire>,
            #g_mode: #ns_core::life::sequence::Mode<
                #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                Retention: #ns_h1::server::task::turn::Mode,
                TransportTag: 'd,
            >,
            #wire
        {
            type Wire = #g_wire;
            type Input = #input;
            type Error = #ns_h1::server::listener::bind::Error;
            type Application<#g_clock, #g_d> = #inner #bound
            where
                'd: #g_clock,
                #g_d: #ns_h1::server::deadline::policies::Policy<'d, #g_id>;

            fn try_bind<#g_clock, #g_d>(
                self,
                #n_bind: #ns_fiber::dope::core::driver::Binding<'d>,
                #n_until: #g_d,
                #n_clock: &#g_clock #ns_h1::server::date::Clock,
            ) -> ::core::result::Result<Self::Application<#g_clock, #g_d>, Self::Error>
            where
                'd: #g_clock,
                #g_d: #ns_h1::server::deadline::policies::Policy<'d, #g_id>,
            {
                let #inner { #f_core: #v_core, #f_stamp: #v_stamp, .. } = self;
                let #core {
                    #owned
                    #moves
                    #f_time: #ns_fiber::manifold::runtime::Empty,
                    #f_life: #v_life,
                    #f_state: #v_state,
                    #f_mark: ::core::marker::PhantomData,
                } = #v_core;
                #startup
                #prep
                let #v_life =
                    #ns_h1::server::deadline::policies::Policy::bind(
                        #n_until,
                        #v_life,
                        #n_bind.owners(),
                    ).map_err(#ns_h1::server::listener::bind::Error::Deadline)?;
                #bind_gz
                ::core::result::Result::Ok(#inner {
                    #f_core: #core {
                        #owned
                        #moves
                        #f_time: #n_bind.timer(),
                        #f_life: #v_life,
                        #f_state: #v_state,
                        #f_mark: ::core::marker::PhantomData,
                    },
                    #f_stamp: #stamp,
                })
            }
        }
    };
    let create = quote! {
        #unpack
        #make(
            #claim
            #v_state,
            ::core::marker::PhantomData::<(
                #ctor<#g_id>, #g_wire, #ns_fiber::manifold::runtime::Cold,
            )>,
            #v_config,
            #ns_core::life::mode::Disabled,
            #initial,
            #producer
        )
    };
    let lending = lending::build(paths, source, plan, &create, &ctor_b, branded);
    let defs = quote! {
        #pending
        #binding
        #lending
        #runs
    };
    let methods = quote! {
        #vis fn new #ctor_g(
            #brand
            #v_state: &#l_env #state_ty,
            #v_config: #config,
        ) -> #ctor_out
        where
            #ctor_b
        {
            #create
        }

        #[doc(hidden)]
        #vis fn __scoped_new #ctor_g(
            #bind_key: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,
            #v_state: &#l_env #state_ty,
            #v_config: #config,
        ) -> #ctor_out
        where
            #ctor_b
        {
            Self::new::<#(#values,)* #g_wire, #g_id>(#claim #v_state, #v_config)
        }

        #serve

        /// Constructs an observed H1 application with static dispatch.
        #vis fn with_observer<
            #l_wire,
            #g_brand: #l_env,
            #l_env,
            #( #lives: #l_env, )*
            'd: #l_wire + #l_env,
            #( #decls, )*
            #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
            #g_id: #ns_fiber::Identity + #l_env,
            #g_o: #ns_core::life::Observer<
                #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                ::core::primitive::u64,
            > + #l_env,
        >(
            #brand
            #v_state: &#l_env #state_ty,
            #v_config: #config,
            #v_obs: #g_o,
        ) -> #ctor_out
        where
            #ctor_b
        {
            #unpack
            #make(
                #claim
                #v_state,
                ::core::marker::PhantomData::<(
                    #ctor<#g_id>, #g_wire, #ns_fiber::manifold::runtime::Cold,
                )>,
                #v_config,
                #ns_core::life::mode::Observed::new(#v_obs),
                #initial,
                #producer
            )
        }
    };

    surfaces::Extras {
        definitions: defs,
        core_fields: fields,
        core_moves: owned,
        public_methods: methods,
        core_initializers: init,
        multiplexed_prepare: prepare,
    }
}
