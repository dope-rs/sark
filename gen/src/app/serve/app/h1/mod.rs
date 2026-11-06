use crate::{
    app::{
        serve::{
            self,
            app::{surfaces, tokens},
            duplex,
            handle::{self, decode, router},
        },
        spec::Driver,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

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
    let [v_prep, v_driver, v_app, v_permit, v_wait] =
        symbols.locals(["prepared", "driver", "app", "permit", "wait"]);
    let [l_wire, l_env] = symbols.lives(["__w", "env"]);
    let [l_n, l_p, l_s, l_issued, l_life, l_watch] =
        symbols.lives(["__n", "__p", "__s", "__issued", "__life", "__watch"]);
    let [g_date, g_prof, g_recv, g_owner, g_shutdown] =
        symbols.idents(["__DATE", "__PROFILE", "__RECEIVER", "__OWNER", "__SHUTDOWN"]);
    let [g_d, g_o] = symbols.idents(["__D", "__O"]);
    let [g_brand, g_clock] = symbols.lives(["brand", "clock"]);
    let [g_id, g_mode, g_store, g_wire] = symbols.idents(["__ID", "__M", "__P", "__W"]);
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
    let lives = &symbols.lifetimes;
    let decls = &symbols.parameters;
    let clauses = &symbols.predicates;
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
    let egress_bound = match duplex {
        duplex::State::Enabled => quote!(#ns_http::o3::buffer::resident::bound::Unbounded),
        duplex::State::Disabled => decode::egress(
            paths,
            spec,
            &source.entries,
            &syn::Lifetime::new("'d", proc_macro2::Span::call_site()),
        ),
    };
    let egress = |tag: proc_macro2::TokenStream| {
        quote! {
            #ns_fiber::dope::net::link::egress::data::Bound<
                #ns_fiber::dope::net::link::egress::data::compose::Either<
                    #ns_h1::server::payload::Body<'d, #tag>,
                    #ns_h1::server::payload::Stream<'d, #tag>,
                >,
                #egress_bound,
            >
        }
    };
    let plain_egress = egress(quote!(()));
    let bind_egress = egress(quote! {
        <#g_mode as #ns_core::life::sequence::Mode<
            #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
        >>::TransportTag
    });
    let upgrade = source.entries.iter().any(|route| route.entry.upgrade);
    let stamp = duplex.stamp(
        quote!(#n_clock),
        quote!(#v_stamp),
        upgrade.then(|| quote!(::std::vec::Vec::new())),
    );
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
            >::Protocol<(), #coding>,
        }
    });
    let gz_init = (count != 0).then(|| {
        quote! {
            #f_gzip: <#g_store as
                #ns_fiber::manifold::runtime::Initial
            >::protocol::<(), #coding>(()),
        }
    });
    let gz_move = (count != 0).then(|| quote!(#f_gzip: #v_gzip,));
    let cache = quote!([::core::option::Option<#ns_h1::wire::cache::Entry>; #count]);
    let fields = quote! {
        #f_cache: <#g_store as #ns_fiber::manifold::runtime::Storage>::Protocol<#cache, #cache>,
        #gz_field
    };
    let init = quote! {
        #f_cache: <#g_store as #ns_fiber::manifold::runtime::Initial>::protocol::<#cache, #cache>(
            [const { ::core::option::Option::None }; #count],
        ),
        #gz_init
    };
    let owned = quote! {
        #f_cache: #v_cache,
        #gz_move
    };
    let bind_gz = (count != 0).then(|| {
        quote! {
            let () = #v_gzip;
            let #v_gzip = <#coding as #ns_http::payload::compress::Compression>::try_new()
                .map_err(#ns_h1::server::listener::bind::Error::Allocation)?;
        }
    });
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
    let ctor = |egress: &proc_macro2::TokenStream| {
        quote! {
            impl #ns_h1::server::listener::bind::Bind<
                #l_wire,
                'd,
                #g_id,
                Wire = #g_wire,
                Input = #input,
                Egress = #egress,
                Error = #ns_h1::server::listener::bind::Error,
            >
            #decode
            + #l_env
        }
    };
    let ctor_out = ctor(&plain_egress);
    let observed_ctor = ctor(&egress(quote!(#ns_core::life::sequence::observed::Tag)));
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
            type Egress = #bind_egress;
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
                    #f_time: (),
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
                fn() -> #g_id, #g_wire, #ns_fiber::manifold::runtime::Cold,
            )>,
            #v_config,
            #ns_core::life::mode::Disabled,
            #initial,
            #producer
        )
    };
    let prepared = quote! {
        #ns_h1::server::run::Prepared<
            #l_n, #l_p, #l_s, #l_wire, 'd, #g_id, #g_date,
            #ns_fiber::dope::manifold::app::env::Bundle<
                #ns_fiber::dope::net::tcp::Tcp, #g_wire, #g_prof,
            >,
            #g_recv, #plain_egress, #input, #g_d,
        >
    };
    let defs = quote! {
        #pending
        #binding
        #runs
    };
    let methods = quote! {
        /// Builds and runs this application while consuming its provider permit.
        #vis fn run_prepared<
            #l_env, #g_brand: #l_env, #l_wire, #l_n, #l_p, #l_s,
            #l_issued, #l_life: #l_issued, #l_watch,
            #( #lives: #l_env, )* 'd: #l_wire + #l_env + #l_life + #l_watch,
            #( #decls, )*
            #g_id: #ns_fiber::Identity + 'd,
            #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
            #g_date: #ns_fiber::Identity,
            #g_prof, #g_recv, #g_d, #g_owner, #g_shutdown,
        >(
            #bind_key: #ns_fiber::dope_fiber::task::storage::scope::Brand<#g_brand>,
            #v_state: ::core::pin::Pin<&#l_env #state_ty>,
            #v_config: #config,
            #v_prep: #prepared,
            #v_wait: #ns_h1::server::run::Wait<#l_watch, 'd>,
            #v_permit: #ns_fiber::dope::runtime::topology::lending::Permit<
                #l_issued, #l_life, 'd, #g_owner, #g_shutdown,
            >,
        ) -> #ns_fiber::dope::runtime::topology::lending::Outcome<
            #l_issued, #l_life, 'd, #g_owner, #g_shutdown,
            #ns_fiber::dope::manifold::BuildError<#ns_h1::server::listener::bind::Error>,
            #ns_h1::server::run::Pending<
                #l_watch, 'd,
                #ns_fiber::dope::runtime::shutdown::Requested<#g_shutdown>,
            >,
        >
        where
            #ctor_b
            #g_prof: #ns_fiber::dope::core::driver::settings::Profile
                + #ns_fiber::dope::manifold::timing::Policy,
            #g_recv: #ns_fiber::dope::net::link::pool::change::Prepare<
                #l_wire, 'd, #g_id, #g_wire,
            >,
            #g_d: #ns_h1::server::deadline::policies::Policy<'d, #g_id>,
            #plain_egress: #ns_fiber::dope::net::link::egress::data::raw::Payload<'d> + 'd,
            #prepared: #l_env,
            #g_owner: #ns_fiber::dope::runtime::topology::lending::raw::Resume<'d>,
            #g_shutdown: #ns_fiber::dope::runtime::executor::Startup,
        {
            #v_permit.try_run(
                |mut #v_driver| {
                    let #v_state = #v_state.get_ref();
                    let #v_app = { #create };
                    #v_prep.build(#v_app, &mut #v_driver)
                        .map(|#v_app| (#v_app, ()))
                },
                |(), #v_app| #ns_h1::server::run::run(#v_wait, #v_app),
            )
        }

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
        ) -> #observed_ctor
        where
            #ctor_b
        {
            #unpack
            #make(
                #claim
                #v_state,
                ::core::marker::PhantomData::<(
                    fn() -> #g_id, #g_wire, #ns_fiber::manifold::runtime::Cold,
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
    }
}
