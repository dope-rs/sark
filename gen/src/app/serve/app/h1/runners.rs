use crate::{runtime::paths::Paths, types};

#[derive(Clone, Copy)]
enum Wire {
    Identity,
    Generic,
}

#[derive(Default)]
pub(super) struct Output {
    pub(super) definitions: proc_macro2::TokenStream,
    pub(super) methods: proc_macro2::TokenStream,
}

pub(super) fn build(
    paths: &Paths,
    spec: &crate::app::spec::Spec,
    duplex: duplex::State,
    branded: bool,
) -> Output {
    let methods = [Wire::Identity, Wire::Generic].map(|wire| method(paths, spec, duplex, wire));
    let defs =
        [Wire::Identity, Wire::Generic].map(|wire| definitions(paths, spec, duplex, wire, branded));
    Output {
        definitions: quote::quote! { #( #defs )* },
        methods: quote::quote! { #( #methods )* },
    }
}

fn definitions(
    paths: &Paths,
    spec: &crate::app::spec::Spec,
    duplex: duplex::State,
    wire: Wire,
    branded: bool,
) -> proc_macro2::TokenStream {
    let vis = &spec.vis;
    let name = &spec.name;
    let generics = &spec.state_generics;
    let state = spec.state.ty();
    let [n_brand, v_app, v_config] = generics.locals(["__brand", "app", "config"]);
    let [v_error, v_finish, v_plan] = generics.locals(["error", "finish", "plan"]);
    let [v_root, v_stage, v_state] = generics.locals(["root", "stage", "state"]);
    let [v_wait] = generics.locals(["wait"]);
    let [v_wire, v_worker] = generics.locals(["wire", "worker"]);
    let [l_driver, l_store, l_own, l_work, l_finish] =
        generics.lives(["driver", "storage", "owner", "worker", "__finish"]);
    let [g_bind, g_date, g_list, g_prof, g_q, g_w] = generics.idents([
        "__BIND",
        "__DATE_ID",
        "__LISTENER_ID",
        "__PROFILE",
        "__Q",
        "__WIRE",
    ]);
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let lives = &generics.lifetimes;
    let decls = &generics.parameters;
    let values = &generics.arguments;
    let clauses = &generics.predicates;
    let claim = branded.then(|| quote::quote!(#n_brand,));
    let runner = runner(name, wire);
    let params = wire_params(wire, generics);
    let ty = wire_type(paths, wire, generics);
    let bounds = wire_bounds(paths, wire, generics);
    let failure = wire_error(wire, generics);
    let binding = match wire {
        Wire::Identity => quote::quote! { #ty },
        Wire::Generic => quote::quote! { #g_bind },
    };
    let storage = match wire {
        Wire::Identity => quote::quote! { () },
        Wire::Generic => quote::quote! {
            #ns_fiber::dope::core::driver::storage::Value<
                #ns_fiber::dope::net::wire::domain::Owner<#g_w, #g_list>,
            >
        },
    };
    let endpoint = match wire {
        Wire::Identity => quote::quote! {
            let #v_wire = #ns_fiber::dope::net::wire::domain::Local::<
                #ty, #g_list,
            >::new(#v_wire);
        },
        Wire::Generic => quote::quote! {
            let #v_wire = #v_stage.storage().0.bind(#v_wire).map_err(|#v_error| {
                #ns_h1::server::run::Failure::Install(
                    #ns_fiber::dope::core::BranchError::First(#v_error),
                )
            })?;
        },
    };
    let config = duplex.config(paths);
    let input = duplex.input_type(paths);
    let run = quote::quote! {
        #v_plan.install(#v_stage, #v_wire, #v_app, |#v_root, #v_wait, #v_stage| {
            #ns_h1::server::run::session(
                #v_stage.with_app(#v_finish, #v_root, |#v_app| #ns_h1::server::run::run(#v_wait, #v_app)),
            )
        }).map_err(|#v_error| #ns_h1::server::run::Failure::Install(
            #ns_fiber::dope::core::BranchError::Second(#v_error),
        ))?
    };
    let worker = quote::quote! {
        #ns_fiber::dope::runtime::topology::Worker<
            #l_work,
            #l_own,
            #l_store,
            #l_driver,
            #ns_fiber::dope::core::driver::storage::With<
                #storage,
                #ns_h1::server::date::Clock,
            >,
            #ns_h1::server::run::Plan<
                #g_list,
                #g_date,
                #g_prof,
                #input,
            >,
            #g_q,
        >
    };

    quote::quote! {
                #vis struct #runner<#l_finish, #( #lives, )* #( #decls, )* #params>
                where
                    #( #clauses, )*
                {
                    finish: &#l_finish mut #ns_fiber::dope::runtime::executor::session::Completion,
                    state: #state,
                    config: #config,
                    wire: #binding,
                    marker: ::core::marker::PhantomData<fn() -> #ty>,
                }

                impl<#l_finish, #l_work, #l_own, #l_store, #l_driver,

                    #( #lives, )*
                    #( #decls, )*
                    #params
                    #g_list: #ns_fiber::Identity,
                    #g_date: #ns_fiber::Identity,
                    #g_prof,
                    #g_q,
                > #ns_fiber::dope::runtime::topology::ops::Operation<#l_driver, #worker> for #runner<#l_finish, #( #lives, )* #( #values, )* #params>
                where
                    #( #clauses, )*
                    #bounds
                    #state: #ns_h1::server::listener::failure::Policy<#ty>,
                    #g_prof: #ns_fiber::dope::core::driver::settings::Profile
                        + #ns_fiber::dope::manifold::timing::Policy,
                    #g_q: #ns_fiber::dope::runtime::executor::Startup,
    {
                    type Output = ::core::result::Result<
                        #ns_fiber::dope::runtime::shutdown::Requested<#g_q>,
                        #ns_h1::server::run::Failure<
                            #l_finish,
                            #ns_fiber::dope::core::BranchError<
                                #failure,
                                #ns_fiber::dope::manifold::BuildError<
                                    #ns_h1::server::listener::bind::Error,
                                >,
                            >,
                            #ns_fiber::dope::runtime::shutdown::Requested<#g_q>,
                        >,
                    >;

                    fn run(
                        self,
                        #v_worker: #worker,
                    ) -> Self::Output
                    where Self: #l_driver {
                        let Self {
                            finish: #v_finish,
                            state: #v_state,
                            config: #v_config,
                            wire: #v_wire,
                            marker: _,
                        } = self;
                        #ns_fiber::dope_fiber::task::storage::scope::Brand::with(|#n_brand| {
                        #v_worker.lend(|#v_plan, #v_stage| {
                            #endpoint
                            let #v_app = #name::new::<#( #values, )* #ty, #g_list>(
                                #claim
                                &#v_state,
                                #v_config,
                            );
                            #run
                        })
                        })
                    }
                }
            }
}

fn method(
    paths: &Paths,
    spec: &crate::app::spec::Spec,
    duplex: duplex::State,
    wire: Wire,
) -> proc_macro2::TokenStream {
    let vis = &spec.vis;
    let app = &spec.name;
    let generics = &spec.state_generics;
    let state = spec.state.ty();
    let [v_config, v_ctx, v_error] = generics.locals(["config", "context", "error"]);
    let [v_finish, v_op, v_result] = generics.locals(["finish", "op", "outcome"]);
    let [v_owner, v_wait, v_plan] = generics.locals(["owner", "pending", "plan"]);
    let [v_prep, v_report, v_server] = generics.locals(["prepared", "report", "server"]);
    let [v_state, v_store, v_wire] = generics.locals(["state", "storage", "wire"]);
    let l_owner = generics.lifetime("__owner");
    let [g_ctx, g_date, g_done, g_list, g_prof] = generics.idents([
        "__CONTEXT",
        "__DATE_ID",
        "__DONE",
        "__LISTENER_ID",
        "__PROFILE",
    ]);
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let lives = &generics.lifetimes;
    let decls = &generics.parameters;
    let values = &generics.arguments;
    let clauses = &generics.predicates;
    let runner = runner(app, wire);
    let config = duplex.config(paths);
    let (name, docs) = identity(wire);
    let params = wire_params(wire, generics);
    let arg = wire_arg(wire, generics);
    let value = wire_value(paths, wire, generics);
    let ty = wire_type(paths, wire, generics);
    let bounds = wire_bounds(paths, wire, generics);
    let error = error(paths, app, generics, duplex, wire);
    let plan = quote::quote! { #v_server };
    let plan = match duplex {
        duplex::State::Enabled => quote::quote!((#plan).gated_scoped()),
        duplex::State::Disabled => plan,
    };
    let storage = match wire {
        Wire::Identity => quote::quote! { let #v_store = (); },
        Wire::Generic => quote::quote! {
            let #v_owner = match #v_plan.owner::<#ty>() {
                ::core::result::Result::Ok(#v_owner) => #v_owner,
                ::core::result::Result::Err(#v_error) => return ::core::result::Result::Err(#ns_fiber::dope::core::BranchError::First(
                    #ns_h1::server::run::prepare::Error::Resource(#v_error),
                )),
            };
            let #v_store = #ns_fiber::dope::core::driver::storage::Value::new(#v_owner);
        },
    };
    let constructor = match wire {
        Wire::Identity => quote::quote! { #runner },
        Wire::Generic => quote::quote! { #runner::<#( #values, )* #ty, _> },
    };

    quote::quote! {
        #[doc = #docs]
        #vis fn #name<
            #( #lives, )*
            #( #decls, )*
            #params
            #g_list: #ns_fiber::Identity,
            #g_date: #ns_fiber::Identity,
            #g_prof,
            #g_ctx,
            #g_done,
        >(
            #v_server: #ns_h1::server::run::Plan<
                #g_list,
                #g_date,
                #g_prof,
            >,
            #arg
            #v_ctx: #g_ctx,
            #v_state: #state,
            #v_config: #config,
            #v_report: impl #ns_fiber::dope::core::driver::life::recovery::Observe,
            #v_finish: impl for<#l_owner> ::core::ops::FnOnce(::core::result::Result<
                #ns_fiber::dope::runtime::shutdown::Requested<#g_ctx::Shutdown>, #error,
            >) -> #g_done,
        ) -> ::core::result::Result<#g_done, #ns_fiber::dope::core::BranchError<
            #ns_h1::server::run::prepare::Error,
            #ns_fiber::dope::core::driver::settings::errors::Claim,
        >>
        where
            #( #clauses, )*
            #g_prof: #ns_fiber::dope::core::driver::settings::Profile
                + #ns_fiber::dope::manifold::timing::Policy,
            #g_ctx: #ns_fiber::dope::runtime::executor::Build,
            #bounds
            #state: #ns_h1::server::listener::failure::Policy<#ty>,
        {
            #value
            let #v_plan = #plan;
            #storage
            let #v_prep = #v_plan.prepare(#v_store)?;
            let mut #v_wait = #ns_fiber::dope::runtime::executor::session::Completion::new();
            let #v_op = #constructor {
                finish: &mut #v_wait,
                state: #v_state,
                config: #v_config,
                wire: #v_wire,
                marker: ::core::marker::PhantomData,
            };
            ::core::result::Result::Ok(#v_prep.enter(#v_ctx, #v_op, #v_report, |#v_result| {
                match #v_result {
                    ::core::result::Result::Ok(#v_result) => #v_finish(#v_result.map_err(#ns_h1::server::run::RunError::Failure)),
                    ::core::result::Result::Err(#v_error) => #v_finish(::core::result::Result::Err(#ns_h1::server::run::RunError::from(#v_error))),
                }
            }))
        }
    }
}

fn identity(wire: Wire) -> (syn::Ident, proc_macro2::TokenStream) {
    match wire {
        Wire::Identity => (
            quote::format_ident!("serve"),
            quote::quote!("Runs this generated H1 application over the identity wire."),
        ),
        Wire::Generic => (
            quote::format_ident!("serve_with"),
            quote::quote!("Runs this generated H1 application over a selected wire plan."),
        ),
    }
}

fn wire_params(wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let [g_bind, g_w] = generics.idents(["__BIND", "__WIRE"]);
    match wire {
        Wire::Identity => proc_macro2::TokenStream::new(),
        Wire::Generic => quote::quote! { #g_w, #g_bind, },
    }
}

fn wire_arg(wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let v_wire = generics.local("wire");
    let g_bind = generics.ident("__BIND");
    match wire {
        Wire::Identity => proc_macro2::TokenStream::new(),
        Wire::Generic => quote::quote! { #v_wire: #g_bind, },
    }
}

fn wire_value(paths: &Paths, wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let [v_wire] = generics.locals(["wire"]);
    let ns_fiber = &paths.fiber;
    match wire {
        Wire::Identity => quote::quote! { let #v_wire = #ns_fiber::dope::net::wire::Identity; },
        Wire::Generic => proc_macro2::TokenStream::new(),
    }
}

fn wire_type(paths: &Paths, wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let g_w = generics.ident("__WIRE");
    let ns_fiber = &paths.fiber;
    match wire {
        Wire::Identity => quote::quote! { #ns_fiber::dope::net::wire::Identity },
        Wire::Generic => quote::quote! { #g_w },
    }
}

fn wire_bounds(paths: &Paths, wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let [g_bind, g_list, g_w] = generics.idents(["__BIND", "__LISTENER_ID", "__WIRE"]);
    let ns_fiber = &paths.fiber;
    match wire {
        Wire::Identity => proc_macro2::TokenStream::new(),
        Wire::Generic => quote::quote! {
            #g_w: #ns_fiber::dope::net::link::pool::recv::input::Execution,
            <#g_w as #ns_fiber::dope::net::wire::Spec>::Receive:
                #ns_fiber::dope::net::wire::receive::Policy<
                    #g_w,
                    Reserve: #ns_fiber::dope::net::link::pool::recv::reserve::Choice<
                        #g_w,
                        <#g_w as #ns_fiber::dope::net::link::pool::recv::input::Execution>::Parked,
                    >,
                >,
            <#g_w as #ns_fiber::dope::net::wire::Spec>::Bootstrap:
                #ns_fiber::dope::net::wire::domain::Allocate<#g_w>
                + #ns_fiber::dope::net::wire::domain::Bootstrap<#g_w>,
            #g_bind: #ns_fiber::dope::net::wire::domain::Plan<#g_w, #g_list>,
        },
    }
}

fn error(
    paths: &Paths,
    app: &syn::Ident,
    generics: &types::Generics,
    duplex: duplex::State,
    wire: Wire,
) -> proc_macro2::TokenStream {
    let l_owner = generics.lifetime("__owner");
    let [g_ctx, g_date, g_list, g_prof] =
        generics.idents(["__CONTEXT", "__DATE_ID", "__LISTENER_ID", "__PROFILE"]);
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let lives = &generics.lifetimes;
    let values = &generics.arguments;
    let runner = runner(app, wire);
    let params = wire_params(wire, generics);
    let input = duplex.input_type(paths);
    let wire = wire_error(wire, generics);
    quote::quote! {
        #ns_h1::server::run::RunError<
            #l_owner,
            ::core::convert::Infallible,
            #ns_fiber::dope::core::BranchError<
                #wire,
                #ns_fiber::dope::manifold::BuildError<#ns_h1::server::listener::bind::Error>,
            >,
            (
                #ns_h1::server::run::Plan<#g_list, #g_date, #g_prof, #input>,
                #runner<#l_owner, #( #lives, )* #( #values, )* #params>,
            ),
            #ns_fiber::dope::runtime::shutdown::Requested<#g_ctx::Shutdown>,
            #g_ctx::Shutdown,
        >
    }
}

fn wire_error(wire: Wire, generics: &types::Generics) -> proc_macro2::TokenStream {
    let g_bind = generics.ident("__BIND");
    match wire {
        Wire::Identity => quote::quote! { ::core::convert::Infallible },
        Wire::Generic => quote::quote! { #g_bind::Error },
    }
}

fn runner(app: &syn::Ident, wire: Wire) -> syn::Ident {
    match wire {
        Wire::Identity => quote::format_ident!("__{}Run", app),
        Wire::Generic => quote::format_ident!("__{}Tls", app),
    }
}

use crate::app::serve::duplex;
