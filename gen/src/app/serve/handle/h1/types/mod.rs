use crate::runtime::paths::Paths;

mod drive;
mod tasks;

use crate::app::serve::{
    self, duplex,
    handle::{
        self, dispatch,
        h1::{app, route, routes, routing},
    },
};

pub(super) struct Names<'a> {
    pub(super) generics: &'a crate::types::Generics,
    pub(super) state_ty: &'a syn::Type,
    pub(super) name: &'a syn::Ident,
    pub(super) core_ident: &'a syn::Ident,
    pub(super) dispatch_ident: syn::Ident,
}

pub(super) struct Generics {
    pub(super) bounds: proc_macro2::TokenStream,
    pub(super) application_params: proc_macro2::TokenStream,
    pub(super) application_generic_use: proc_macro2::TokenStream,
    pub(super) core_params: proc_macro2::TokenStream,
    pub(super) core_generic_use: proc_macro2::TokenStream,
    pub(super) mixed_application_params: proc_macro2::TokenStream,
    pub(super) mixed_application_generic_use: proc_macro2::TokenStream,
}

pub(super) struct Routing<'a> {
    pub(super) egress_bound: &'a proc_macro2::TokenStream,
    pub(super) runtime: proc_macro2::TokenStream,
    pub(super) method_mask: &'a proc_macro2::TokenStream,
    pub(super) dispatch_body: proc_macro2::TokenStream,
    pub(super) dispatch_line: proc_macro2::TokenStream,
    pub(super) resume_head: proc_macro2::TokenStream,
}

pub(super) struct Connections {
    pub(super) pending: proc_macro2::TokenStream,
    pub(super) ready: proc_macro2::TokenStream,
    pub(super) head: proc_macro2::TokenStream,
    pub(super) http_connection: proc_macro2::TokenStream,
}

pub(super) struct Protocol {
    pub(super) duplex: duplex::State,
    pub(super) upgrade: bool,
    pub(super) duplex_ty: proc_macro2::TokenStream,
    pub(super) connections: Connections,
    pub(super) transition_policy: proc_macro2::TokenStream,
    pub(super) egress_type: proc_macro2::TokenStream,

    pub(super) input_type: proc_macro2::TokenStream,
    pub(super) h1_slot: proc_macro2::TokenStream,
}

pub(in crate::app::serve::handle) struct Types<'a> {
    pub(super) names: Names<'a>,
    pub(super) generics: Generics,
    pub(super) routing: Routing<'a>,
    pub(super) protocol: Protocol,
    pub(super) h2: Option<&'a proc_macro2::TokenStream>,
    pub(super) http_mut: proc_macro2::TokenStream,
    pub(super) audit: proc_macro2::TokenStream,
    pub(super) tasks: tasks::Tasks,
    pub(super) task_methods: proc_macro2::TokenStream,
}

impl<'a> Types<'a> {
    pub(in crate::app::serve::handle) fn build<'view, 'source, 'spec>(
        paths: &Paths,
        context: &'a handle::Context<'view, 'source, 'spec>,
        plan: &'a handle::H1Plan<'source, 'spec>,
        dispatch: &'a dispatch::Dispatch,
        direct: &proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let types = Self::new(paths, context, plan, dispatch, direct);
        let routes = routes::Routes::new(paths, context, plan, &types);
        let methods = route::methods(paths, context, plan, &types);
        let routing = routing::build(paths, &types, &routes, &methods);
        let app = app::build(paths, &types, &routes);
        quote::quote! {
            #routing
            #app
        }
    }

    pub(super) fn new<'view, 'source, 'spec>(
        paths: &Paths,
        context: &'a handle::Context<'view, 'source, 'spec>,
        plan: &'a handle::H1Plan<'source, 'spec>,
        dispatch: &'a dispatch::Dispatch,
        direct: &proc_macro2::TokenStream,
    ) -> Self {
        let symbols = &context.plan.spec.state_generics;
        let [n_owners, n_http] = symbols.locals(["__connection_tasks", "__http"]);
        let [n_run, n_snap] = symbols.locals(["__runtime", "__snapshot"]);
        let [v_link, v_wait] = symbols.locals(["connection", "pending"]);
        let g_event = context.plan.spec.state_generics.lifetime("e");
        let [g_id, g_mode, g_wire] = context
            .plan
            .spec
            .state_generics
            .idents(["__ID", "__M", "__W"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

        let dispatch_ident = crate::handler::names::encoded("__SarkH1Dispatch", &context.name);
        let tasks = tasks::Tasks::build(paths, context, &dispatch_ident);
        let generics = {
            let [l_env, g_brand, g_clock] = symbols.lives(["env", "brand", "clock"]);
            let [g_h2a, g_h2f] = symbols.idents(["__H2A", "__H2F"]);
            let core = &context.core_ident;

            let lives = &context.plan.spec.state_generics.lifetimes;
            let decls = &context.plan.spec.state_generics.parameters;
            let task_types = &dispatch.shape.core.tasks;
            let base = &dispatch.shape.base_bounds;
            let bounds = quote! {
                #base
                #g_mode: #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    Retention: #ns_h1::server::task::turn::Mode,
                    TransportTag: 'd,
                >,
            };
            let params = |clock: bool| {
                let clock = clock.then_some(&g_clock);
                let clock = clock.as_slice();
                quote! {
                    #g_brand: #l_env,
                    #l_env,
                    #( #lives: #l_env, )*
                    #( #clock, )*
                    'd: #l_env #( + #clock )*,
                    #( #decls, )*
                    #g_id: #ns_fiber::Identity,
                    #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
                    #g_mode,
                    #task_types
                }
            };
            let app = params(true);
            let duplex = plan.duplex.state();
            let stamp = &plan.stamp;
            let core_use = dispatch
                .shape
                .core
                .with(quote!(#ns_fiber::manifold::runtime::Direct), &g_mode);
            let app_use = |stamp: &proc_macro2::TokenStream| quote!(<#core #core_use, #stamp>);
            let (mixed_application_params, mixed_application_generic_use) = plan
                .cleartext
                .map(|ns_h2| {
                    let stamp = match duplex {
                        duplex::State::Enabled => stamp.clone(),
                        duplex::State::Disabled => {
                            quote!((#stamp, #ns_fiber::dope::manifold::app::receive::limits::Budget))
                        }
                    };
                    let h2 = quote! {
                        #ns_h2::server::runtime::app::App<
                            'd, #g_h2a, #g_wire, #g_h2f, #ns_h1::server::payload::Owner<'d>,
                        >
                    };
                    (
                        quote!(#app #g_h2a: #ns_h2::server::app::Spec, #g_h2f,),
                        app_use(&quote!((#stamp, #h2))),
                    )
                })
                .unwrap_or_default();
            Generics {
                bounds,
                application_params: app,
                application_generic_use: app_use(stamp),
                core_params: params(false),
                core_generic_use: core_use,
                mixed_application_params,
                mixed_application_generic_use,
            }
        };
        let routing = {
            let n_method = symbols.local("__method");
            let [n_rproto] = symbols.locals(["__route_protocol"]);
            let [n_shead, n_target] = symbols.locals(["__sark_head", "__target"]);
            let [v_error, v_head, v_input] = symbols.locals(["error", "head", "input"]);
            let [v_method, v_result, v_parsed] = symbols.locals(["method", "outcome", "parsed"]);
            let [v_wait, v_turn, v_prior] = symbols.locals(["pending", "turn", "prior"]);
            let g_brand = symbols.lifetime("brand");
            let spec = context.plan.spec;
            let bound = &plan.egress_bound;
            let task = &context.exec.identity;
            let ended = if context.exec.tasks().is_empty() {
                quote!(#task<#g_brand, 'd, #g_id>)
            } else {
                quote!(#ns_fiber::dope_fiber::task::storage::fixed::Id<
                #g_brand, 'd, #g_id, #ns_fiber::dope_fiber::task::storage::fixed::local::Ended,
            >)
            };
            let runtime = quote! {
                #ns_h1::server::task::runtime::Runtime<
                    'd,
                    #task<#g_brand, 'd, #g_id>,
                    <#g_mode as #ns_core::life::sequence::Mode<
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    >>::Session,
                    <#g_mode as #ns_core::life::sequence::Mode<
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    >>::TransportTag,
                    #bound,
                    #ended,
                >
            };
            let build = |owned: bool| {
                let target = if owned {
                    quote!(#v_head.plan().route_path(#v_parsed.as_ref()))
                } else {
                    quote!(#v_head.plan().route_path(#v_input.as_slice()))
                };
                let trie = spec.route_plan.compile(paths, symbols, |index, parts| {
                    route::start(paths, context, plan, index, parts, owned)
                });
                quote! {
                    if let ::core::option::Option::Some(#n_target) = #target {
                        let #n_method = #v_method;
                        let #n_rproto: ::core::option::Option<&[::core::primitive::u8]> =
                            ::core::option::Option::None;
                        #trie
                    }
                }
            };
            let body = build(false);
            let line = build(true);

            let heads = &plan.head;
            let resume = plan.heads()
                .map(|(index, item)| {
                    let variant = &item.prepared;
                    let complete =
                        route::complete(index, &context.plan.spec.state_generics);
                    quote! {
                        #heads::#variant(#v_wait) => {
                            let #v_prior = #v_wait.len() as usize;
                            match #v_wait.try_resume(#v_input, |#n_shead| #complete) {
                                ::core::result::Result::Ok(::core::ops::ControlFlow::Continue(#v_result)) => #v_result,
                                ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#v_wait)) => {
                                    #v_turn.await_head(#heads::#variant(#v_wait))
                                }
                                ::core::result::Result::Err(#v_error) => {
                                    #v_turn.reject_head(#v_error.into_rejection())
                                }
                            }
                        }
                    }
                });
            let headers = quote!(match #v_wait { #( #resume )* });
            Routing {
                resume_head: headers,
                egress_bound: bound,
                runtime,
                method_mask: &dispatch.methods.method_mask,
                dispatch_body: body,
                dispatch_line: line,
            }
        };
        let spec = context.plan.spec;
        let state = spec.state.ty();
        let name = &context.name;
        let core = &context.core_ident;
        let duplex = plan.duplex.state();
        let edit = quote!((#v_link.state_mut()));
        let kind = plan.duplex.ty(paths, false);
        let continuation = &plan.continuation;
        let pending = quote!(#continuation<'d, #g_id>);
        let ready = if context.exec.fibers().is_empty() {
            quote!(::core::convert::Infallible)
        } else {
            let built = &plan.ready_route;
            quote!(#continuation<'d, #g_id, #built<'d>>)
        };
        let partial = &plan.head;
        let head = quote!(#partial);
        let bound = routing.egress_bound;
        let rt = &routing.runtime;
        let policy = match duplex {
            duplex::State::Enabled => quote! {
                #ns_h1::switch::Enabled<
                    #ns_core::duplex::machine::Machine<'d, #kind>
                >
            },
            duplex::State::Disabled => quote!(#ns_h1::switch::Disabled),
        };
        let conn = quote! {
            #ns_h1::session::Conn<
                'd,
                #rt,
                #pending,
                #policy,
                #ready,
                #head,
            >
        };
        let tag = quote! {
            <#g_mode as #ns_core::life::sequence::Mode<
                #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
            >>::TransportTag
        };
        let egress = quote! {
            #ns_fiber::dope::net::link::egress::data::Bound<#ns_h1::server::payload::Owner<'d, #tag>, #bound>
        };
        let input = duplex.input_type(paths);
        let view = quote! {
            #ns_fiber::dope::manifold::network::listener::conn::Ctx<
                #g_event,
                '_,
                '_,
                '_,
                '_,
                'd,
                 #g_id,
                #g_wire,
                #conn,
                #egress,
            >
        };
        let owner = &plan.route;
        let heads = plan.bodies().map(|route| {
            let variant = &route.prepared;
            match route.mode {
                serve::Mode::Fiber { .. } => quote!(#owner::#variant(_) => 0,),
                serve::Mode::Sync | serve::Mode::Stream { .. } | serve::Mode::Duplex { .. } => {
                    quote!(#owner::#variant(ref #v_wait) => #v_wait.head_len(),)
                }
            }
        });
        let lengths = plan.heads().map(|(_, route)| {
            let variant = &route.prepared;
            quote!(#partial::#variant(#v_wait) => #v_wait.len(),)
        });
        let probe = quote! {
            {
                let #n_run = &#n_http.async_state;
                let #n_owners = #n_run.owners();
                let mut #n_snap = #n_http.ingress
                    .audit(|#v_wait| match #v_wait.route { #( #heads )* }, |_| 0,
                        |#v_wait| match #v_wait { #( #lengths )* })
                    .task(#n_owners, #n_owners, 0)
                    .owners(#n_owners)
                    .stage_bytes(#n_run.staged_bytes())
                    .conn_ready(0)
                    .wire_bytes(
                        #v_link.response_len(),
                    );
                if #n_owners == 0 {
                    #n_snap = #n_snap.owner_bytes(0);
                }
                #n_snap
            }
        };
        let audit = quote! {
            {
                let #n_http = #v_link.state();
                #probe
            }
        };
        let task_methods = drive::methods(paths, context, direct, &view, bound);
        Self {
            task_methods,
            names: Names {
                generics: &spec.state_generics,
                state_ty: state,
                name,
                core_ident: core,
                dispatch_ident,
            },
            generics,
            routing,
            h2: plan.cleartext,
            protocol: Protocol {
                duplex,
                upgrade: plan.upgrade,
                duplex_ty: kind,
                connections: Connections {
                    pending,
                    ready,
                    head,
                    http_connection: conn,
                },
                transition_policy: policy,
                egress_type: egress,
                input_type: input,
                h1_slot: view,
            },
            http_mut: edit,
            audit,
            tasks,
        }
    }
}
