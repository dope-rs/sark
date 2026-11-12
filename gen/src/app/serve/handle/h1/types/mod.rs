use crate::runtime::paths::Paths;

mod drive;
mod tasks;

use crate::app::serve::{
    self, duplex,
    handle::{
        self, dispatch,
        h1::{app, route, routes, routing, terminal},
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
    pub(super) egress_bound: proc_macro2::TokenStream,
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
            let values = &context.plan.spec.state_generics.arguments;
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
                let (futures, makers) = context.exec.producers();
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
                    #( #futures, )*
                    #( #makers, )*
                }
            };
            let app = params(true);
            let duplex = plan.duplex.state();
            let upgrade = context.plan.entries.iter().any(|route| route.entry.upgrade);
            let stamp = duplex.stamp(
                quote!(&#g_clock #ns_h1::server::date::Clock),
                duplex.retention(paths),
                upgrade.then(|| quote!(::std::vec::Vec<::core::primitive::u8>)),
            );
            let (futures, makers) = context.exec.producers();
            let core_use = quote! {
                    <
                        #g_brand,
                        #l_env,
                        #( #lives, )*
                        'd,
                        #( #values, )*
                        #g_id,
                        #g_wire,
                        #ns_fiber::manifold::runtime::Direct,
                        #g_mode,
                        #( #futures, )*
                        #( #makers, )*
                    >
            };
            let app_use = |stamp| quote!(<#core #core_use, #stamp>);
            let (mixed_application_params, mixed_application_generic_use) = plan
                .cleartext
                .map(|ns_h2| {
                    let stamp = match duplex {
                        duplex::State::Enabled => stamp.clone(),
                        duplex::State::Disabled => {
                            quote!((#stamp, #ns_fiber::dope::manifold::app::receive::limits::Budget))
                        }
                    };
                    let payload = handle::h1_payload(paths, quote!(()));
                    let h2 = quote! {
                        #ns_h2::server::runtime::app::App<
                            'd, #g_h2a, #g_wire, #g_h2f, #payload,
                        >
                    };
                    (
                        quote!(#app #g_h2a: #ns_h2::server::app::Spec, #g_h2f,),
                        app_use(quote!((#stamp, #h2))),
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
            let [n_method, n_raw] = symbols.locals(["__method", "__raw"]);
            let [n_rproto] = symbols.locals(["__route_protocol"]);
            let [n_shead, n_target] = symbols.locals(["__sark_head", "__target"]);
            let [v_error, v_head, v_input] = symbols.locals(["error", "head", "input"]);
            let [v_method, v_result, v_parsed] = symbols.locals(["method", "outcome", "parsed"]);
            let [v_wait, v_permit, v_target] = symbols.locals(["pending", "permit", "target"]);
            let [v_this, v_routes] = symbols.locals(["this", "routes"]);
            let g_brand = symbols.lifetime("brand");
            let ns_http = &paths.http;

            let spec = context.plan.spec;
            let routes = &spec.route_specs;
            let bound = if plan.cleartext.is_some()
                || matches!(plan.duplex.state(), duplex::State::Enabled)
            {
                quote!(#ns_http::o3::buffer::resident::bound::Unbounded)
            } else {
                handle::decode::egress(
                    paths,
                    spec,
                    &context.plan.entries,
                    &syn::Lifetime::new("'d", proc_macro2::Span::call_site()),
                )
            };
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
            let rejected = terminal::rejected(paths, &context.plan.spec.state_generics);
            let build = |owned: bool| {
                let (source, start, input, target) = match owned {
                    false => (
                        route::Source::Borrowed,
                        quote!(#ns_h1::wire::parsing::Rejected::start),
                        quote!(&#v_parsed),
                        quote!(#v_head.plan().route_path(#v_input.as_slice())),
                    ),
                    true => (
                        route::Source::Owned,
                        quote!(#ns_h1::wire::parsing::Rejected::start_owned),
                        quote!(#v_parsed),
                        quote!(#v_head.plan().route_path(#v_parsed.as_ref())),
                    ),
                };
                let emit = |index, params| {
                    route::emit(paths, context, plan, index, params, &runtime, source)
                };
                let reject = quote! {{
                    let #v_result = #start(#input, #v_permit, #ns_http::h1::codec::canned::NOT_FOUND);
                    let mut #v_this = #v_routes.as_mut().project();
                    #rejected
                    #v_result.into()
                }};
                let trie = spec
                    .route_plan
                    .compile(paths, &spec.state_generics, |index, parts| {
                        let route = &routes[index].route;
                        if spec.route_plan.has_params(index) {
                            let call = emit(index, quote!(#n_raw));
                            quote! {
                                let #n_raw =
                                    <<<#route as #ns_core::route::Spec>::Request
                                        as #ns_core::route::Schema>::RawParams
                                        as #ns_core::route::Params>::from_match(
                                        #parts,
                                    );
                                #call
                            }
                        } else {
                            emit(index, quote!(()))
                        }
                    });
                quote! {
                        let ::core::option::Option::Some(#v_target) = #target else {
                            return #reject;
                        };
                        let #n_method = #v_method;
                        let #n_target = #v_target;
                        let #n_rproto: ::core::option::Option<&[::core::primitive::u8]> =
                            ::core::option::Option::None;
                        #trie
                        #reject
                }
            };
            let body = build(false);
            let line = build(true);

            let heads = plan.continuations.head();
            let failed = terminal::failed(paths, &context.plan.spec.state_generics);
            let resume = context
                .plan
                .entries
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let variant = &item.pending_variant;
                    let complete =
                        route::complete(index, &context.plan.spec.state_generics, &v_routes);
                    quote! {
                        #heads::#variant(#v_wait) => match #v_wait.try_resume(
                            #v_input,
                            #v_permit,
                            #heads::#variant,
                            |#n_shead, #v_permit| {
                                #complete
                            },
                        ) {
                            ::core::result::Result::Ok(#v_result) => #v_result,
                            ::core::result::Result::Err(#v_error) => {
                                let mut #v_this = #v_routes.as_mut().project();
                                #failed
                            }
                        },
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
        let pending = plan.continuations.ty();
        let ready = plan.continuations.ready_ty();
        let partial = plan.continuations.head();
        let head = quote!(#partial);
        let bound = &routing.egress_bound;
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
        let payload = handle::h1_payload(
            paths,
            quote! {
                <#g_mode as #ns_core::life::sequence::Mode<
                    #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                >>::TransportTag
            },
        );
        let egress = quote!(#ns_fiber::dope::net::link::egress::data::Bound<#payload, #bound>);
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
        let owner = plan.continuations.route();
        let heads = plan.continuations.bodies().map(|route| {
            let variant = &route.pending_variant;
            match route.mode {
                serve::Mode::Fiber { .. } => quote!(#owner::#variant(_) => 0,),
                serve::Mode::Sync { .. }
                | serve::Mode::Stream { .. }
                | serve::Mode::Duplex { .. } => {
                    quote!(#owner::#variant(ref #v_wait) => #v_wait.head_len(),)
                }
            }
        });
        let lengths = context.plan.entries.iter().map(|route| {
            let variant = &route.pending_variant;
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
                upgrade: context.plan.entries.iter().any(|route| route.entry.upgrade),
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
