use crate::runtime::paths::Paths;

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
}

pub(super) struct Generics {
    pub(super) bounds: proc_macro2::TokenStream,
    pub(super) application_params: proc_macro2::TokenStream,
    pub(super) application_generic_use: proc_macro2::TokenStream,
}

impl Generics {
    fn new(
        paths: &Paths,
        context: &handle::Context<'_, '_, '_>,
        plan: &handle::H1Plan<'_, '_>,
        dispatch: &dispatch::Dispatch,
    ) -> Self {
        let [l_env, g_brand, g_clock] = context
            .plan
            .spec
            .state_generics
            .lives(["env", "brand", "clock"]);
        let [g_id, g_mode, g_wire] = context
            .plan
            .spec
            .state_generics
            .idents(["__ID", "__M", "__W"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

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
        let (futures, makers) = context.exec.producers();
        let app = quote! {
                #g_brand: #l_env,
                #l_env,
                #( #lives: #l_env, )*
                #g_clock,
                'd: #l_env + #g_clock,
                #( #decls, )*
                #g_id: #ns_fiber::Identity,
                #g_wire: #ns_fiber::dope::net::link::pool::recv::input::Execution + 'd,
                #g_mode,
                #( #futures, )*
                #( #makers, )*
        };
        let duplex = plan.duplex.state();
        let stamp = duplex.stamp(
            quote!(&#g_clock #ns_h1::server::date::Clock),
            duplex.retention(paths),
        );
        let (futures, makers) = context.exec.producers();
        let app_use = quote! {
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
                #stamp,
                #( #futures, )*
                #( #makers, )*
            >
        };
        Self {
            bounds,
            application_params: app,
            application_generic_use: app_use,
        }
    }
}

pub(super) struct Routing<'a> {
    pub(super) egress_bound: proc_macro2::TokenStream,
    pub(super) runtime: proc_macro2::TokenStream,
    pub(super) method_mask: &'a proc_macro2::TokenStream,
    pub(super) dispatch_body: proc_macro2::TokenStream,
    pub(super) dispatch_line: proc_macro2::TokenStream,
    pub(super) resume_head: proc_macro2::TokenStream,
}

impl<'a> Routing<'a> {
    fn new<'view, 'source, 'spec>(
        paths: &Paths,
        ctx: &handle::Context<'view, 'source, 'spec>,
        plan: &handle::H1Plan<'source, 'spec>,
        dispatch: &'a dispatch::Dispatch,
        generics: &Generics,
    ) -> Self {
        let symbols = &ctx.plan.spec.state_generics;
        let [n_method, n_raw] = symbols.locals(["__method", "__raw"]);
        let [n_rproto] = symbols.locals(["__route_protocol"]);
        let [n_shead, n_target] = symbols.locals(["__sark_head", "__target"]);
        let [v_error, v_head, v_input] = symbols.locals(["error", "head", "input"]);
        let [v_method, v_result, v_parsed] = symbols.locals(["method", "outcome", "parsed"]);
        let [v_wait, v_permit, v_target] = symbols.locals(["pending", "permit", "target"]);
        let [v_this, v_routes] = symbols.locals(["this", "routes"]);
        let g_brand = symbols.lifetime("brand");
        let [g_id, g_mode] = symbols.idents(["__ID", "__M"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        let ns_http = &paths.http;
        use quote::quote;

        let spec = ctx.plan.spec;
        let routes = &spec.route_specs;
        let core = &ctx.core_ident;
        let args = &generics.application_generic_use;
        let bound = match plan.duplex.state() {
            duplex::State::Enabled => quote!(#ns_http::o3::buffer::resident::bound::Unbounded),
            duplex::State::Disabled => {
                quote!(<#core #args as #ns_core::decode::Decode>::Egress<'d>)
            }
        };
        let task = &ctx.exec.task.identity;
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
            >
        };
        let rejected = terminal::rejected(paths, &ctx.plan.spec.state_generics);
        let build = |owned: bool| {
            let project = owned.then(|| quote!(let mut #v_this = self.as_mut().project();));
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
            let emit =
                |index, params| route::emit(paths, ctx, plan, index, params, &runtime, source);
            let reject = quote! {{
                let #v_result = #start(#input, #v_permit, #ns_http::h1::codec::canned::NOT_FOUND);
                #project
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
        let failed = terminal::failed(paths, &ctx.plan.spec.state_generics);
        let resume = ctx.plan.entries.iter().enumerate().map(|(index, item)| {
            let variant = &item.pending_variant;
            let complete =
                route::complete(index, &ctx.plan.spec.state_generics, &quote!(#v_routes));
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
        Self {
            resume_head: headers,
            egress_bound: bound,
            runtime,
            method_mask: &dispatch.methods.method_mask,
            dispatch_body: body,
            dispatch_line: line,
        }
    }
}

pub(super) struct Connections {
    pub(super) pending: proc_macro2::TokenStream,
    pub(super) ready: proc_macro2::TokenStream,
    pub(super) head: proc_macro2::TokenStream,
    pub(super) http_connection: proc_macro2::TokenStream,
}

pub(super) struct Protocol {
    pub(super) duplex: duplex::State,
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
    pub(super) http: proc_macro2::TokenStream,
    pub(super) http_mut: proc_macro2::TokenStream,
    pub(super) audit: proc_macro2::TokenStream,
    pub(super) tasks: tasks::Tasks,
}

impl<'a> Types<'a> {
    pub(in crate::app::serve::handle) fn build<'view, 'source, 'spec>(
        paths: &Paths,
        context: &'a handle::Context<'view, 'source, 'spec>,
        plan: &'a handle::H1Plan<'source, 'spec>,
        dispatch: &'a dispatch::Dispatch,
    ) -> proc_macro2::TokenStream {
        let types = Self::new(paths, context, plan, dispatch);
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
    ) -> Self {
        let symbols = &context.plan.spec.state_generics;
        let [n_bytes] = symbols.locals(["__bytes"]);
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

        let tasks = tasks::Tasks::build(paths, context);
        let generics = Generics::new(paths, context, plan, dispatch);
        let routing = Routing::new(paths, context, plan, dispatch, &generics);
        let spec = context.plan.spec;
        let state = spec.state.ty();
        let name = &context.name;
        let core = &context.core_ident;
        let duplex = plan.duplex.state();
        let http = quote!((#v_link.state()));
        let edit = quote!((#v_link.state_mut()));
        let kind = plan.duplex.ty(paths);
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
        let payload = quote! {
            #ns_fiber::dope::net::link::egress::data::compose::Either<
                #ns_h1::server::payload::Body<
                    'd,
                    <#g_mode as #ns_core::life::sequence::Mode<
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    >>::TransportTag,
                >,
                #ns_h1::server::payload::Stream<
                    'd,
                    <#g_mode as #ns_core::life::sequence::Mode<
                        #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                    >>::TransportTag,
                >,
            >
        };
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
        let heads = context.plan.entries.iter().map(|route| {
            let variant = &route.pending_variant;
            match route.mode {
                serve::Mode::Fiber { .. } => quote!(#owner::#variant(_) => 0,),
                serve::Mode::Sync { .. }
                | serve::Mode::Stream { .. }
                | serve::Mode::Duplex { .. } => {
                    quote!(#owner::#variant(#v_wait) => #v_wait.head_len(),)
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
                    .audit(|#v_wait| match &#v_wait.route { #( #heads )* }, |_| 0,
                        |#v_wait| match #v_wait { #( #lengths )* })
                    .task(#n_owners, #n_owners, 0)
                    .owners(#n_run.owners())
                    .stage_bytes(#n_run.staged_bytes())
                    .conn_ready(0)
                    .wire_bytes(
                        #v_link.response_len(),
                    );
                if let ::core::option::Option::Some(#n_bytes) =
                    #n_run.cleared()
                {
                    #n_snap = #n_snap.owner_bytes(#n_bytes);
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
        Self {
            names: Names {
                generics: &spec.state_generics,
                state_ty: state,
                name,
                core_ident: core,
            },
            generics,
            routing,
            protocol: Protocol {
                duplex,
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
            http,
            http_mut: edit,
            audit,
            tasks,
        }
    }
}
