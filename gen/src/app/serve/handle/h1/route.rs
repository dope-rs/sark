use crate::{
    app::{
        inputs,
        serve::{
            self, handle,
            handle::h1::{admit, terminal, types},
        },
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

#[derive(Clone, Copy)]
pub(super) enum Source {
    Borrowed,
    Owned,
    Resumed,
}

pub(super) fn complete(
    index: usize,
    generics: &crate::types::Generics,
    receiver: &proc_macro2::Ident,
) -> proc_macro2::TokenStream {
    let [n_shead, v_conn] = generics.locals(["__sark_head", "conn"]);
    let [v_link, v_date] = generics.locals(["connection", "date"]);
    let [v_permit, v_write] = generics.locals(["permit", "write"]);
    let method = quote::format_ident!("__complete_head_{index}");
    quote::quote! {
        #receiver.as_mut().#method(#v_link, #v_permit, #n_shead, #v_write, #v_date, #v_conn)
    }
}

pub(super) fn methods<'view, 'source, 'spec>(
    paths: &Paths,
    ctx: &handle::Context<'view, 'source, 'spec>,
    h1: &handle::H1Plan<'source, 'spec>,
    types: &types::Types<'_>,
) -> proc_macro2::TokenStream {
    let f_state = local("state");
    let symbols = &ctx.plan.spec.state_generics;
    let [n_shead, v_conn] = symbols.locals(["__sark_head", "conn"]);
    let [v_link, v_date, v_head] = symbols.locals(["connection", "date", "head"]);
    let [v_input, v_parsed, v_permit] = symbols.locals(["input", "parsed", "permit"]);
    let [v_state, v_this, v_write] = symbols.locals(["state", "this", "write"]);
    let l_env = ctx.plan.spec.state_generics.lifetime("env");
    let g_id = ctx.plan.spec.state_generics.ident("__ID");
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let state = ctx.plan.spec.state.ty();
    let conn = &types.protocol.connections.http_connection;
    let pending = &types.protocol.connections.pending;
    let heads = &types.protocol.connections.head;
    let policy = &types.protocol.transition_policy;
    let generics = &ctx.plan.spec.state_generics;
    let [head_lt, req_lt] = generics.lives(["__sark_head", "__sark_request"]);
    let methods = ctx.plan.entries.iter().enumerate().map(|(index, plan)| {
        let method = quote::format_ident!("__complete_head_{index}");
        let route = &plan.entry.route;
        let admission = plan
            .entry
            .upgrade
            .then(|| quote::quote!(, #ns_h1::route::duplex::Upgrade));
        let coding = match plan.mode {
            serve::Mode::Sync { .. } => quote::quote!(#ns_http::h1::head::request::Coding),
            serve::Mode::Duplex { .. } | serve::Mode::Fiber { .. } | serve::Mode::Stream { .. } => {
                quote::quote!(#ns_h1::wire::parsing::framing::Plain)
            }
        };
        let body = emit(
            paths,
            ctx,
            h1,
            index,
            quote::quote!(()),
            &types.routing.runtime,
            Source::Resumed,
        );
        quote::quote! {
            #[inline(always)]
            fn #method<#head_lt, #req_lt>(
                self: ::core::pin::Pin<&mut Self>,
                #v_link: #ns_fiber::dope::manifold::network::listener::conn::Id<'d, #g_id>,
                #v_permit: #ns_h1::session::Permit,
                #n_shead: #ns_h1::wire::parsing::framing::Head<
                    #head_lt, #req_lt, #route, #coding #admission,
                >,
                #v_write: #ns_http::o3::buffer::write::Slice<'_, &mut usize>,
                #v_date: &[::core::primitive::u8; 29],
                #v_conn: &mut #conn,
            ) -> #ns_h1::session::RouteOutcome<
                'd,
                #pending,
                <#policy as #ns_h1::switch::Engine>::Raw,
                #heads,
                #ns_h1::wire::parsing::Rejected<'d>,
            > {
                let mut #v_this = self.project();
                let #v_state: &#l_env #state = *#v_this.#f_state;
                let #v_parsed = #n_shead.source();
                let #v_input = #v_parsed.input();
                let #v_head = #v_parsed.request();
                #body
            }
        }
    });
    quote::quote!(#(#methods)*)
}

pub(super) fn emit<'view, 'source, 'spec>(
    paths: &Paths,
    ctx: &handle::Context<'view, 'source, 'spec>,
    h1: &handle::H1Plan<'source, 'spec>,
    index: usize,
    params: proc_macro2::TokenStream,
    runtime: &proc_macro2::TokenStream,
    source: Source,
) -> proc_macro2::TokenStream {
    let [f_life, f_cache, f_gzip] = ["lifecycle", "response_cache", "gzip"].map(local);
    let [f_ids, f_time, f_makers] = ["ids", "timer", "task_producers"].map(local);
    let symbols = &ctx.plan.spec.state_generics;
    let [v_expect, v_make] = symbols.locals(["expectation", "producer"]);
    let [v_sess, v_timer, v_value] = symbols.locals(["session", "timer", "value"]);
    let [n_id, n_resp] = symbols.locals(["__id", "__response"]);
    let [n_shead, n_seq] = symbols.locals(["__sark_head", "__sark_id"]);
    let [n_tasks, v_conn, v_date] = symbols.locals(["__tasks", "conn", "date"]);
    let [v_error, v_exch, v_head] = symbols.locals(["error", "exchange", "head"]);
    let [v_result, v_parsed, v_wait] = symbols.locals(["outcome", "parsed", "pending"]);
    let [v_permit, v_resp, v_state] = symbols.locals(["permit", "response", "state"]);
    let [v_this, v_write] = symbols.locals(["this", "write"]);
    let v_routes = symbols.local("routes");
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    use quote::quote;

    let spec = ctx.plan.spec;
    let state = spec.state.ty();
    let cont = &h1.continuations;
    use syn::Index;
    let Some(plan) = ctx.plan.route(index) else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "route machine emitted an unknown route",
        )
        .into_compile_error();
    };
    let route = &plan.entry.route;
    let admission = plan
        .entry
        .upgrade
        .then(|| quote!(, #ns_h1::wire::parsing::framing::Plain, #ns_h1::route::duplex::Upgrade));
    let slot = &plan.head;
    let heads = cont.head();
    let head = &plan.pending_variant;
    let open = terminal::open(paths, &ctx.plan.spec.state_generics);
    let failed = terminal::failed(paths, &ctx.plan.spec.state_generics);
    let reject = quote! {
        #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
    };
    let parsed = match source {
        Source::Owned => quote!(#v_parsed),
        Source::Borrowed | Source::Resumed => quote!(&#v_parsed),
    };
    let found = if spec.route_plan.has_params(index) {
        quote! {
            // SAFETY: these captures matched this route against this unchanged head.
            unsafe { #ns_h1::wire::parsing::raw::Bound::<_, #route>::new(#parsed, #params) }
        }
    } else {
        quote! { #ns_h1::wire::parsing::raw::Bound::<_, #route>::empty(#parsed) }
    };
    if matches!(source, Source::Owned) {
        let (start, coding) = match &plan.mode {
            serve::Mode::Sync { .. } => (
                quote! {
                    #ns_h1::wire::parsing::framing::PendingHead::<
                        #route,
                        #ns_http::h1::head::request::Coding,
                    >::try_start
                },
                quote!(<#route as #ns_h1::route::PolicyOf>::POLICY.coding,),
            ),
            serve::Mode::Duplex { .. } | serve::Mode::Fiber { .. } | serve::Mode::Stream { .. } => {
                (
                    quote!(#ns_h1::wire::parsing::framing::PendingHead::<#route #admission>::try_start),
                    quote!(),
                )
            }
        };
        let complete = complete(index, &ctx.plan.spec.state_generics, &v_routes);
        return quote! {
            return match #start(
                #found,
                #coding
                #v_permit,
                #heads::#head,
                |#n_shead, #v_permit| {
                    #complete
                },
            ) {
                ::core::result::Result::Ok(#v_result) => #v_result,
                ::core::result::Result::Err(#v_error) => {
                    let mut #v_this = #v_routes.as_mut().project();
                    #failed
                }
            };
        };
    }
    let method = plan.entry.method.http_token(paths);
    let guard = terminal::guard(paths, &ctx.plan.spec.state_generics, &method, slot, state);
    if matches!(source, Source::Borrowed) {
        let parse = match &plan.mode {
            serve::Mode::Sync { .. } => quote! {
                #ns_h1::wire::parsing::framing::Head::<
                    #route,
                    #ns_http::h1::head::request::Coding,
                >::start(#found, <#route as #ns_h1::route::PolicyOf>::POLICY.coding)
            },
            serve::Mode::Duplex { .. } | serve::Mode::Fiber { .. } | serve::Mode::Stream { .. } => {
                quote! {
                    #ns_h1::wire::parsing::framing::Head::<#route #admission>::start(#found)
                }
            }
        };
        let complete = complete(index, &ctx.plan.spec.state_generics, &v_routes);
        return quote! {
            let #n_shead = match #parse {
                ::core::result::Result::Ok(
                    #ns_h1::wire::parsing::framing::HeadProgress::Complete(#v_head),
                ) => #v_head,
                ::core::result::Result::Ok(
                    #ns_h1::wire::parsing::framing::HeadProgress::Pending(#v_wait),
                ) => {
                    return #ns_h1::session::RouteOutcome::AwaitHead {
                        permit: #v_permit,
                        pending: #heads::#head(#v_wait),
                    };
                }
                ::core::result::Result::Err(#v_error) => {
                    let mut #v_this = #v_routes.as_mut().project();
                    return { #failed };
                }
            };
            return #complete;
        };
    }
    let (prepare, invoke) = match &plan.mode {
        serve::Mode::Duplex { family } => {
            let wrap = h1.duplex.wrap(*family, false, quote!(#v_sess));
            let (method, write) = if plan.entry.upgrade {
                (quote!(upgrade::<#route, _, _>), quote!(#v_write,))
            } else {
                (quote!(dispatch::<#route, _, _>), quote!())
            };
            (
                quote!(),
                quote! {
                    #ns_h1::route::duplex::Endpoint::new(#v_state, #v_conn).#method(
                        #v_permit,
                        #n_shead,
                        #write
                        |#v_sess| #wrap,
                        #ns_core::life::Mode::body(#v_this.#f_life, #v_exch),
                    )
                },
            )
        }
        serve::Mode::Sync { cache } => {
            let index = Index::from(*cache);
            (
                quote!(),
                quote! {
                    #ns_h1::route::sync::Endpoint::new(
                        #ns_h1::route::sync::Egress::new(
                            #ns_h1::wire::output::Egress::new(#v_write, #v_date, #n_resp),
                            #ns_h1::wire::cache::Cache::new(&mut #v_this.#f_cache[#index]),
                            #v_this.#f_gzip,
                        ),
                        #v_conn,
                    ).dispatch::<#route, #state, _>(
                        #v_permit,
                        #n_shead,
                        #v_state,
                        #ns_core::life::Mode::body(#v_this.#f_life, #v_exch),
                    )
                },
            )
        }
        serve::Mode::Fiber { task, fiber } => {
            let task = serve::Task::new(plan, task);
            let index = Index::from(fiber.slot);
            let f_tasks = local("tasks");
            let tasks = quote!(#v_this.#f_tasks.as_mut().scope());
            let source = admit::fiber(
                paths,
                ctx,
                task,
                runtime,
                quote!(#v_state),
                quote!(#v_timer),
                quote!(#v_make),
            );
            let feed = match fiber.body {
                inputs::Body::Discarded => {
                    quote!(#ns_h1::route::task::Pending::<'d, #route>::try_discard)
                }
                inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => {
                    quote!(#ns_h1::route::task::Pending::<'d, #route>::try_retain)
                }
            };
            (
                quote! {
                    let #n_seq = match #v_this.#f_ids.try_next() {
                        ::core::result::Result::Ok(#n_id) => #n_id,
                        ::core::result::Result::Err(
                            #ns_fiber::dope::core::driver::schedule::sequence::Exhausted,
                        ) => {
                            #open
                            #ns_core::life::sequence::Mode::reject(
                                #v_this.#f_life,
                                #v_exch,
                            );
                            return #ns_h1::session::Outcome::Close(
                                #ns_http::h1::codec::canned::SERVICE_UNAVAILABLE,
                            ).into();
                        }
                    };
                },
                quote! {
                    {
                        let mut #n_tasks = #tasks;
                        let #v_timer = <#ns_fiber::manifold::runtime::Direct as
                            #ns_fiber::manifold::runtime::Runnable
                        >::handle(#v_this.#f_time);
                        let #v_make = #v_this.#f_makers.#index;
                        #ns_h1::route::task::Endpoint::new(
                            #v_conn,
                        ).dispatch::<#route, _, _, _>(
                            #n_seq,
                            #v_permit,
                            #n_shead,
                            #source,
                            #feed,
                            #ns_core::life::Mode::body(
                                #v_this.#f_life,
                                #v_exch,
                            ),
                        )
                    }
                },
            )
        }
        serve::Mode::Stream { task } => {
            let task = serve::Task::new(plan, task);
            let f_tasks = local("tasks");
            let tasks = quote!(#v_this.#f_tasks.as_mut().scope());
            let source = admit::stream(paths, ctx, task, runtime);
            let invoke = quote! {
                {
                    let mut #n_tasks = #tasks;
                    #ns_h1::route::stream::Endpoint::new(
                        #ns_h1::wire::output::Egress::new(#v_write, #v_date, #n_resp),
                        #v_conn,
                    ).dispatch::<#route, #state, _, _, _>(
                        #v_permit,
                        #n_shead,
                        #v_state,
                        #source,
                        #ns_core::life::Mode::body(
                            #v_this.#f_life,
                            #v_exch,
                        ),
                    )
                }
            };
            (quote!(), invoke)
        }
    };
    let input = quote! {
        #ns_core::life::Mode::observe(
            #v_this.#f_life,
            #ns_core::life::Event::InputFinished(#v_exch),
        );
    };
    let commit = quote! {
        #ns_core::life::sequence::Mode::commit(
            #v_this.#f_life,
            #v_conn.async_state.session_mut(),
            #v_exch,
            0,
            #ns_http::o3::buffer::hold::Charge::default(),
        );
    };
    let response = match &plan.mode {
        serve::Mode::Sync { .. } | serve::Mode::Stream { .. } => {
            let finished = terminal::completed(
                paths,
                &ctx.plan.spec.state_generics,
                &quote!(#v_conn.async_state.session_mut()),
            );
            quote! {
                #ns_h1::session::Outcome::Complete { response: #v_resp, .. } => {
                    #input
                    #finished
                }
            }
        }
        serve::Mode::Duplex { .. } | serve::Mode::Fiber { .. } => quote! {
            #ns_h1::session::Outcome::Complete { .. } => {}
        },
    };
    let watch = match &plan.mode {
        serve::Mode::Sync { .. } | serve::Mode::Duplex { .. } => {
            quote! {
                #ns_h1::session::Outcome::Switch { .. } => {
                    #input
                    #commit
                    #v_exch.retire(#v_this.#f_life, #ns_core::life::Outcome::Completed);
                }
                #ns_h1::session::Outcome::Streamed { .. }
                | #ns_h1::session::Outcome::Park { .. } => {}
            }
        }
        serve::Mode::Fiber { .. } | serve::Mode::Stream { .. } => {
            let admitted = quote! {
                #ns_core::life::Mode::observe(
                    #v_this.#f_life,
                    #ns_core::life::Event::TaskAdmitted(#v_exch),
                );
            };
            quote! {
                #ns_h1::session::Outcome::Park { .. } => {
                    #input
                    #admitted
                }
                #ns_h1::session::Outcome::Streamed { .. } => {
                    #input
                    #commit
                    #admitted
                }
                #ns_h1::session::Outcome::Switch { .. } => {}
            }
        }
    };
    let name = cont.name();
    let pending = cont.route();
    let body = if plan.entry.upgrade {
        quote! {
            #ns_h1::session::RouteOutcome::AwaitBody { pending: #v_wait, .. } => match #v_wait {},
        }
    } else {
        quote! {
            #ns_h1::session::RouteOutcome::AwaitBody {
                state: #v_state,
                expectation: #v_expect,
                pending: #v_wait,
            } => #ns_h1::session::RouteOutcome::AwaitBody {
                state: #v_state,
                expectation: #v_expect,
                pending: #name {
                    exchange: #v_exch,
                    route: #pending::#head(#v_wait),
                },
            },
        }
    };
    quote! {
        let #n_resp = #n_shead.response();
        #prepare
        #guard
        #open
        let #v_result = #invoke;
        return match #v_result {
            #ns_h1::session::RouteOutcome::AwaitHead { pending: #v_wait, .. }
            | #ns_h1::session::RouteOutcome::AwaitReject { pending: #v_wait, .. } => match #v_wait {},
            #ns_h1::session::RouteOutcome::Outcome(#v_value) => {
                match &#v_value {
                    #response
                    #watch
                    #ns_h1::session::Outcome::Close(_)
                    | #ns_h1::session::Outcome::Failed(_) => { #reject }
                    #ns_h1::session::Outcome::NeedMore { .. } => {}
                }
                #ns_h1::session::RouteOutcome::Outcome(#v_value)
            }
            #body
        };
    }
}
