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

pub(super) fn complete(
    index: usize,
    generics: &crate::types::Generics,
) -> proc_macro2::TokenStream {
    let [n_shead, v_turn, v_prior, v_date] =
        generics.locals(["__sark_head", "turn", "prior", "date"]);
    let method = quote::format_ident!("__complete_head_{index}");
    quote::quote! {
        Self::#method(#n_shead, #v_date, #v_turn, #v_prior)
    }
}

pub(super) fn start(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    h1: &handle::H1Plan<'_, '_>,
    index: usize,
    parts: proc_macro2::TokenStream,
    owned: bool,
) -> proc_macro2::TokenStream {
    use quote::quote;
    let spec = ctx.plan.spec;
    let symbols = &spec.state_generics;
    let [n_raw, n_shead, v_turn] = symbols.locals(["__raw", "__sark_head", "turn"]);
    let [v_error, v_parsed] = symbols.locals(["error", "parsed"]);
    let [v_result, v_wait] = symbols.locals(["outcome", "pending"]);
    let ns_core = &paths.core;
    let ns_h1 = &paths.h1;

    let route = &ctx.plan.entries[index].entry.route;
    let operation = h1.operations[index];
    let heads = &h1.head;
    let head = &ctx.plan.entries[operation].prepared;
    let complete = complete(operation, symbols);
    let parsed = if owned {
        quote!(#v_parsed)
    } else {
        quote!(&#v_parsed)
    };
    let found = if spec.route_plan.has_params(index) {
        quote! {{
            let #n_raw = <<<#route as #ns_core::route::Spec>::Request
                as #ns_core::route::Schema>::RawParams
                as #ns_core::route::Params>::from_match(#parts);
            // SAFETY: these captures matched this route against this unchanged head.
            unsafe { #ns_h1::wire::parsing::raw::Bound::<_, #route>::new(#parsed, #n_raw) }
        }}
    } else {
        quote!(#ns_h1::wire::parsing::raw::Bound::<_, #route>::empty(#parsed))
    };
    let (start, binding, finish) = if owned {
        (
            quote!(#ns_h1::wire::parsing::framing::PendingHead::try_start(#found, |#n_shead| #complete)),
            &v_result,
            quote!(#v_result),
        )
    } else {
        (
            quote!(#ns_h1::wire::parsing::framing::Head::start(#found)),
            &n_shead,
            complete,
        )
    };
    quote! {
        return match #start {
            ::core::result::Result::Ok(::core::ops::ControlFlow::Continue(#binding)) => #finish,
            ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#v_wait)) => {
                #v_turn.await_head(#heads::#head(#v_wait))
            }
            ::core::result::Result::Err(#v_error) => #v_turn.reject_head(#v_error.into_rejection()),
        };
    }
}

pub(super) fn methods<'view, 'source, 'spec>(
    paths: &Paths,
    ctx: &handle::Context<'view, 'source, 'spec>,
    h1: &handle::H1Plan<'source, 'spec>,
    types: &types::Types<'_>,
) -> proc_macro2::TokenStream {
    use quote::quote;
    let [f_state, f_life, f_gzip] = ["state", "lifecycle", "gzip"].map(local);
    let [f_ids, f_time, f_makers, f_tasks] = ["ids", "timer", "task_producers", "tasks"].map(local);
    let symbols = &ctx.plan.spec.state_generics;
    let [n_shead, v_conn] = symbols.locals(["__sark_head", "conn"]);
    let [v_link, v_date] = symbols.locals(["connection", "date"]);
    let [v_state, v_this, v_write] = symbols.locals(["state", "this", "write"]);
    let [v_expect, v_make] = symbols.locals(["expectation", "producer"]);
    let [v_sess, v_timer, v_value] = symbols.locals(["session", "timer", "value"]);
    let [n_id, n_resp, n_seq, n_tasks] =
        symbols.locals(["__id", "__response", "__sark_id", "__tasks"]);
    let [v_exch, v_result, v_wait] = symbols.locals(["exchange", "outcome", "pending"]);
    let l_env = symbols.lifetime("env");
    let [v_turn, v_prior, v_used, v_reply] = symbols.locals(["turn", "prior", "used", "reply"]);
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    let ns_http = &paths.http;
    let state = ctx.plan.spec.state.ty();
    let wait = &types.protocol.connections.pending;
    let policy = &types.protocol.transition_policy;
    let continuation = &h1.continuation;
    let pending = &h1.route;
    let runtime = &types.routing.runtime;
    let open = terminal::open(paths, symbols);
    let [head_lt, req_lt] = symbols.lives(["__sark_head", "__sark_request"]);
    let methods = h1.heads().map(|(index, plan)| {
        let method = quote::format_ident!("__complete_head_{index}");
        let route = &plan.entry.route;
        let head = &plan.prepared;
        let admission = plan
            .entry
            .upgrade
            .then(|| quote!(, #ns_h1::route::duplex::Upgrade));
        let guard = terminal::guard(paths, symbols, plan.entry, state);
        let write = (plan.entry.upgrade || guard.is_some() || matches!(plan.mode, serve::Mode::Sync | serve::Mode::Stream { .. }))
            .then(|| quote!(let #v_write = #v_turn.output.reborrow();));
        let context = (guard.is_some() || matches!(plan.mode, serve::Mode::Sync))
            .then(|| quote!(let #n_resp = #n_shead.response();));
        let body = if plan.entry.upgrade {
            quote!(match #v_wait {})
        } else {
            quote! {
                #v_turn.await_body(
                    #v_state, #v_expect, #continuation {
                        exchange: #v_exch,
                        route: #pending::#head(#v_wait),
                    }, #v_prior,
                )
            }
        };
        let (prepare, invoke) = match &plan.mode {
            serve::Mode::Duplex { family } => {
                let wrap = h1.duplex.wrap(*family, false, quote!(#v_sess));
                let (method, write) = if plan.entry.upgrade {
                    (
                        quote!(upgrade_websocket::<#route, #state, _, _>),
                        quote!(#v_write,),
                    )
                } else {
                    (quote!(dispatch_connect::<#route, #state, _, _>), quote!())
                };
                (
                    quote!(),
                    quote! {
                        #v_conn.#method(
                            #n_shead,
                            #v_state,
                            #write
                            |#v_sess| #wrap,
                            #ns_core::life::Mode::body(#v_this.#f_life, #v_exch),
                        )
                    },
                )
            }
            serve::Mode::Sync => {
                let args = quote!(&mut #v_value, #v_state, #ns_h1::wire::output::Egress::new(#v_write, #v_date, #n_resp), #v_this.#f_gzip);
                let (frame, complete, pending) = if plan.empty_sync() {
                    (quote!(#n_shead.sync_empty(#args)), quote!((#v_used, #v_reply)), None)
                } else {
                    (
                        quote!(#n_shead.sync(&mut #v_conn.ingress, #args)),
                        quote!(#ns_h1::wire::parsing::framing::Progress::Complete((#v_used, #v_reply))),
                        Some(quote! {
                            ::core::result::Result::Ok(#ns_h1::wire::parsing::framing::Progress::Pending(#v_wait)) => {
                                let (#v_state, #v_expect, #v_wait) = #v_wait.retain(&mut #v_conn.ingress, #v_value);
                                return #body;
                            }
                        }),
                    )
                };
                (quote!(), quote! {{
                    let mut #v_value = #ns_core::life::Mode::body(#v_this.#f_life, #v_exch);
                    match #frame {
                        ::core::result::Result::Ok(#complete) => {
                            ::core::mem::drop(#v_value);
                            #v_reply.finish::<true, _, _>(#v_this.#f_life, #v_conn.async_state.session_mut(), #v_exch);
                            return #v_turn.complete(#v_used, #v_reply, #n_resp.connection(), #v_prior);
                        }
                        #pending
                        ::core::result::Result::Err(#v_result) => #v_result.into_rejection(),
                    }
                }})
            }
            serve::Mode::Fiber { task, fiber } => {
                let task = serve::Task::new(plan, task);
                let index = syn::Index::from(fiber.slot);
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
                                return #v_turn.close(#ns_http::h1::codec::canned::SERVICE_UNAVAILABLE);
                            }
                        };
                    },
                    quote! {
                        {
                            let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
                            let #v_timer = <#ns_fiber::manifold::runtime::Direct as
                                #ns_fiber::manifold::runtime::Runnable
                            >::handle(#v_this.#f_time);
                            let #v_make = #v_this.#f_makers.#index;
                            #v_conn.dispatch_task::<#route, _, _, _>(
                                #n_seq,
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
                let source = admit::stream(paths, ctx, task, runtime);
                let invoke = quote! {
                    {
                        let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
                        #v_conn.dispatch_stream::<#route, #state, _, _, _>(
                            #n_shead,
                            #v_state,
                            #v_write,
                            #v_date,
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
        let invoke = if matches!(plan.mode, serve::Mode::Sync) || plan.entry.upgrade {
            invoke
        } else {
            quote! {
                match #invoke {
                    #ns_h1::session::RouteOutcome::Outcome(#v_value) => #v_value,
                    #ns_h1::session::RouteOutcome::AwaitBody { state: #v_state, expectation: #v_expect, pending: #v_wait } => return #body,
                }
            }
        };
        let finish = if matches!(plan.mode, serve::Mode::Sync) {
            quote! {
                #ns_core::life::sequence::Mode::reject(#v_this.#f_life, #v_exch);
                #v_turn.rejected(#v_result)
            }
        } else {
            quote! {
                #v_result.settle::<true, _, _>(#v_this.#f_life, #v_conn.async_state.session_mut(), #v_exch);
                #v_turn.apply(#v_result, #v_prior)
            }
        };
        quote! {
            #[inline(always)]
            fn #method<#head_lt, #req_lt>(
                #n_shead: #ns_h1::wire::parsing::framing::Head<
                    #head_lt, #req_lt, #route #admission,
                >,
                #v_date: &[::core::primitive::u8; 29],
                #v_turn: &mut #ns_h1::session::Turn<'_, '_, 'd, #runtime, #wait, #policy, Self>,
                #v_prior: ::core::primitive::usize,
            ) -> #ns_h1::session::Flow {
                let #v_conn = &mut *#v_turn.conn;
                #write
                let #v_this = #v_turn.router.as_mut().get_mut();
                let #v_link = #v_this.id;
                let mut #v_this = #v_this.app.as_mut().project();
                let #v_state: &#l_env #state = *#v_this.#f_state;
                #context
                #prepare
                #guard
                #open
                let #v_result = #invoke;
                #finish
            }
        }
    });
    quote!(#(#methods)*)
}
