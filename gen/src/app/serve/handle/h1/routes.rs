use handle::h1::{admit, types};

use crate::{
    app::{
        serve::{self, handle},
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) struct Routes {
    pub(super) body_data_arms: Vec<proc_macro2::TokenStream>,
    pub(super) body_work_arms: Vec<proc_macro2::TokenStream>,
    pub(super) pending_resume_arms: Vec<proc_macro2::TokenStream>,
    pub(super) finish_arms: Vec<proc_macro2::TokenStream>,
    pub(super) resume_arms: Vec<proc_macro2::TokenStream>,
    pub(super) cancel_arms: Vec<proc_macro2::TokenStream>,
    pub(super) ready_cancel: proc_macro2::TokenStream,
}

impl Routes {
    pub(super) fn new<'view, 'source, 'spec>(
        paths: &Paths,
        context: &handle::Context<'view, 'source, 'spec>,
        h1: &handle::H1Plan<'source, 'spec>,
        types: &types::Types<'_>,
    ) -> Self {
        let [f_state, f_life, f_cache] = ["state", "lifecycle", "response_cache"].map(local);
        let [f_gzip, f_time, f_makers] = ["gzip", "timer", "task_producers"].map(local);
        let symbols = &context.plan.spec.state_generics;
        let [n_tasks, v_body, v_date] = symbols.locals(["__tasks", "body", "date"]);
        let [v_error, v_exch, v_exec] = symbols.locals(["error", "exchange", "execution"]);
        let [v_finish, v_result] = symbols.locals(["finish", "outcome"]);
        let [v_output, v_wait, v_permit] = symbols.locals(["output", "pending", "permit"]);
        let [v_ready, v_run, v_sess] = symbols.locals(["ready", "runtime", "session"]);
        let [v_this, v_write] = symbols.locals(["this", "write"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

        let state_ty = types.names.state_ty;
        let task_id = &context.exec.identity;
        let runtime = &types.routing.runtime;
        let duplex = &types.protocol.duplex_ty;
        let cont = &h1.continuations;
        let owner = cont.route();
        let ready = cont.ready();
        let built = cont.ready_route();
        let work: Vec<_> = cont
            .bodies()
            .map(|route| {
                let variant = &route.pending_variant;
                match &route.mode {
                    serve::Mode::Fiber { .. } => quote! {
                        #owner::#variant(ref #v_wait) => #v_wait.has_work()
                    },
                    _ => quote! { #owner::#variant(_) => false },
                }
            })
            .collect();
        let pending: Vec<_> = cont
            .bodies()
            .map(|route| {
                let variant = &route.pending_variant;
                match &route.mode {
                    serve::Mode::Fiber { .. } => quote! {
                        #owner::#variant(#v_wait) => #v_wait
                            .resume(#ns_core::life::Mode::body(#v_this.#f_life, #v_exch))
                            .map(#owner::#variant)
                    },
                    _ => quote! {
                        #owner::#variant(#v_wait) =>
                            ::core::result::Result::Ok(#owner::#variant(#v_wait))
                    },
                }
            })
            .collect();
        let body: Vec<_> = cont
            .bodies()
            .map(|route| {
                let variant = &route.pending_variant;
                match &route.pending_flow {
                    serve::Flow::Discard | serve::Flow::Retain => {
                        let method = if matches!(route.pending_flow, serve::Flow::Discard) {
                            quote!(try_discard)
                        } else {
                            quote!(try_retain)
                        };
                        quote! {
                            #owner::#variant(#v_wait) => {
                                #v_wait
                                    .#method(
                                        #v_body,
                                        #ns_core::life::Mode::body(
                                            #v_this.#f_life,
                                            #v_exch,
                                        ),
                                    )
                                    .map(#owner::#variant)
                            }
                        }
                    }
                    serve::Flow::Push => {
                        quote! {
                            #owner::#variant(mut #v_wait) => {
                                match #v_wait.push_body(
                                        #v_body,
                                        #ns_core::life::Mode::body(
                                            #v_this.#f_life,
                                            #v_exch,
                                        ),
                                    ) {
                                    ::core::result::Result::Ok(()) =>
                                        ::core::result::Result::Ok(#owner::#variant(#v_wait)),
                                    ::core::result::Result::Err(#v_error) => {
                                        #v_wait.cancel(#ns_core::life::Mode::body(
                                            #v_this.#f_life,
                                            #v_exch,
                                        ));
                                        let #v_error = #ns_h1::wire::Failure::Assembly(#v_error);
                                        #ns_core::life::Mode::diagnostic(#v_this.#f_life, &#v_error);
                                        ::core::result::Result::Err(#v_error.body())
                                    }
                                }
                            }
                        }
                    }
                }
            })
            .collect();
        let mut resume = Vec::new();
        let finish: Vec<_> = cont
            .bodies()
            .map(|route| {
                let pending = &route.pending_variant;
                match &route.mode {
                    serve::Mode::Sync { cache } => {
                        let cache = syn::Index::from(*cache);
                        quote! {
                            #owner::#pending(#v_wait) => {
                                let #v_output = #ns_h1::wire::output::Egress::new(
                                    #v_write, #v_date, #v_wait.response(),
                                );
                                #v_wait.finish::<#state_ty, _, _>(
                                    #v_body,
                                    #v_permit,
                                    *#v_this.#f_state,
                                    #ns_h1::route::sync::Egress::new(
                                        #v_output,
                                        #ns_h1::wire::cache::Cache::new(
                                            &mut #v_this.#f_cache[#cache],
                                        ),
                                        #v_this.#f_gzip,
                                    ),
                                    #ns_core::life::Mode::body(
                                        #v_this.#f_life,
                                        #v_exch,
                                    ),
                                )
                            }
                        }
                    }
                    serve::Mode::Fiber { task, fiber } => {
                        let task = serve::Task::new(route, task);
                        let prod = syn::Index::from(fiber.slot);
                        let tasks = task.path().route(&context.plan.spec.state_generics);
                        let source = admit::fiber(
                            paths,
                            context,
                            task,
                            runtime,
                            quote!(*#v_this.#f_state),
                            quote! {
                                <#ns_fiber::manifold::runtime::Direct as
                                    #ns_fiber::manifold::runtime::Runnable
                                >::handle(#v_this.#f_time)
                            },
                            quote!(#v_this.#f_makers.#prod),
                        );
                        let settle = quote! {
                            match #v_result {
                                #ns_h1::session::BodyOutcome::Complete(#v_result) => #v_result,
                                #ns_h1::session::BodyOutcome::Pending(#v_wait) => {
                                    return #ns_h1::session::BodyOutcome::Pending(#ready {
                                        exchange: #v_exch,
                                        route: #built::#pending(#v_wait),
                                    });
                                }
                            }
                        };
                        resume.push(quote! {
                            #built::#pending(#v_wait) => {
                                let mut #n_tasks = #tasks;
                                let #v_result = #v_wait.resume(
                                    #v_permit,
                                    #v_run,
                                    #source,
                                    #ns_core::life::Mode::body(#v_this.#f_life, #v_exch),
                                );
                                #settle
                            }
                        });
                        quote! {
                            #owner::#pending(#v_wait) => {
                                let mut #n_tasks = #tasks;
                                let #v_result = #v_wait.finish(
                                    #v_body,
                                    #v_permit,
                                    #v_run,
                                    #source,
                                    #ns_core::life::Mode::body(
                                        #v_this.#f_life,
                                        #v_exch,
                                    ),
                                );
                                #settle
                            }
                        }
                    }
                    serve::Mode::Stream { task } => {
                        let task = serve::Task::new(route, task);
                        let tasks = task.path().route(&context.plan.spec.state_generics);
                        let source = admit::stream(
                            paths,
                            &context.plan.spec.state_generics,
                            task_id,
                            runtime,
                            &task.variant(),
                        );
                        quote! {
                            #owner::#pending(#v_wait) => {
                                let mut #n_tasks = #tasks;
                                let #v_output = #ns_h1::wire::output::Egress::new(
                                    #v_write, #v_date, #v_wait.response(),
                                );
                                let #v_exec = #ns_h1::route::stream::Execution::new(
                                    #v_output,
                                    *#v_this.#f_state,
                                    #v_run,
                                    #source,
                                );
                                let #v_result = #v_wait.finish::<
                                    #state_ty,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                >(
                                    #v_body,
                                    #v_permit,
                                    #v_exec,
                                    #ns_core::life::Mode::body(
                                        #v_this.#f_life,
                                        #v_exch,
                                    ),
                                );
                                #v_result
                            }
                        }
                    }
                    serve::Mode::Duplex { family } => {
                        let wrapped = h1.duplex.wrap(*family, false, quote!(#v_sess));
                        quote! {
                            #owner::#pending(#v_wait) => {
                                let #v_finish = (
                                    *#v_this.#f_state,
                                    |#v_sess| #wrapped,
                                );
                                #v_wait.finish::<#state_ty, #duplex, _, _>(
                                    #v_body,
                                    #v_permit,
                                    #v_finish,
                                    #ns_core::life::Mode::body(
                                        #v_this.#f_life,
                                        #v_exch,
                                    ),
                                )
                            }
                        }
                    }
                }
            })
            .collect();
        let cancel: Vec<_> = cont
            .bodies()
            .map(|route| {
                let pending = &route.pending_variant;
                quote! {
                        #owner::#pending(#v_wait) => {
                            #v_wait.cancel(
                                #ns_core::life::Mode::body(
                                    #v_this.#f_life,
                                    #v_exch,
                                ),
                            );
                            #ns_core::life::Outcome::Cancelled
                        }
                }
            })
            .collect();
        let cleanup = if context.exec.fibers().is_empty() {
            quote!(match #v_ready {})
        } else {
            let arms = context
                .plan
                .entries
                .iter()
                .filter(|route| route.kind == spec::Kind::Fiber)
                .map(|route| {
                    let variant = &route.pending_variant;
                    quote! {
                        #built::#variant(#v_ready) => #v_ready.cancel(
                            #ns_core::life::Mode::body(#v_this.#f_life, #v_exch),
                        ),
                    }
                });
            quote! {
                let #v_exch = #v_ready.exchange;
                match #v_ready.route { #( #arms )* }
                #v_exch.retire(#v_this.#f_life, #ns_core::life::Outcome::Cancelled);
            }
        };
        Self {
            body_data_arms: body,
            body_work_arms: work,
            pending_resume_arms: pending,
            finish_arms: finish,
            resume_arms: resume,
            cancel_arms: cancel,
            ready_cancel: cleanup,
        }
    }
}
