use handle::h1::{admit, types};

use crate::{
    app::{
        inputs,
        serve::{self, handle},
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

#[derive(Default)]
pub(super) struct Routes {
    pub(super) body_data_arms: proc_macro2::TokenStream,
    pub(super) body_work_arms: proc_macro2::TokenStream,
    pub(super) pending_resume_arms: proc_macro2::TokenStream,
    pub(super) finish_arms: proc_macro2::TokenStream,
    pub(super) resume_arms: proc_macro2::TokenStream,
    pub(super) cancel_arms: proc_macro2::TokenStream,
    pub(super) ready_cancel: proc_macro2::TokenStream,
}

impl Routes {
    pub(super) fn new<'view, 'source, 'spec>(
        paths: &Paths,
        context: &handle::Context<'view, 'source, 'spec>,
        h1: &handle::H1Plan<'source, 'spec>,
        types: &types::Types<'_>,
    ) -> Self {
        let [f_state, f_life] = ["state", "lifecycle"].map(local);
        let [f_gzip, f_time, f_makers] = ["gzip", "timer", "task_producers"].map(local);
        let symbols = &context.plan.spec.state_generics;
        let [n_tasks, v_body, v_date] = symbols.locals(["__tasks", "body", "date"]);
        let [v_error, v_exch, v_result] = symbols.locals(["error", "exchange", "outcome"]);
        let [v_output, v_wait, v_permit] = symbols.locals(["output", "pending", "permit"]);
        let [v_ready, v_run, v_sess] = symbols.locals(["ready", "runtime", "session"]);
        let [v_this, v_write] = symbols.locals(["this", "write"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

        let state_ty = types.names.state_ty;
        let runtime = &types.routing.runtime;
        let duplex = &types.protocol.duplex_ty;
        let cont = &h1.continuations;
        let owner = cont.route();
        let ready = cont.ready();
        let built = cont.ready_route();
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
        let evidence = quote!(#ns_core::life::Mode::body(#v_this.#f_life, #v_exch));
        let mut routes = Self {
            ready_cancel: cleanup,
            ..Self::default()
        };
        let mut has_non_fiber = false;
        for route in cont.bodies() {
            let pending = &route.pending_variant;
            if let serve::Mode::Fiber { fiber, .. } = &route.mode {
                let feed = match fiber.body {
                    inputs::Body::Discarded => quote!(try_discard),
                    inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => {
                        quote!(try_retain)
                    }
                };
                routes
                    .body_work_arms
                    .extend(quote!(#owner::#pending(ref #v_wait) => #v_wait.has_work(),));
                routes.pending_resume_arms.extend(quote!(
                    #owner::#pending(#v_wait) => #v_wait.resume(#evidence).map(#owner::#pending),
                ));
                routes.body_data_arms.extend(quote!(
                    #owner::#pending(#v_wait) => #v_wait.#feed(#v_body, #evidence).map(#owner::#pending),
                ));
            } else {
                has_non_fiber = true;
                routes.body_data_arms.extend(quote!(
                    #owner::#pending(mut #v_wait) => match #v_wait.push_body(#v_body, #evidence) {
                        ::core::result::Result::Ok(()) => ::core::result::Result::Ok(#owner::#pending(#v_wait)),
                        ::core::result::Result::Err(#v_error) => {
                            #v_wait.cancel(#evidence);
                            let #v_error = #ns_h1::wire::Failure::Assembly(#v_error);
                            #ns_core::life::Mode::diagnostic(#v_this.#f_life, &#v_error);
                            ::core::result::Result::Err(#v_error.body())
                        }
                    },
                ));
            }
            routes.cancel_arms.extend(quote!(
                #owner::#pending(#v_wait) => {
                    #v_wait.cancel(#evidence);
                    #ns_core::life::Outcome::Cancelled
                },
            ));
            let finish = match &route.mode {
                serve::Mode::Sync => quote! {
                    #owner::#pending(#v_wait) => {
                        let #v_output = #ns_h1::wire::output::Egress::new(
                            #v_write, #v_date, #v_wait.response(),
                        );
                        #v_wait.respond::<#state_ty, _, _>(
                            #v_body,
                            #v_permit,
                            *#v_this.#f_state,
                            #ns_h1::route::sync::Egress::new(#v_output, #v_this.#f_gzip),
                            #evidence,
                        )
                    }
                },
                serve::Mode::Fiber { task, fiber } => {
                    let task = serve::Task::new(route, task);
                    let prod = syn::Index::from(fiber.slot);
                    let f_tasks = local("tasks");
                    let tasks = quote!(#v_this.#f_tasks.as_mut().scope());
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
                    routes.resume_arms.extend(quote! {
                        #built::#pending(#v_wait) => {
                            let mut #n_tasks = #tasks;
                            let #v_result = #v_wait.resume(
                                #v_permit,
                                #v_run,
                                #source,
                                #evidence,
                            );
                            #settle
                        },
                    });
                    quote! {
                        #owner::#pending(#v_wait) => {
                            let mut #n_tasks = #tasks;
                            let #v_result = #v_wait.finish(
                                #v_body,
                                #v_permit,
                                #v_run,
                                #source,
                                #evidence,
                            );
                            #settle
                        }
                    }
                }
                serve::Mode::Stream { task } => {
                    let task = serve::Task::new(route, task);
                    let f_tasks = local("tasks");
                    let tasks = quote!(#v_this.#f_tasks.as_mut().scope());
                    let source = admit::stream(paths, context, task, runtime);
                    quote! {
                        #owner::#pending(#v_wait) => {
                            let mut #n_tasks = #tasks;
                            #v_wait.stream::<#state_ty, _, _, _, _, _>(
                                #v_body, #v_permit, *#v_this.#f_state,
                                #v_write, #v_date, #v_run, #source, #evidence,
                            )
                        }
                    }
                }
                serve::Mode::Duplex { family } => {
                    let wrapped = h1.duplex.wrap(*family, false, quote!(#v_sess));
                    quote! {
                        #owner::#pending(#v_wait) => #v_wait.connect::<#state_ty, #duplex, _, _>(
                            #v_body, #v_permit, *#v_this.#f_state, |#v_sess| #wrapped, #evidence,
                        )
                    }
                }
            };
            routes.finish_arms.extend([finish, quote!(,)]);
        }
        if has_non_fiber {
            routes.body_work_arms.extend(quote!(_ => false,));
            routes
                .pending_resume_arms
                .extend(quote!(#v_wait => ::core::result::Result::Ok(#v_wait),));
        }
        routes
    }
}
