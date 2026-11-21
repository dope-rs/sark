use handle::h1::{admit, types};

use crate::{
    app::{
        inputs,
        serve::{self, handle},
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
    pub(super) finish_settles: bool,
    pub(super) resume_arms: proc_macro2::TokenStream,
    pub(super) cancel_arms: proc_macro2::TokenStream,
    pub(super) ready_cancel_arms: proc_macro2::TokenStream,
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
        let v_wait = symbols.local("pending");
        let [v_ready, v_run, v_sess] = symbols.locals(["ready", "runtime", "session"]);
        let [v_this, v_write] = symbols.locals(["this", "write"]);
        let [turn, used, reply, connection] =
            symbols.locals(["turn", "used", "reply", "connection"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

        let state_ty = types.names.state_ty;
        let runtime = &types.routing.runtime;
        let duplex = &types.protocol.duplex_ty;
        let owner = &h1.route;
        let continuation = &h1.continuation;
        let built = &h1.ready_route;
        let evidence = quote!(#ns_core::life::Mode::body(#v_this.#f_life, #v_exch));
        let mut routes = Self::default();
        let mut has_non_fiber = false;
        for route in h1.bodies() {
            let pending = &route.prepared;
            routes.finish_settles |= !matches!(route.mode, serve::Mode::Sync);
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
                #owner::#pending(#v_wait) => #v_wait.cancel(#evidence),
            ));
            let finish = match &route.mode {
                serve::Mode::Sync => quote! {
                    #owner::#pending(#v_wait) => {
                        return match #v_wait.respond::<#state_ty, _>(
                            #v_body.body_len,
                            *#v_this.#f_state,
                            #v_write,
                            #v_date,
                            #v_this.#f_gzip,
                            #evidence,
                        ) {
                            ::core::result::Result::Ok((#reply, #connection)) => {
                                #reply.finish::<false, _, _>(#v_this.#f_life, #v_run.session_mut(), #v_exch);
                                #turn.complete(#ns_h1::session::Consumption::Buffered(#used), #reply, #connection, 0)
                            }
                            ::core::result::Result::Err(#v_error) => {
                                #v_exch.retire(#v_this.#f_life, #ns_core::life::Outcome::Rejected);
                                #turn.rejected(#v_error)
                            }
                        };
                    }
                },
                serve::Mode::Fiber { task, fiber } => {
                    routes.ready_cancel_arms.extend(quote!(
                        #built::#pending(#v_ready) => #v_ready.cancel(#evidence),
                    ));
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
                    let [finish, resume] = [
                        quote!(finish(#v_body, #v_run, #source, #evidence)),
                        quote!(resume(#v_run, #source, #evidence)),
                    ]
                    .map(|call| {
                        quote! {{
                            let #v_result = {
                                let mut #n_tasks = #tasks;
                                #v_wait.#call
                            };
                            match #v_result {
                                ::core::ops::ControlFlow::Continue(#v_result) => #v_result,
                                ::core::ops::ControlFlow::Break(#v_wait) => {
                                    return #turn.await_ready(#continuation {
                                        exchange: #v_exch,
                                        route: #built::#pending(#v_wait),
                                    }, #used);
                                }
                            }
                        }}
                    });
                    routes
                        .resume_arms
                        .extend(quote!(#built::#pending(#v_wait) => #resume,));
                    quote!(#owner::#pending(#v_wait) => #finish)
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
                                #v_body, *#v_this.#f_state,
                                #v_write, #v_date, #v_run, #source, #evidence,
                            )
                        }
                    }
                }
                serve::Mode::Duplex { family } => {
                    let wrapped = h1.duplex.wrap(*family, false, quote!(#v_sess));
                    quote! {
                        #owner::#pending(#v_wait) => #v_wait.connect::<#state_ty, #duplex, _, _>(
                            #v_body, *#v_this.#f_state, |#v_sess| #wrapped, #evidence,
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
