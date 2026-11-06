use crate::{
    app::serve::{handle, layout},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) struct Tasks {
    pub(super) drive_body: proc_macro2::TokenStream,
    pub(super) emit_body: proc_macro2::TokenStream,
    pub(super) resume_body: proc_macro2::TokenStream,
    pub(super) task_work_body: proc_macro2::TokenStream,
    pub(super) wake_body: proc_macro2::TokenStream,
    pub(super) try_wake_body: proc_macro2::TokenStream,
    pub(super) cancel_one_body: proc_macro2::TokenStream,
}

impl Tasks {
    /// Builds only protocol-neutral task operations. No H1 plan is accepted,
    /// so the task-only call graph cannot construct or render H1 machinery.
    pub(super) fn build(paths: &Paths, ctx: &handle::Context<'_, '_, '_>) -> Self {
        let [f_core, f_tasks] = ["core", "tasks"].map(local);
        let symbols = &ctx.plan.spec.state_generics;
        let [n_ready, n_driver] = symbols.locals(["__admitted", "__driver"]);
        let [n_hand, n_seen] = symbols.locals(["__handoff", "__observed"]);
        let [n_output, n_ticket] = symbols.locals(["__output", "__receipt"]);
        let [n_make] = symbols.locals(["__emit"]);
        let [n_scope, n_target] = symbols.locals(["__scope", "__target"]);
        let [n_task, n_tasks, n_work] = symbols.locals(["__task", "__tasks", "__work"]);
        let [v_never, v_this] = symbols.locals(["never", "this"]);
        use quote::quote;

        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;

        let ident = &ctx.exec.fiber;
        let fixed = quote::format_ident!("__{}Fixed", ctx.plan.spec.name);
        let fibers = ctx.exec.fibers();
        if fibers.is_empty() {
            let absent = quote! {
                match #n_task {
                    #ident::__Never(#v_never, _) => match *#v_never {},
                }
            };
            return Self {
                drive_body: quote!(#ns_core::manifold::task::app::Turn::Idle),
                emit_body: absent.clone(),
                resume_body: absent,
                task_work_body: quote!(false),
                wake_body: quote!(),
                try_wake_body: quote!(#ns_core::manifold::task::app::Wake::Idle),
                cancel_one_body: quote! {
                    match #n_task {
                        #ident::__Never(#v_never, _) => match #v_never {},
                    }
                },
            };
        }
        let routes = fibers.iter().map(|pair| {
            let path = pair.1.path();
            let variant = pair.0.variant();
            let (yielded, completed) = if pair.1.stream {
                (
                    quote!(#ns_core::manifold::task::app::Turn::Ready(
                        #ns_core::manifold::task::app::Handoff::ready(#n_hand, #n_target, #n_output),
                        #n_ticket,
                    )),
                    quote!(#ns_core::manifold::task::app::Handoff::ready(
                        #n_hand, #n_target, #ns_core::manifold::task::response::Event::<::core::convert::Infallible>::End,
                    )),
                )
            } else {
                (
                    quote!(match #n_output {}),
                    quote!(#ns_core::manifold::task::app::Handoff::complete(
                        #n_hand, #n_target, #fixed::#variant(#n_output),
                    )),
                )
            };
            quote! {
                |(#n_driver, #n_hand), #n_ready| {
                    let (#n_seen, #n_ticket) = #ns_fiber::dope_fiber::task::storage::fixed::schedule::selected::route::Entry::<_, #path, _, _>::step(
                        #n_ready,
                        #n_driver,
                    );
                    match #n_seen {
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Pending { target: #n_target } => {
                            #ns_core::manifold::task::app::Handoff::pending(
                                #n_hand,
                                #n_target,
                            );
                            #ns_core::manifold::task::app::Turn::Pending(#n_ticket)
                        }
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::PendingReady { target: #n_target } => {
                            #ns_core::manifold::task::app::Handoff::pending(
                                #n_hand,
                                #n_target,
                            );
                            #ns_core::manifold::task::app::Turn::PendingReady(#n_ticket)
                        }
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Yielded { target: #n_target, output: #n_output } => {
                            #yielded
                        }
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Complete { target: #n_target, output: #n_output } => {
                            #ns_core::manifold::task::app::Turn::Ready(
                                #completed,
                                #n_ticket,
                            )
                        }
                    }
                }
            }
        }).collect::<Vec<_>>();
        let routes = layout::Tree::new(&routes).value(paths);
        let drive = quote! {
            let mut #v_this = self.project().#f_core.project();
            let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
            let #n_ready = match #n_tasks.as_mut().select(#n_scope, #n_work) {
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Selection::Idle => {
                    return #ns_core::manifold::task::app::Turn::Idle;
                }
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Selection::Yielded => {
                    return #ns_core::manifold::task::app::Turn::Deferred;
                }
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Selection::Ready(#n_ready) => #n_ready,
            };
            #ns_fiber::dope_fiber::task::storage::fixed::schedule::selected::route::Routes::run(
                #routes,
                (#n_driver, #n_hand),
                #n_ready,
            )
        };
        let work = quote!(match self
            .project_ref()
            .#f_core
            .project_ref()
            .#f_tasks
            .as_pin_ref()
            .owners()
            .readiness(#n_scope)
        {
            #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Ready
            | #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Pending => true,
            #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Idle => false,
        });
        let wake = quote! {
            self.project()
                .#f_core
                .project()
                .#f_tasks
                .as_mut()
                .scope()
                .as_mut()
                .wake(#n_scope);
        };
        let probe = quote! {
            match self.project()
                .#f_core
                .project()
                .#f_tasks
                .as_mut()
                .scope()
                .as_mut()
                .try_wake(#n_scope)
            {
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Ready
                | #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Pending => {
                    #ns_core::manifold::task::app::Wake::Ready
                }
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Wake::Idle => {
                    #ns_core::manifold::task::app::Wake::Idle
                }
            }
        };
        let cancel = fibers.iter().map(|(task, fiber)| {
            let var = task.variant();
            let path = fiber.path();
            quote! {
                #ident::#var(#n_task) => {
                    #v_this.#f_tasks
                        .as_mut()
                        .scope()
                        .into_route::<#path>()
                        .remove(#n_task);
                }
            }
        });
        let one = quote! {
            let mut #v_this = self.project().#f_core.project();
            match #n_task {
                #( #cancel, )*
            }
        };
        let emit = fibers.iter().map(|(task, fiber)| {
            let var = task.variant();
            let path = fiber.path();
            let emit = if fiber.stream {
                quote! {
                    match #v_this.#f_tasks.as_mut().scope().into_route::<#path>()
                        .get_mut(#n_task).map(|#n_task| #n_task.head(|#n_output| {
                            match #n_output.emit(#n_make) {
                                Ok((#n_ready, #n_output, #n_seen)) => {
                                    let #n_seen = if #n_output.is_some() {
                                        #ns_core::manifold::task::response::State::Streaming
                                    } else {
                                        #ns_core::manifold::task::response::State::Complete(#n_seen)
                                    };
                                    (#n_output, Ok((#n_ready, #n_seen)))
                                }
                                Err(#n_output) => (None, Err(#n_output)),
                            }
                        }))
                    {
                        Ok(Some(#n_ready)) => #ns_core::manifold::task::response::Access::Ready(#n_ready),
                        Ok(None) | Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Active) =>
                            #ns_core::manifold::task::response::Access::Active,
                        Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Absent) =>
                            #ns_core::manifold::task::response::Access::Absent,
                    }
                }
            } else {
                quote!(#ns_core::manifold::task::response::Access::Absent)
            };
            quote!(#ident::#var(#n_task) => { #emit },)
        });
        let emission = quote! {
            let mut #v_this = self.project().#f_core.project();
            match #n_task {
                #( #emit )*
            }
        };
        let resume = fibers.iter().map(|(task, fiber)| {
            let var = task.variant();
            let path = fiber.path();
            let resume = if fiber.stream {
                quote! {
                    match #v_this.#f_tasks.as_mut().scope().into_route::<#path>().resume(#n_task) {
                        Ok(()) => #ns_core::manifold::task::response::Access::Ready(()),
                        Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Active) =>
                            #ns_core::manifold::task::response::Access::Active,
                        Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Absent) =>
                            #ns_core::manifold::task::response::Access::Absent,
                    }
                }
            } else {
                quote!(#ns_core::manifold::task::response::Access::Absent)
            };
            quote!(#ident::#var(#n_task) => { #resume },)
        });
        let resumption = quote! {
            let mut #v_this = self.project().#f_core.project();
            match #n_task {
                #( #resume )*
            }
        };
        Self {
            drive_body: drive,
            emit_body: emission,
            resume_body: resumption,
            task_work_body: work,
            wake_body: wake,
            try_wake_body: probe,
            cancel_one_body: one,
        }
    }
}
