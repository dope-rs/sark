use crate::{
    app::serve::{flat, handle},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) struct Tasks {
    pub(super) drive_body: proc_macro2::TokenStream,
    pub(super) emit_body: proc_macro2::TokenStream,
    pub(super) resume_body: proc_macro2::TokenStream,
    pub(super) task_work_body: proc_macro2::TokenStream,
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
                try_wake_body: quote!(#ns_core::manifold::task::app::Wake::Idle),
                cancel_one_body: quote! {
                    match #n_task {
                        #ident::__Never(#v_never, _) => match #v_never {},
                    }
                },
            };
        }
        let phase = flat::name(ctx.plan.spec, true);
        let projection = quote::format_ident!("{}Projection", phase);
        let event = quote::format_ident!("{}Event", phase);
        let completed = quote::format_ident!("{}Completed", phase);
        let f_quotas = local("quotas");
        let has_streams = fibers.iter().any(|(_, fiber)| fiber.stream);
        let fixed_quota = fibers
            .iter()
            .filter(|(_, fiber)| !fiber.stream)
            .map(|(task, fiber)| {
                let variant = task.variant();
                let credit = fiber
                    .quota
                    .map(|index| quote!(#v_this.#f_quotas[#index] += 1;));
                quote!(#fixed::#variant(_) => { #credit })
            });
        let fixed_complete = quote! {
            match &#n_output { #(#fixed_quota,)* }
            #ns_core::manifold::task::app::Handoff::complete(#n_hand, #n_target, #n_output)
        };
        let completion = if has_streams {
            let fixed_arm = fibers.iter().any(|(_, fiber)| !fiber.stream).then(|| {
                quote! {
                    #completed::Fixed(#n_output) => { #fixed_complete }
                }
            });
            let mut end_names = Vec::new();
            let end_arms: Vec<_> = fibers.iter().filter(|(_, fiber)| fiber.stream).filter_map(|(task, fiber)| {
                let end = flat::end(*task);
                if end_names.contains(&end) { return None; }
                end_names.push(end.clone());
                let credit = fiber.quota.map(|index| quote!(#v_this.#f_quotas[#index] += 1;));
                Some(quote! {
                    #completed::#end => {
                        #credit
                        #ns_core::manifold::task::app::Handoff::ready(#n_hand, #n_target,
                            #ns_core::manifold::task::response::Event::<::core::convert::Infallible>::End)
                    }
                })
            }).collect();
            quote!(match #n_output { #fixed_arm #(#end_arms,)* })
        } else {
            fixed_complete
        };
        let yielded = if has_streams {
            let failures = fibers.iter().filter(|(_, fiber)| fiber.stream).map(|(task, _)| {
                let error = task.phase("Error");
                quote!(#event::#error(#n_output) => #ns_core::manifold::task::app::Handoff::ready(
                    #n_hand, #n_target, #ns_core::manifold::task::response::Event::Failed(#n_output)),)
            });
            quote! {
                match #n_output {
                    #event::Head => #ns_core::manifold::task::app::Handoff::ready(#n_hand, #n_target,
                        #ns_core::manifold::task::response::Event::<::core::convert::Infallible>::Head),
                    #event::Chunk(#n_output) => #ns_core::manifold::task::app::Handoff::ready(#n_hand, #n_target,
                        #ns_core::manifold::task::response::Event::<::core::convert::Infallible>::Chunk(#n_output)),
                    #(#failures)*
                }
            }
        } else {
            quote!(match #n_output {})
        };
        let yielded = if has_streams {
            quote!(#ns_core::manifold::task::app::Turn::Ready({ #yielded }, #n_ticket))
        } else {
            yielded
        };
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
            let (#n_seen, #n_ticket) = #n_ready.step(#n_driver);
            match #n_seen {
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Pending { target: #n_target } => {
                    #ns_core::manifold::task::app::Handoff::pending(#n_hand, #n_target);
                    #ns_core::manifold::task::app::Turn::Pending(#n_ticket)
                }
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::PendingReady { target: #n_target } => {
                    #ns_core::manifold::task::app::Handoff::pending(#n_hand, #n_target);
                    #ns_core::manifold::task::app::Turn::PendingReady(#n_ticket)
                }
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Yielded { target: #n_target, output: #n_output } =>
                    { #yielded },
                #ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Outcome::Complete { target: #n_target, output: #n_output } =>
                    #ns_core::manifold::task::app::Turn::Ready({ #completion }, #n_ticket),
            }
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
        let n_entry = symbols.local("__entry");
        let remove = flat::remove(
            ctx.plan.spec,
            ctx.exec,
            true,
            &n_entry,
            quote!(#v_this.#f_quotas),
        );
        let one = quote! {
            let mut #v_this = self.project().#f_core.project();
            let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
            if let Ok(mut #n_entry) = #n_tasks.entry(#n_task) { #remove }
        };
        let emit = fibers.iter().filter(|(_, fiber)| fiber.stream).map(|(task, _)| {
            let head = task.phase("Head");
            let body = task.phase("Body");
            let end = flat::end(*task);
            quote! {
                #projection::#head(#n_output, _) => {
                    // SAFETY: Head is unpinned; replace it immediately before any fallible operation.
                    let #n_output = unsafe {
                        let #n_output = ::core::ptr::read(#n_output);
                        ::core::ptr::write(#n_ready.as_mut().get_unchecked_mut(), #phase::#end { held: true });
                        #n_output
                    };
                    match #n_output.emit(#n_make) {
                        Ok((#n_output, #n_seen, #n_ticket)) => {
                            let #n_seen = match #n_seen {
                                Some(#n_seen) => {
                                    #n_ready.set(#phase::#body(#n_seen, true));
                                    #ns_core::manifold::task::response::State::Streaming
                                }
                                None => #ns_core::manifold::task::response::State::Complete(#n_ticket),
                            };
                            #ns_core::manifold::task::response::Access::Ready(Ok((#n_output, #n_seen)))
                        }
                        Err(#n_output) => #ns_core::manifold::task::response::Access::Ready(Err(#n_output)),
                    }
                }
            }
        });
        let access = |result: proc_macro2::TokenStream| {
            quote! {
                match #result {
                    Ok(#n_ready) => #ns_core::manifold::task::response::Access::Ready(#n_ready),
                    Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Active) =>
                        #ns_core::manifold::task::response::Access::Active,
                    Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Absent) =>
                        #ns_core::manifold::task::response::Access::Absent,
                }
            }
        };
        let resumption = access(
            quote!(self.project().#f_core.project().#f_tasks.as_mut().scope().resume(#n_task)),
        );
        let emission = quote! {
            let mut #v_this = self.project().#f_core.project();
            let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
            match #n_tasks.get_mut(#n_task) {
                Ok(mut #n_ready) => match #n_ready.as_mut().project() {
                    #(#emit,)*
                    _ => #ns_core::manifold::task::response::Access::Active,
                },
                Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Active) =>
                    #ns_core::manifold::task::response::Access::Active,
                Err(#ns_fiber::dope_fiber::task::storage::fixed::schedule::result::Error::Absent) =>
                    #ns_core::manifold::task::response::Access::Absent,
            }
        };
        Self {
            drive_body: drive,
            emit_body: emission,
            resume_body: resumption,
            task_work_body: work,
            try_wake_body: probe,
            cancel_one_body: one,
        }
    }
}
