use crate::{
    app::serve::{flat, handle},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(in crate::app::serve::handle::h1) struct Tasks {
    pub(in crate::app::serve::handle::h1) send_body: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle::h1) wake_body: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle::h1) close_body: proc_macro2::TokenStream,
}

impl Tasks {
    pub(super) fn build(
        paths: &Paths,
        ctx: &handle::Context<'_, '_, '_>,
        dispatch: &syn::Ident,
    ) -> Self {
        let symbols = &ctx.plan.spec.state_generics;
        let v_routes = symbols.local("routes");
        let [n_drive, n_prog] = symbols.locals(["__drive", "__progress"]);
        let [v_link, v_date, v_driver] = symbols.locals(["connection", "date", "driver"]);
        let ns_h1 = &paths.h1;
        use quote::quote;

        if ctx.exec.tasks().is_empty() {
            return Self {
                send_body: quote! {
                    let mut #v_routes = #dispatch {
                        app: self,
                        id: #v_link.id(),
                        date: #v_date,
                    };
                    let #n_drive = #ns_h1::server::conn::driver::progress(
                        ::core::pin::Pin::new(&mut #v_routes),
                        #v_link,
                        #v_driver,
                    );
                    let (_, #n_prog) = #n_drive.into_parts();
                    #n_prog
                },
                wake_body: quote! {},
                close_body: quote! {},
            };
        }
        let f_life = local("lifecycle");
        let [v_cancel, n_active] = symbols.locals(["cancellation", "__activity"]);
        let [n_charge, n_owner, n_flow] = symbols.locals(["__charge", "__owner", "__flow"]);
        let [n_exch] = symbols.locals(["__exchange"]);
        let [n_live] = symbols.locals(["__running"]);
        let [n_tag, n_task] = symbols.locals(["__tag", "__task"]);
        let [n_turn, n_tasks] = symbols.locals(["__task_turn", "__tasks"]);
        let [v_active] = symbols.locals(["running"]);
        let [v_staged, v_task] = symbols.locals(["staged", "task"]);
        let [v_this] = symbols.locals(["this"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let f_tasks = local("tasks");
        let f_quotas = local("quotas");
        let quota = ctx
            .exec
            .tasks()
            .iter()
            .any(|task| task.quota().is_some())
            .then(|| quote!(#v_this.#f_quotas,));
        let view = quote!((#v_link.state()));
        let state = quote!((#v_link.state_mut()));
        let pump = quote! {
            let #n_exch = #ns_core::life::sequence::Mode::current(
                #v_this.#f_life,
                #view.async_state.session(),
                #v_link.id(),
            );
            if let ::core::option::Option::Some(#n_exch) = #n_exch
                && let ::core::option::Option::Some(#n_live) = #state.async_state.take()
            {
                let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
                let #n_turn = Self::__task(
                    &mut #n_tasks, #quota #n_live, #v_date, #v_link, #v_driver, #v_this.#f_life, #n_exch,
                );
                #n_turn.settle(
                    #v_this.#f_life,
                    #state.async_state.session_mut(),
                    #n_exch,
                );
            }
        };
        let body = quote! {
            if ::core::matches!(
                #ns_h1::pump::Tasks::activity(&#view.async_state),
                #ns_h1::pump::Activity::Stream
            ) {
                let mut #v_this = self.as_mut().project();
                #pump
                if !::core::matches!(
                    #ns_h1::pump::Tasks::activity(&#view.async_state),
                    #ns_h1::pump::Activity::Idle
                ) {
                    return #ns_h1::server::deadline::Progress::default();
                }
            }
            let #n_drive = {
                let mut #v_routes = #dispatch {
                    app: self.as_mut(),
                    id: #v_link.id(),
                    date: #v_date,
                };
                #ns_h1::server::conn::driver::progress(
                    ::core::pin::Pin::new(&mut #v_routes),
                    #v_link,
                    #v_driver,
                )
            };
            let #n_active = #ns_h1::pump::Tasks::activity(&#view.async_state);
            let (#n_flow, #n_prog) = #n_drive.into_parts();
            if #n_flow.is_runnable(#n_active) {
                self.activate(#v_date, #v_link, #v_driver);
            }
            #n_prog
        };
        let wake = quote! {
            let mut #v_this = self.project();
            match #ns_h1::pump::Tasks::activity(&#view.async_state) {
                #ns_h1::pump::Activity::Idle => {}
                #ns_h1::pump::Activity::Stream
                    if ::core::matches!(
                        #v_link.write().pending(),
                        #ns_fiber::dope::net::link::egress::Pending::Present
                    ) => {}
                #ns_h1::pump::Activity::Task | #ns_h1::pump::Activity::Stream => {
                    #pump
                }
            }
        };
        let staged = flat::staged(ctx.plan.spec, ctx.exec, true, quote!(#v_task.get()));
        let remove = flat::remove(
            ctx.plan.spec,
            ctx.exec,
            false,
            &v_task,
            quote!(#v_this.#f_quotas),
        );
        let cancel = quote! {
            match #n_tasks.entry(#n_task) {
                Ok(mut #v_task) => {
                    let #v_staged = #staged;
                    #remove
                    if #v_staged { #ns_h1::server::task::turn::Turn::StagedCancelled }
                    else { #ns_h1::server::task::turn::Turn::ConnectionCancelled }
                }
                Err(_) => #ns_h1::server::task::turn::Turn::ConnectionCancelled,
            }
        };
        let closing = quote! {
            match #n_tasks.entry(#n_task) {
                Ok(mut #v_task) => { #remove #ns_h1::server::task::turn::Turn::StagedCancelled }
                Err(_) => #ns_h1::server::task::turn::Turn::ConnectionCancelled,
            }
        };
        let close = quote! {
            let mut #v_this = self.project();
            let #n_exch = #ns_core::life::sequence::Mode::current(
                #v_this.#f_life,
                #view.async_state.session(),
                #v_link.id(),
            );
            if let ::core::option::Option::Some(#v_staged) =
                #state.async_state.take_stream()
            {
                let #n_tag = #v_staged.release();
                if let ::core::option::Option::Some((#n_owner, #n_charge)) =
                    #ns_core::life::sequence::Mode::transport(
                        #v_this.#f_life,
                        #v_link.id(),
                        #n_tag,
                    )
                {
                    #ns_core::life::sequence::Mode::unstage(
                        #v_this.#f_life,
                        #state.async_state.session_mut(),
                        #n_owner,
                        #n_charge,
                    );
                }
            }
            if let ::core::option::Option::Some(#v_active) = #state.async_state.take() {
                let mut #n_tasks = #v_this.#f_tasks.as_mut().scope();
                let #v_cancel = match #v_active {
                    #ns_h1::server::task::runtime::Running::Future(_, #n_task)
                    | #ns_h1::server::task::runtime::Running::Head(_, #n_task)
                    | #ns_h1::server::task::runtime::Running::Body(#n_task) => { #cancel }
                    #ns_h1::server::task::runtime::Running::Closing(#n_task) => { #closing }
                };
                if let ::core::option::Option::Some(#n_exch) = #n_exch {
                    #v_cancel.settle(
                        #v_this.#f_life,
                        #state.async_state.session_mut(),
                        #n_exch,
                    );
                }
            }
        };
        Self {
            send_body: body,
            wake_body: wake,
            close_body: close,
        }
    }
}
