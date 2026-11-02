use crate::{app::serve::handle, macros::tokens::local, runtime::paths::Paths};

pub(in crate::app::serve::handle::h1) struct Tasks {
    pub(in crate::app::serve::handle::h1) send_body: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle::h1) wake_body: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle::h1) close_body: proc_macro2::TokenStream,
}

impl Tasks {
    pub(super) fn build(paths: &Paths, ctx: &handle::Context<'_, '_, '_>) -> Self {
        let f_life = local("lifecycle");
        let symbols = &ctx.plan.spec.state_generics;
        let [v_cancel, n_active] = symbols.locals(["cancellation", "__activity"]);
        let [n_charge, n_drive] = symbols.locals(["__charge", "__drive"]);
        let [n_exch, n_kind] = symbols.locals(["__exchange", "__kind"]);
        let [n_owner, n_prog] = symbols.locals(["__owner", "__progress"]);
        let [n_resp, n_live] = symbols.locals(["__response", "__running"]);
        let [n_tag, n_task] = symbols.locals(["__tag", "__task"]);
        let [n_turn, n_tasks] = symbols.locals(["__task_turn", "__tasks"]);
        let [v_link, v_date, v_driver] = symbols.locals(["connection", "date", "driver"]);
        let [v_kind, v_active, v_slab] = symbols.locals(["kind", "running", "slab"]);
        let [v_staged, v_task, v_tdate] = symbols.locals(["staged", "task", "task_date"]);
        let [v_this] = symbols.locals(["this"]);
        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_h1 = &paths.h1;
        use quote::quote;

        let tasks = ctx.exec.tasks();
        let fibers = ctx.exec.fibers();
        let streams = ctx.exec.streams();
        let ident = &ctx.exec.task.identity;
        let view = quote!((#v_link.state()));
        let state = quote!((#v_link.state_mut()));
        let farms = fibers.iter().map(|pair| {
            let task = pair.0;
            let var = task.variant();
            let store = task.path().route(&ctx.plan.spec.state_generics);
            quote! {
                    #ident::#var(#n_task) => {
                        let mut #n_tasks = #store;
                        let #v_tdate = #v_date.load();
                        #ns_fiber::manifold::H1::new(
                            #n_tasks.as_mut(),
                            #n_task,
                            #n_resp,
                        ).poll(
                            &#v_tdate,
                            #ident::#var,
                            #v_link,
                            #v_driver,
                            #v_this.#f_life,
                            #n_exch,
                        )
                    },
            }
        });
        let sarms = streams.iter().map(|task| {
            let var = task.variant();
            let store = task.path().route(&ctx.plan.spec.state_generics);
            quote! {
                    #ident::#var(#n_task) => {
                        let mut #n_tasks = #store;
                        #ns_fiber::manifold::H1::new(
                            #n_tasks.as_mut(),
                            #n_task,
                            #n_resp,
                        ).stream(
                            #n_kind,
                            #ident::#var,
                            #v_link,
                            #v_driver,
                            #v_this.#f_life,
                            #n_exch,
                        )
                    },
            }
        });
        let arms: Vec<proc_macro2::TokenStream> = farms.chain(sarms).collect();
        let pump = quote! {
            let #n_exch = #ns_core::life::sequence::Mode::current(
                #v_this.#f_life,
                #view.async_state.session(),
                #v_link.id(),
            );
            if let ::core::option::Option::Some(#n_exch) = #n_exch
                && let ::core::option::Option::Some(#n_live) = #state.async_state.take()
            {
                let (#n_task, #n_kind, #n_resp) = #n_live.into_parts();
                let #n_turn = match #n_task {
                    #( #arms )*
                };
                #n_turn.settle(
                    #v_this.#f_life,
                    #state.async_state.session_mut(),
                    #n_exch,
                );
            }
        };
        let body = if tasks.is_empty() {
            quote! {
                let #n_drive = #ns_h1::server::conn::driver::Driver::new(self, #v_date).progress(
                    #v_link,
                    #v_driver,
                );
                let (_, #n_prog) = #n_drive.into_parts();
                #n_prog
            }
        } else {
            quote! {
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
                let #n_drive =
                    #ns_h1::server::conn::driver::Driver::new(self.as_mut(), #v_date)
                        .progress(#v_link, #v_driver);
                let #n_active = #ns_h1::pump::Tasks::activity(&#view.async_state);
                let (_, #n_prog) = #n_drive.settle(#n_active, || {
                    self.activate(#v_date, #v_link, #v_driver);
                });
                #n_prog
            }
        };
        let wake = if tasks.is_empty() {
            quote! {}
        } else {
            quote! {
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
            }
        };
        let fdrops = fibers
            .iter()
            .map(|pair| {
                let task = pair.0;
                let var = task.variant();
                let store = task.path().route(&ctx.plan.spec.state_generics);
                quote! {
                        #ident::#var(#v_task) => {
                            let mut #v_slab = #store;
                            match #ns_fiber::dope_fiber::task::storage::fixed::staged::Store::remove_state(
                                #v_slab.as_mut(),
                                #v_task,
                            ) {
                                ::core::option::Option::Some(
                                    #ns_fiber::dope_fiber::task::storage::fixed::staged::State::Staged,
                                ) => #ns_h1::server::task::turn::Turn::StagedCancelled,
                                ::core::option::Option::Some(
                                    #ns_fiber::dope_fiber::task::storage::fixed::staged::State::Live
                                    | #ns_fiber::dope_fiber::task::storage::fixed::staged::State::Vacant,
                                )
                                | ::core::option::Option::None => {
                                    #ns_h1::server::task::turn::Turn::ConnectionCancelled
                                }
                            }
                        },
                }
            });
        let sdrops = streams.iter().map(|task| {
            let var = task.variant();
            let store = task.path().route(&ctx.plan.spec.state_generics);
            quote! {
                    #ident::#var(#v_task) => {
                        let mut #v_slab = #store;
                        match #v_slab.remove(#v_task) {
                            #ns_fiber::dope_fiber::task::storage::fixed::local::Removal::Removed
                            | #ns_fiber::dope_fiber::task::storage::fixed::local::Removal::Absent => {
                                match #v_kind {
                                    #ns_h1::server::task::runtime::Kind::Closing => {
                                        #ns_h1::server::task::turn::Turn::StagedCancelled
                                    }
                                    #ns_h1::server::task::runtime::Kind::Fiber
                                    | #ns_h1::server::task::runtime::Kind::Stream => {
                                        #ns_h1::server::task::turn::Turn::ConnectionCancelled
                                    }
                                }
                            }
                        }
                    },
            }
        });
        let drops: Vec<proc_macro2::TokenStream> = fdrops.chain(sdrops).collect();
        let close = if tasks.is_empty() {
            quote! {}
        } else {
            quote! {
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
                    let (#v_task, #v_kind, _) = #v_active.into_parts();
                    let #v_cancel = match #v_task {
                        #( #drops )*
                    };
                    if let ::core::option::Option::Some(#n_exch) = #n_exch {
                        #v_cancel.settle(
                            #v_this.#f_life,
                            #state.async_state.session_mut(),
                            #n_exch,
                        );
                    }
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
