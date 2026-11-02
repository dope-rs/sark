use decode::state;

use crate::{
    app::serve::{
        duplex::Connect,
        handle::{self, decode, dispatch, head, tasks},
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

mod body;
mod forward;
mod open;
mod route;

pub(super) struct Task {
    opens: Vec<proc_macro2::TokenStream>,
    dispatch_body: proc_macro2::TokenStream,
    ident: syn::Ident,
    definition: proc_macro2::TokenStream,
    reject_step: proc_macro2::TokenStream,
    reject_push: proc_macro2::TokenStream,
    start_body: proc_macro2::TokenStream,
    built_body: proc_macro2::TokenStream,
}

impl Task {
    pub(super) fn new(
        paths: &Paths,
        ctx: &handle::Context<'_, '_, '_>,
        state: &state::State,
    ) -> Self {
        body::build(
            paths,
            ctx,
            state,
            open::generate(paths, ctx, state),
            route::generate(paths, ctx, state),
        )
    }

    /// Emits the complete protocol-neutral handle surface directly. H1 state is
    /// intentionally absent from the signature, so task expansion cannot reach
    /// listener, continuation, cache, or sequential-lifecycle generation.
    pub(super) fn build(
        self,
        paths: &Paths,
        ctx: &handle::Context<'_, '_, '_>,
        disp: &dispatch::Dispatch,
        head: &head::Head,
        decode: &decode::Decode,
        tasks: &tasks::Tasks,
    ) -> proc_macro2::TokenStream {
        let [f_core, f_tasks, f_time] = ["core", "tasks", "timer"].map(local);
        let symbols = &ctx.plan.spec.state_generics;
        let [n_body, n_build] = symbols.locals(["__body", "__build"]);
        let [n_driver, n_error] = symbols.locals(["__driver", "__error"]);
        let [n_hand, n_owner] = symbols.locals(["__handoff", "__owner"]);
        let [n_parent, n_prep] = symbols.locals(["__parent", "__prepared"]);
        let [n_req, n_scope] = symbols.locals(["__request", "__scope"]);
        let [n_target, n_task] = symbols.locals(["__target", "__task"]);
        let [n_work, v_error] = symbols.locals(["__work", "error"]);
        let l_turn = ctx.plan.spec.state_generics.lifetime("__turn");
        let [g_b, g_h] = ctx.plan.spec.state_generics.idents(["__B", "__H"]);
        let g_brand = ctx.plan.spec.state_generics.lifetime("brand");
        let [g_target, g_id, g_store] = ctx.plan.spec.state_generics.idents(["__C", "__ID", "__P"]);
        use quote::quote;

        let ns_core = &paths.core;
        let ns_fiber = &paths.fiber;
        let ns_http = &paths.http;

        let spec = ctx.plan.spec;
        let vis = &spec.vis;
        let owner = &spec.name;
        let task = &ctx.exec.task.fiber;
        let done = quote::format_ident!("__{}Completion", owner);
        let state = spec.state.ty();
        let complete = if ctx.exec.fibers().is_empty() {
            quote!(::core::convert::Infallible)
        } else {
            quote!(#done<'d, #state>)
        };
        let name = &ctx.name;
        let core = &ctx.core_ident;
        let base = &disp.shape.base_bounds;
        let def = &disp.shape.generic_def;
        let pdef = &disp.shape.protocol_generic_def;
        let args = &disp.shape.generic_use;
        let mux = &disp.shape.multiplexed_generic_use;
        let plan = &head.plan;
        let ready = &head.names.prepared_ident;
        let vars = &decode.prepared.variant_defs;
        let slots = &decode.prepared.state.slots;
        let reqdef = &decode.prepared.state.definition;
        let guards = handle::emit_guards(paths, spec, &ctx.plan.entries);
        let Self {
            opens,
            dispatch_body: route,
            ident: build,
            definition: bdef,
            reject_step: rstep,
            reject_push: rpush,
            start_body: begin,
            built_body: choose,
        } = self;
        let duplex = &ctx.plan.duplex;
        let defs = duplex.definitions(paths);
        let raw = duplex.ty(paths);
        let ext = match duplex.connect() {
            Connect::Enabled => quote!(#ns_http::field::metadata::block::Connect::Enabled),
            Connect::Disabled => quote!(#ns_http::field::metadata::block::Connect::Disabled),
        };
        let error = &decode.prepared.type_defs;
        let method = &decode.method;
        let forward = forward::build(paths, ctx, disp, decode);
        let never = quote!(::core::convert::Infallible);
        let drive = &tasks.drive_body;
        let work = &tasks.task_work_body;
        let wake = &tasks.wake_body;
        let probe = &tasks.try_wake_body;
        let cancel = &tasks.cancel_one_body;
        let fibers = !ctx.exec.fibers().is_empty();
        let failure = if fibers {
            quote!(#ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Error<#g_target>)
        } else {
            quote!(#never)
        };
        let reject = if fibers {
            quote! {
                match #n_error {
                    #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Error::Task(
                        #ns_fiber::dope::core::driver::schedule::ready::task::Error::Capacity { .. },
                    ) => #ns_core::route::Reject::Unavailable,
                    #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Error::Task(
                        #ns_fiber::dope::core::driver::schedule::ready::task::Error::ParentStale
                        | #ns_fiber::dope::core::driver::schedule::ready::task::Error::WakeHopCeiling { .. },
                    )
                    | #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::Error::Owner(
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::OwnerError::Foreign { .. }
                        | #ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::OwnerError::Stale { .. },
                    ) => #ns_core::route::Reject::Internal,
                }
            }
        } else {
            quote!(match *#n_error {})
        };
        let policy = fibers.then(|| {
            quote! {
                #g_store: #ns_fiber::manifold::runtime::Runnable,
            }
        });
        let guarded = decode.guard.as_ref().map(|guard| {
            quote! {
                impl #def #ns_core::decode::Guarded for #core #args
                where
                    #policy
                    #base
                {
                    #guard
                }
            }
        });
        let [reg, retire] = [quote!(register), quote!(try_retire)].map(|method| {
            if fibers {
                quote! {
                    self.project_ref()
                        .#f_core
                        .project_ref()
                        .#f_tasks
                        .as_pin_ref()
                        .owners()
                        .#method(#n_owner)
                        .map_err(|#v_error| #v_error.cause())
                }
            } else {
                quote!(::core::result::Result::Ok(()))
            }
        });
        let (own, ret) = if fibers {
            (
                quote!(#ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::OwnerCause),
                quote!(#ns_fiber::dope_fiber::task::storage::fixed::schedule::queue::RetireCause),
            )
        } else {
            (
                quote!(::core::convert::Infallible),
                quote!(::core::convert::Infallible),
            )
        };
        let (sarg, warg, darg, harg, oarg) = if fibers {
            (
                quote!(#n_scope),
                quote!(#n_work),
                quote!(#n_driver),
                quote!(#n_hand),
                quote!(#n_owner),
            )
        } else {
            (quote!(_), quote!(_), quote!(_), quote!(_), quote!(_))
        };
        let timers = [
            (name, quote!(self.#f_core.#f_time)),
            (core, quote!(self.#f_time)),
        ]
        .into_iter()
        .map(|(target, timer)| {
            quote! {
                impl #def #ns_core::manifold::timer::Source<'d> for #target #args
                where
                    #g_store: #ns_fiber::manifold::runtime::Runnable,
                    #base
                {
                    fn timer(&self) -> #ns_fiber::driver::schedule::clock::timer::Timer<'d> {
                        <#g_store as #ns_fiber::manifold::runtime::Runnable>::handle(&#timer)
                    }
                }
            }
        });
        quote! {
            #defs
            #plan

            #( #slots )*
            #( #guards )*

            #bdef

            #[doc(hidden)]
            #vis enum #ready {
                #( #vars )*
            }

            #reqdef
            #error

            impl #def #ns_core::decode::Decode for #core #args
            where
                #policy
                #base
            {
                #method
            }

            #guarded

            #forward

            impl #def #ns_core::duplex::Application<'d> for #core #args
            where
                #policy
                #base
            {
                type Duplex = #raw;
                const CONNECT: #ns_http::field::metadata::block::Connect = #ext;

                #[inline(always)]
                fn try_open<#g_b>(
                    &self,
                    #n_req: #ns_core::decode::request::Admitted<
                        #ns_core::decode::exec::Duplex,
                        Self::Prepared,
                        #g_b,
                    >,
                ) -> ::core::result::Result<
                    (Self::Duplex, #g_b),
                    (
                        #ns_core::duplex::OpenError<Self::Error<'d>>,
                        #g_b,
                    ),
                > {
                    #n_req.build(|#n_prep| match #n_prep {
                        #( #opens, )*
                    })
                }
            }

            impl #def #ns_core::duplex::Application<'d> for #name #args
            where
                #policy
                #base
            {
                type Duplex = #raw;
                const CONNECT: #ns_http::field::metadata::block::Connect =
                    <#core #args as #ns_core::duplex::Application<'d>>::CONNECT;

                #[inline(always)]
                fn try_open<#g_b>(
                    &self,
                    #n_req: #ns_core::decode::request::Admitted<
                        #ns_core::decode::exec::Duplex,
                        Self::Prepared,
                        #g_b,
                    >,
                ) -> ::core::result::Result<
                    (Self::Duplex, #g_b),
                    (
                        #ns_core::duplex::OpenError<Self::Error<'d>>,
                        #g_b,
                    ),
                > {
                    #ns_core::duplex::Application::try_open(
                        &self.#f_core,
                        #n_req,
                    )
                }
            }

            impl #pdef #ns_core::manifold::task::app::Runtime<'d>
                for #name #mux
            where
                #base
            {
                type Task = #task<#g_brand, 'd, #g_id>;
                type Build = #build<'d>;
                type Target = #g_target;
                type Completion = #complete;
                type Admission = #failure;
                type OwnerCause = #own;
                type RetireCause = #ret;

                fn reject_step(
                    #n_error: <Self::Build as #ns_core::body::Build<'d>>::StepError,
                ) -> #ns_core::route::Reject {
                    #rstep
                }

                fn reject_push(
                    #n_error: <Self::Build as #ns_core::body::Push<'d>>::Error,
                ) -> #ns_core::route::Reject {
                    #rpush
                }

                fn rejection(
                    #n_error: &#ns_core::manifold::task::Error<Self::Admission>,
                ) -> #ns_core::route::Reject {
                    match #n_error {
                        #ns_core::manifold::task::Error::Admission(#n_error) => { #reject }
                        #ns_core::manifold::task::Error::Sequence(_) => {
                            #ns_core::route::Reject::Unavailable
                        }
                        #ns_core::manifold::task::Error::Unsupported => {
                            #ns_core::route::Reject::Internal
                        }
                    }
                }

                fn register(
                    self: ::core::pin::Pin<&Self>,
                    #oarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Owner<
                        Self::Target,
                    >,
                ) -> ::core::result::Result<(), Self::OwnerCause> {
                    #reg
                }

                fn try_retire(
                    self: ::core::pin::Pin<&Self>,
                    #oarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Owner<
                        Self::Target,
                    >,
                ) -> ::core::result::Result<(), Self::RetireCause> {
                    #retire
                }

                fn try_dispatch(
                    self: ::core::pin::Pin<&mut Self>,
                    #n_req: #ns_core::decode::request::Ready<
                        #ns_core::decode::exec::Task,
                        Self::Prepared,
                        #ns_http::payload::owned::Segments<'d>,
                    >,
                    #n_target: Self::Target,
                    #n_owner: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Owner<
                        Self::Target,
                    >,
                    #n_parent: #ns_fiber::dope::core::driver::schedule::ready::Target<'d>,
                ) -> ::core::result::Result<
                    Self::Task,
                    #ns_core::manifold::task::Error<Self::Admission>,
                > {
                    let (#n_prep, #n_body) = #n_req.into_parts();
                    let #n_parent = #ns_fiber::context::wake::RootWaker::from(#n_parent);
                    #route
                }

                fn try_start(
                    &self,
                    #n_prep: Self::Prepared,
                ) -> ::core::result::Result<
                    Self::Build,
                    #ns_core::decode::dispatch::Error<
                        #never,
                        #never,
                        #ns_core::route::Reject,
                    >,
                > {
                    #begin
                }

                fn try_built(
                    self: ::core::pin::Pin<&mut Self>,
                    #n_build: <Self::Build as #ns_core::body::Build<'d>>::Output,
                    #n_target: Self::Target,
                    #n_owner: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Owner<
                        Self::Target,
                    >,
                    #n_parent: #ns_fiber::dope::core::driver::schedule::ready::Target<'d>,
                ) -> ::core::result::Result<
                    Self::Task,
                    #ns_core::manifold::task::Error<Self::Admission>,
                > {
                    let #n_parent = #ns_fiber::context::wake::RootWaker::from(#n_parent);
                    #choose
                }

                fn drive<
                    #l_turn,
                    #g_h: #ns_core::manifold::task::app::Handoff<
                        'd,
                        Self::Target,
                        Self::Completion,
                    >
                >(
                    self: ::core::pin::Pin<&mut Self>,
                    #sarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Scope<
                        Self::Target,
                    >,
                    #warg: #ns_fiber::dope::core::driver::schedule::app::Work<#l_turn, 'd>,
                    #darg: &mut #ns_fiber::dope::core::driver::retained::Context<'_, '_, 'd>,
                    #harg: #g_h,
                ) -> #ns_core::manifold::task::app::Turn<#l_turn, 'd, #g_h::Complete> {
                    #drive
                }

                fn has_work(
                    self: ::core::pin::Pin<&Self>,
                    #sarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Scope<
                        Self::Target,
                    >,
                ) -> ::core::primitive::bool {
                    #work
                }

                fn wake(
                    self: ::core::pin::Pin<&mut Self>,
                    #sarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Scope<
                        Self::Target,
                    >,
                ) {
                    #wake
                }

                fn try_wake(
                    self: ::core::pin::Pin<&mut Self>,
                    #sarg: #ns_fiber::dope::core::driver::schedule::ready::task::owner::Scope<
                        Self::Target,
                    >,
                ) -> #ns_core::manifold::task::app::Wake {
                    #probe
                }

                fn cancel(
                    self: ::core::pin::Pin<&mut Self>,
                    #n_task: Self::Task,
                ) {
                    #cancel
                }
            }

            #(#timers)*
        }
    }
}
