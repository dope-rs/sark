use decode::state;
use handle::{decode, task};
use task::Task;

use crate::{
    app::{inputs, serve::handle, spec},
    macros::tokens::local,
    runtime::paths::Paths,
};

pub(super) fn build(
    paths: &Paths,
    ctx: &handle::Context<'_, '_, '_>,
    state: &state::State,
    opens: Vec<proc_macro2::TokenStream>,
    route: proc_macro2::TokenStream,
) -> Task {
    let [f_core, f_ids, f_makers] = ["core", "ids", "task_producers"].map(local);
    let [f_state, f_time] = ["state", "timer"].map(local);
    let symbols = &ctx.plan.spec.state_generics;
    let [n_build, n_error] = symbols.locals(["__build", "__error"]);
    let [n_insert] = symbols.locals(["__insert"]);
    let [n_prep] = symbols.locals(["__prepared"]);
    let [n_make, n_req] = symbols.locals(["__producer", "__request"]);
    let [n_id, n_target] = symbols.locals(["__sark_id", "__target"]);
    let [n_task] = symbols.locals(["__task"]);
    let [v_build, v_error, v_head] = symbols.locals(["builder", "error", "head"]);
    let [v_never, v_req, v_this] = symbols.locals(["never", "request", "this"]);
    let [v_seg, v_len, v_budget] = symbols.locals(["segment", "body_len", "budget"]);
    let v_obs = symbols.local("observer");
    let [g_c, g_s, g_o, g_target] = ctx.plan.spec.state_generics.idents(["C", "S", "O", "__C"]);
    use quote::quote;

    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_http = &paths.http;

    let spec = ctx.plan.spec;
    let vis = &spec.vis;
    let prep = &state.prepared;
    let ident = quote::format_ident!("{}TaskBuild", spec.name);
    let ready_id = quote::format_ident!("{}TaskReady", spec.name);
    let fibers = ctx
        .exec
        .fibers()
        .iter()
        .map(|pair| (pair.0, pair.0.prepared(), pair.1.body))
        .collect::<Vec<_>>();
    let defs = fibers.iter().map(|fiber| {
        let route = fiber.0.route();
        let var = fiber.1;
        quote! {
            #var(
                <<#route as #ns_core::route::Spec>::Request as #ns_core::route::Own>::TaskBuilder<'d>,
            ),
        }
    });
    let ready_v = fibers.iter().map(|fiber| {
        let route = fiber.0.route();
        let var = fiber.1;
        quote! {
            #var(
                #ns_core::route::Built<
                    'd,
                    <<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Own>::Task<'d>,
                >,
            ),
        }
    });
    let push_id = quote::format_ident!("{}TaskPush", spec.name);
    let push_v: Vec<_> = fibers
        .iter()
        .map(|fiber| {
            let route = fiber.0.route();
            let var = fiber.1;
            match fiber.2 {
                inputs::Body::Discarded => quote! {
                    #var(#ns_http::payload::owned::Segment<'d>),
                },
                inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => {
                    quote! {
                        #var(
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Own>::TaskBuilder<'d>
                                as #ns_core::body::Push<'d>>::Error,
                        ),
                    }
                }
            }
        })
        .collect();
    let push_ty = match push_v.is_empty() {
        true => quote!(::core::convert::Infallible),
        false => quote!(#push_id<'d>),
    };
    let push_def = (!push_v.is_empty()).then(|| {
        quote! {
            #[doc(hidden)]
            #vis enum #push_id<'d> {
                #( #push_v )*
            }
        }
    });
    let step_id = quote::format_ident!("{}TaskStep", spec.name);
    let step_v: Vec<_> = fibers
        .iter()
        .filter(|fiber| matches!(fiber.2, inputs::Body::Json | inputs::Body::Contiguous))
        .map(|fiber| {
            let route = fiber.0.route();
            let var = fiber.1;
            quote! {
                #var(
                    <<<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Own>::TaskBuilder<'d>
                        as #ns_core::body::Build<'d>>::StepError,
                ),
            }
        })
        .collect();
    let step_ty = match step_v.is_empty() {
        true => quote!(::core::convert::Infallible),
        false => quote!(#step_id<'d>),
    };
    let step_def = (!step_v.is_empty()).then(|| {
        quote! {
            #[doc(hidden)]
            #[derive(::core::fmt::Debug)]
            #vis enum #step_id<'d> {
                #( #step_v )*
            }
        }
    });
    let rej_push = if push_v.is_empty() {
        quote!(match #n_error {})
    } else {
        let arms = fibers.iter().map(|fiber| {
            let route = fiber.0.route();
            let var = fiber.1;
            match fiber.2 {
                inputs::Body::Discarded => quote! {
                    #push_id::#var(_) => #ns_core::route::Reject::Internal
                },
                inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => {
                    quote! {
                        #push_id::#var(#v_error) =>
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Own>::TaskBuilder<'d>
                                as #ns_core::route::Push<'d>>::reject_push(#v_error)
                    }
                }
            }
        });
        quote! {
            match #n_error {
                #( #arms, )*
            }
        }
    };
    let observed = if push_v.is_empty() {
        quote!(match *#v_error {})
    } else {
        let arms = fibers.iter().map(|fiber| {
            let route = fiber.0.route();
            let var = fiber.1;
            match fiber.2 {
                inputs::Body::Discarded => quote! {
                    #push_id::#var(_) => #v_obs.diagnostic(&"discarded"),
                },
                inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => quote! {
                    #push_id::#var(#v_error) =>
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Own>::TaskBuilder<'d>
                            as #ns_core::body::Push<'d>>::observe(#v_error, #v_obs),
                },
            }
        });
        quote!(match #v_error { #( #arms )* })
    };
    let rej_step = if step_v.is_empty() {
        quote!(match #n_error {})
    } else {
        let arms = fibers
            .iter()
            .filter(|fiber| matches!(fiber.2, inputs::Body::Json | inputs::Body::Contiguous))
            .map(|fiber| {
                let route = fiber.0.route();
                let var = fiber.1;
                quote! {
                    #step_id::#var(#v_error) =>
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Own>::TaskBuilder<'d>
                            as #ns_core::route::TaskBuild<'d>>::reject(#v_error)
                }
            });
        quote! {
            match #n_error {
                #( #arms, )*
            }
        }
    };
    let start: Vec<proc_macro2::TokenStream> = ctx
        .plan
        .entries
        .iter()
        .map(|route| {
            let var = &route.prepared;
            let slot = &route.request;
            let ty = &route.entry.route;
            match route.kind {
                spec::Kind::Fiber => quote! {
                    #prep::#var(#slot {
                        head: #v_head,
                    }) => {
                        let #n_req = #v_head.retain().with_id(#n_id);
                        let #v_build = match
                            <<#ty as #ns_core::route::Spec>::Request
                                as #ns_core::route::Own>::start_stream(
                                #n_req,
                            )
                        {
                            ::core::result::Result::Ok(#v_build) => #v_build,
                            ::core::result::Result::Err(#v_error) => {
                                return ::core::result::Result::Err(
                                    #ns_core::decode::dispatch::Error::Invalid(
                                        <<#ty as #ns_core::route::Spec>::Request
                                            as #ns_core::route::Own>::reject(#v_error),
                                    ),
                                );
                            }
                        };
                        ::core::result::Result::Ok(#ident::#var(#v_build))
                    }
                },
                spec::Kind::Sync | spec::Kind::Stream | spec::Kind::Duplex => {
                    quote! {
                        #prep::#var(_) =>
                            ::core::result::Result::Err(
                                #ns_core::decode::dispatch::Error::Unsupported,
                            )
                    }
                }
            }
        })
        .collect();
    let pushes = fibers.iter().map(|fiber| {
        let var = fiber.1;
        match fiber.2 {
            inputs::Body::Discarded => quote! {
                Self::#var(#v_build) => #ns_core::body::Transfer::Rejected {
                    build: Self::#var(#v_build),
                    error: #push_id::#var(#v_seg),
                }
            },
            inputs::Body::Json | inputs::Body::Segmented | inputs::Body::Contiguous => quote! {
                Self::#var(#v_build) =>
                    #ns_core::body::Push::push(#v_build, #v_seg)
                        .map(Self::#var)
                        .map_err(#push_id::#var)
            },
        }
    });
    let finish = fibers.iter().map(|fiber| {
        let var = fiber.1;
        quote! {
            Self::#var(#v_build) =>
                Self::#var(#ns_core::body::Build::finish_input(#v_build, #v_len))
        }
    });
    let steps = fibers.iter().map(|fiber| {
        let var = fiber.1;
        match fiber.2 {
            inputs::Body::Discarded | inputs::Body::Segmented => quote! {
                Self::#var(#v_build) =>
                    #ns_core::body::Build::step(#v_build, #v_budget).map(
                        Self::#var,
                        #ready_id::#var,
                        |#v_never| match #v_never {},
                    )
            },
            inputs::Body::Json | inputs::Body::Contiguous => quote! {
                Self::#var(#v_build) =>
                    #ns_core::body::Build::step(#v_build, #v_budget).map(
                        Self::#var,
                        #ready_id::#var,
                        #step_id::#var,
                    )
            },
        }
    });
    let has = !fibers.is_empty();
    let never = match has {
        true => proc_macro2::TokenStream::new(),
        false => quote! {
            __Never(
                ::core::convert::Infallible,
                ::core::marker::PhantomData<&'d ()>,
            ),
        },
    };
    let dead = match has {
        true => proc_macro2::TokenStream::new(),
        false => quote! {
            Self::__Never(#v_never, _) => match #v_never {},
        },
    };
    let def = quote! {
        #push_def
        #step_def

        #[doc(hidden)]
        #vis enum #ident<'d> {
            #( #defs )*
            #never
        }

        #[doc(hidden)]
        #vis enum #ready_id<'d> {
            #( #ready_v )*
            #never
        }

        impl<'d> #ns_core::body::Build<'d> for #ident<'d> {
            type Output = #ready_id<'d>;
            type StepError = #step_ty;

            fn finish_input(
                self,
                #v_len: ::core::primitive::usize,
            ) -> Self {
                match self {
                    #( #finish, )*
                    #dead
                }
            }

            fn step(
                self,
                #v_budget: ::core::num::NonZero<::core::primitive::usize>,
            ) -> #ns_core::body::Step<
                Self,
                Self::Output,
                Self::StepError,
            > {
                match self {
                    #( #steps, )*
                    #dead
                }
            }
        }

        impl<'d> #ns_core::body::Push<'d> for #ident<'d> {
            type Error = #push_ty;

            fn observe<#g_c: ::core::marker::Copy, #g_s: ::core::marker::Copy, #g_o: #ns_core::life::Observer<#g_c, #g_s>>(
                #v_error: &Self::Error,
                #v_obs: &mut #g_o,
            ) {
                #observed
            }


            fn push(
                self,
                #v_seg: #ns_http::payload::owned::Segment<'d>,
            ) -> #ns_core::body::Transfer<Self, Self::Error> {
                match self {
                    #( #pushes, )*
                    #dead
                }
            }
        }
    };
    let id = has.then(|| {
        quote! {
            let #n_id = self.#f_core.#f_ids.try_next().map_err(
                #ns_core::decode::dispatch::Error::Sequence,
            )?;
        }
    });
    let begin = quote! {
        #id
        match #n_prep {
            #( #start, )*
        }
    };
    let arms = ctx
        .exec
        .fibers()
        .iter()
        .map(|pair| {
            let item = pair.0;
            let var = item.prepared();
            let fiber = pair.1;
            let field = syn::Index::from(fiber.slot);
            let flat = crate::app::serve::flat::name(ctx.plan.spec, true);
            let kind = item.phase("Future");
            let admission = item.scheduled_admission(paths, &ctx.plan.spec.state_generics, fiber);
            let f_quotas = local("quotas");
            let debit = fiber
                .quota
                .map(|index| quote!(#v_this.#f_quotas[#index] -= 1;));
            quote! {
                #ready_id::#var(#v_req) => {
                    #admission
                    let #n_make = #v_this.#f_makers.#field;
                    let #n_task = #n_make((
                        #ns_core::manifold::task::request::Input::Ready {
                            request: #v_req,
                        },
                        *#v_this.#f_state,
                        <#ns_fiber::manifold::runtime::Prepared<
                            #g_target,
                        > as
                            #ns_fiber::manifold::runtime::Runnable
                        >::handle(#v_this.#f_time),
                    ));
                    let #n_task = #n_insert.insert((#flat::#kind(#n_task), #n_target));
                    #debit
                    ::core::result::Result::Ok(
                        #n_task,
                    )
                }
            }
        })
        .collect::<Vec<proc_macro2::TokenStream>>();
    let empty = match has {
        true => proc_macro2::TokenStream::new(),
        false => quote!(#ready_id::__Never(#v_never, _) => match #v_never {},),
    };
    let body = quote! {
        let mut #v_this = self.project().#f_core.project();
        match #n_build {
            #( #arms, )*
            #empty
        }
    };
    Task {
        opens,
        dispatch_body: route,
        ident,
        definition: def,
        reject_step: rej_step,
        reject_push: rej_push,
        start_body: begin,
        built_body: body,
    }
}
