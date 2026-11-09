use crate::{
    app::{
        serve,
        serve::handle::{decode::state, head},
        spec,
    },
    runtime::paths::Paths,
};

pub(in crate::app::serve::handle) struct Prepared {
    pub(in crate::app::serve::handle) state: state::State,
    pub(in crate::app::serve::handle) type_defs: proc_macro2::TokenStream,
    pub(super) body_type: proc_macro2::TokenStream,
    pub(super) error_type: proc_macro2::TokenStream,
    pub(in crate::app::serve::handle) variant_defs: Vec<proc_macro2::TokenStream>,
    pub(super) guard_arms: Vec<proc_macro2::TokenStream>,
    pub(super) dispatch_arms: Vec<proc_macro2::TokenStream>,
    pub(super) projection_arms: Vec<proc_macro2::TokenStream>,
}

impl Prepared {
    pub(super) fn build<M>(
        paths: &Paths,
        spec: &spec::Spec,
        head: &head::Head,
        entries: &[serve::RouteSpec<'_, M>],
        app: &proc_macro2::TokenStream,
    ) -> Self {
        let symbols = &spec.state_generics;
        let [n_body, n_call] = symbols.locals(["__body", "__call"]);
        let [n_encode, n_error] = symbols.locals(["__encoder", "__error"]);
        let [n_fail, n_ghead] = symbols.locals(["__failure", "__guard_head"]);
        let [n_head, n_hdrs] = symbols.locals(["__head", "__headers"]);
        let [n_input, n_method] = symbols.locals(["__input", "__method"]);
        let [n_result, n_output] = symbols.locals(["__outcome", "__output"]);
        let [n_params] = symbols.locals(["__params"]);
        let [n_prep, n_proj] = symbols.locals(["__prepared", "__project"]);
        let [n_req] = symbols.locals(["__request"]);
        let [n_resp, n_scope] = symbols.locals(["__response", "__scope"]);
        let [n_slot, n_source] = symbols.locals(["__slot", "__source"]);
        let [n_view, n_visit] = symbols.locals(["__view", "__visit"]);
        let [v_len, v_head] = symbols.locals(["content_length", "head"]);
        let [l_driver, l_input] = spec.state_generics.lives(["__sark_driver", "req"]);
        let g_t = spec.state_generics.ident("__T");
        use quote::{format_ident, quote};

        let ns_core = &paths.core;
        let ns_http = &paths.http;

        let state_ty = spec.state.ty();
        let error_id = format_ident!("{}ProjectError", spec.name);
        let body_id = format_ident!("{}Projected", spec.name);
        let state = state::State::build(paths, spec, head, entries);
        let prepared = &state.prepared;
        let request = &state.request;
        let guarded = &state.guarded;
        let error = quote!(#error_id<#l_input>);
        let bodies = entries.iter().map(|item| {
            let entry = item.entry;
            let variant = &item.prepared;
            let route = &entry.route;
            quote! {
                #variant(
                    #ns_core::decode::Call<
                        #ns_core::request::raw::Parsed<
                            #ns_core::request::Head<
                                #ns_http::field::metadata::section::storage::Bytes,
                                <<#route as #ns_core::route::Spec>::Request
                                    as #ns_core::route::Schema>::Frame,
                            >,
                            <#route as #ns_core::route::Spec>::Request,
                        >,
                        #ns_core::body::Scoped<
                            #l_input,
                            'd,
                            <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::Body,
                        >,
                    >,
                ),
            }
        });
        let errors = entries.iter().map(|item| {
            let entry = item.entry;
            let variant = &item.prepared;
            let route = &entry.route;
            quote! {
                #variant(
                    <<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Project>::Error<#l_input>,
                ),
            }
        });
        let rejects = entries.iter().map(|item| {
            let entry = item.entry;
            let variant = &item.prepared;
            let route = &entry.route;
            quote! {
                Self::#variant(#n_error) =>
                    <<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Project>::reject(#n_error),
            }
        });
        let bounds = entries.iter().map(|item| {
            let route = &item.entry.route;
            quote! {
                <<#route as #ns_core::route::Spec>::Request
                    as #ns_core::route::Project>::Error<#l_input>:
                    #ns_core::route::Never,
            }
        });
        let arms = entries.iter().map(|item| {
            let variant = &item.prepared;
            quote! {
                Self::#variant(#n_error) => #ns_core::route::Never::never(#n_error),
            }
        });
        let vis = &spec.vis;
        let types = quote! {
            #[doc(hidden)]
            #vis enum #body_id<#l_input, 'd>
            where
                'd: #l_input,
            {
                #( #bodies )*
            }

            #[doc(hidden)]
            #vis enum #error_id<#l_input> {
                #( #errors )*
            }

            impl<#l_input> #error_id<#l_input> {
                fn reject(self) -> #ns_core::route::Reject {
                    match self {
                        #( #rejects )*
                    }
                }
            }

            impl<#l_input> #ns_core::route::Never for #error_id<#l_input>
            where
                #( #bounds )*
            {
                fn never<#g_t>(self) -> #g_t {
                    match self {
                        #( #arms )*
                    }
                }
            }
        };
        let defs = entries
            .iter()
            .map(|item| {
                let variant = &item.prepared;
                let slot = &item.head;
                quote! {
                    #variant(#slot),
                }
            })
            .collect();
        let admits = entries
            .iter()
            .filter(|item| !item.entry.guards.is_empty())
            .map(|item| {
                let variant = &item.prepared;
                let head_ty = &item.head;
                let slot = &item.request;
                let open = format_ident!("{variant}Open");
                let finished = format_ident!("{variant}Finished");
                let (enter, leave) = match item.kind {
                    spec::Kind::Sync => (quote!(sync_open), quote!(sync_done)),
                    spec::Kind::Fiber => (quote!(task_open), quote!(task_done)),
                    spec::Kind::Duplex => (quote!(duplex_open), quote!(duplex_done)),
                    spec::Kind::Stream => (quote!(reject_open), quote!(reject_done)),
                };
                let (take, put) = match item.kind {
                    spec::Kind::Duplex => (
                        quote! {
                            let #slot { head: #v_head, content_length: #v_len } = #n_slot;
                        },
                        quote! { #slot { head: #v_head, content_length: #v_len } },
                    ),
                    spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => (
                        quote! { let #slot { head: #v_head } = #n_slot; },
                        quote! { #slot { head: #v_head } },
                    ),
                };
                let guard = quote! {
                    {
                        #take
                        #v_head.scope(|#v_head| {
                        let #n_encode = #ns_core::decode::Encode::begin_head(
                            #n_encode,
                            #v_head.owner().owner().view(),
                        );
                        let #n_head = #v_head.owner().borrow();
                        let #n_ghead =
                            #ns_core::route::guard::Head::from_parts(
                                &#n_method,
                                #n_head.form(),
                                ::core::option::Option::Some(#n_head.path()),
                                #n_head.authority(),
                            );
                        match <#head_ty as #ns_core::route::guard::Slot<#state_ty>>::try_admit(
                                #n_ghead,
                                #app,
                                #n_encode,
                        ).map_continue(::core::mem::drop) {
                            ::core::ops::ControlFlow::Continue(()) => {
                                let #v_head = #v_head.into_inner();
                                ::core::ops::ControlFlow::Continue(#put)
                            }
                            ::core::ops::ControlFlow::Break(
                            ::core::result::Result::Ok(#n_prep),
                        ) => {
                                let #n_prep =
                                    #ns_core::decode::Lineage::resolve_head(
                                        #n_prep,
                                        #v_head.into_owner(),
                                    );
                                ::core::ops::ControlFlow::Break(
                                    ::core::result::Result::Ok(#n_prep),
                                )
                            }
                            ::core::ops::ControlFlow::Break(
                                ::core::result::Result::Err((#n_error, #n_encode)),
                            ) => {
                                ::core::ops::ControlFlow::Break(
                                    ::core::result::Result::Err((
                                        #ns_core::decode::dispatch::Error::Encode(#n_error),
                                        #n_encode,
                                    )),
                                )
                            }
                        }
                        })
                    }
                };
                let arm = |phase: &syn::Ident, visit: &proc_macro2::TokenStream| {
                    quote! {
                        #guarded::#phase(#n_slot, #n_body) => {
                            let #n_result = #guard;
                            match #ns_core::decode::state::admit::TryVisit::#visit(
                                #n_visit,
                                #n_result,
                                #n_body,
                            ) {
                                ::core::ops::ControlFlow::Continue((#n_slot, #n_body)) => {
                                    ::core::ops::ControlFlow::Continue(
                                        #request::#phase(#n_slot, #n_body),
                                    )
                                }
                                ::core::ops::ControlFlow::Break(#n_output) => {
                                    ::core::ops::ControlFlow::Break(#n_output)
                                }
                            }
                        }
                    }
                };
                let open_arm = arm(&open, &enter);
                let done_arm = arm(&finished, &leave);
                quote! {
                    #open_arm,
                    #done_arm,
                }
            })
            .collect();
        let dispatch = entries
            .iter()
            .map(|item| {
                let entry = item.entry;
                let variant = &item.prepared;
                let route = &entry.route;
                match item.kind {
                    spec::Kind::Sync => {}
                    spec::Kind::Fiber | spec::Kind::Stream | spec::Kind::Duplex => {
                        return quote! {
                            #body_id::#variant(_) =>
                                ::core::result::Result::Err(
                                    (
                                        #ns_core::decode::dispatch::Error::Unsupported,
                                        #ns_core::decode::Cancel::cancel(#n_encode),
                                    ),
                                )
                        };
                    }
                }
                quote! {
                    #body_id::#variant(#n_call) => {
                        let (#v_head, #n_body) =
                            #n_call.into_parts();
                        #v_head.scope(|#v_head| {
                        let #n_encode = #ns_core::decode::Encode::begin_head(
                            #n_encode,
                            #v_head.owner().owner().view(),
                        );
                        let #n_head = #v_head.borrow();
                        let #n_resp = (|| {
                            let #n_body = #n_body.into_inner();
                            let #n_view = #n_body.into();
                            let #n_params = <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Project>::from_params(&#n_head)?;
                            let #n_hdrs = <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Project>::from_headers(&#n_head)?;
                            let #n_body = <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Project>::parse_body(#n_body)?;
                            let #n_req = #ns_core::request::View::from_head(
                                #n_head.into_owner(), #n_view,
                            );
                            ::core::result::Result::Ok(
                                <#route as #ns_core::manifold::Route<#state_ty>>::invoke(
                                    (#n_params, &#n_req, #n_hdrs, #n_body, #app),
                                ),
                            )
                        })();
                        let #n_prep = match #n_resp {
                            ::core::result::Result::Ok(#n_resp) =>
                                #ns_http::payload::response::dispatch::Complete::complete(
                                    #n_resp,
                                    #n_encode,
                                ).map_err(|(#n_error, #n_encode)| (
                                    #ns_core::decode::dispatch::Error::Encode(#n_error),
                                    #n_encode,
                                )),
                            ::core::result::Result::Err(#n_error) =>
                                ::core::result::Result::Err((
                                    #ns_core::decode::dispatch::Error::Invalid(
                                        #error_id::#variant(#n_error),
                                    ),
                                    #ns_core::decode::Cancel::cancel(#n_encode),
                                )),
                        }?;
                        let #n_prep =
                            #ns_core::decode::Lineage::resolve_head(
                                #n_prep,
                                #v_head.into_owner(),
                            );
                        ::core::result::Result::Ok(#n_prep)
                        })
                    }
                }
            })
            .collect();
        let project = entries
            .iter()
            .map(|item| {
                let entry = item.entry;
                let variant = &item.prepared;
                let route = &entry.route;
                quote! {
                    #prepared::#variant(#n_slot) => {
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Body
                            as #ns_core::body::Project>::try_input(
                            #n_body,
                            #n_slot,
                            |#n_scope| {
                                let (#n_slot, #n_body, #n_source) = #n_scope.into_source();
                                #n_proj(
                                    #ns_core::decode::Scoped::<Self, _>::new(
                                        #body_id::#variant(
                                            #ns_core::decode::Call::new(#n_slot.head, #n_body),
                                        ),
                                        #n_source,
                                    ),
                                )
                            },
                        ).map_err(|#n_fail| {
                            #n_fail.map(|(#n_input, #n_slot)| (#prepared::#variant(#n_slot), #n_input))
                        })
                    },
                }
            })
            .collect();
        Self {
            state,
            type_defs: types,
            body_type: quote!(#body_id<#l_input, #l_driver>),
            error_type: error,
            variant_defs: defs,
            guard_arms: admits,
            dispatch_arms: dispatch,
            projection_arms: project,
        }
    }
}
