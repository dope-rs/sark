use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use handle::{dispatch, head};

use crate::{
    app::{
        serve::{self, handle},
        spec,
    },
    macros::tokens::local,
    runtime::paths::Paths,
};

mod arms;
mod state;

pub(in crate::app::serve) fn egress<M>(
    paths: &Paths,
    spec: &spec::Spec,
    entries: &[serve::RouteSpec<'_, M>],
    lifetime: &syn::Lifetime,
) -> TokenStream {
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let state = spec.state.ty();

    let bounds = entries.iter().map(|item| {
        let route = &item.entry.route;
        match item.kind {
            spec::Kind::Fiber => quote! {
                <<#route as #ns_core::route::Task<'d, #state>>::Output
                    as #ns_http::payload::response::dispatch::Emit<'d, 'd>>::Bound
            },
            spec::Kind::Sync | spec::Kind::Stream => quote! {
                <<#route as #ns_core::manifold::Route<#state>>::Response<#lifetime, #lifetime>
                    as #ns_http::payload::response::dispatch::Emit<#lifetime, #lifetime>>::Bound
            },
            spec::Kind::Duplex => quote! {
                #ns_http::o3::buffer::resident::bound::AtMost<0>
            },
        }
    });
    let guards = entries
        .iter()
        .flat_map(|item| item.entry.guards.iter())
        .map(|guard| {
            quote! {
                <#guard as #ns_core::route::guard::Guard<#state>>::Egress<#lifetime>
            }
        });
    bounds.chain(guards).fold(
        quote!(#ns_http::o3::buffer::resident::bound::AtMost<0>),
        |left, right| quote!(#ns_http::o3::buffer::resident::bound::Max<#left, #right>),
    )
}

pub(super) struct Decode {
    pub(super) contract: TokenStream,
    guard: Option<TokenStream>,
    method: TokenStream,
    full: TokenStream,
    pub(super) prepared: syn::Ident,
    definitions: TokenStream,
}

impl Decode {
    pub(super) fn implement(
        &self,
        paths: &Paths,
        params: &TokenStream,
        target: TokenStream,
    ) -> TokenStream {
        let core = &paths.core;
        let definitions = &self.definitions;
        let decode = &self.method;
        let full = &self.full;
        let guarded = self.guard.as_ref().map(|guard| {
            quote! {
                impl #params #core::decode::Guarded for #target { #guard }
            }
        });
        quote! {
            #definitions
            impl #params #core::decode::Decode for #target { #decode }
            impl #params #core::decode::Full for #target { #full }
            #guarded
        }
    }

    pub(super) fn build<M>(
        paths: &Paths,
        spec: &spec::Spec,
        methods: &dispatch::Methods,
        head: &head::Head,
        entries: &[serve::RouteSpec<'_, M>],
        receiver: TokenStream,
    ) -> Self {
        let f_state = local("state");
        let symbols = &spec.state_generics;
        let [n_body, n_proj] = symbols.locals(["__body", "__project"]);
        let [n_authrg, n_cause] = symbols.locals(["__authority_range", "__cause"]);
        let [n_plan, n_encode] = symbols.locals(["__control_plan", "__encoder"]);
        let [n_field, n_iter] = symbols.locals(["__field", "__field_iter"]);
        let [n_fields, n_fstart] = symbols.locals(["__fields", "__fields_start"]);
        let [n_first, n_head] = symbols.locals(["__first_regular", "__head"]);
        let [n_hbytes, n_hostrg] = symbols.locals(["__head_bytes", "__host_range"]);
        let [n_method] = symbols.locals(["__method"]);
        let [n_mbytes, n_mkey] = symbols.locals(["__method_bytes", "__method_key"]);
        let [n_next] = symbols.locals(["__next"]);
        let [n_pmeth] = symbols.locals(["__planned_method"]);
        let [n_pproto] = symbols.locals(["__planned_protocol"]);
        let [n_proute] = symbols.locals(["__planned_route"]);
        let [n_ptarg] = symbols.locals(["__planned_target"]);
        let [n_prep] = symbols.locals(["__prepared"]);
        let [n_range, n_req] = symbols.locals(["__range", "__request"]);
        let [n_reqtgt, n_route] = symbols.locals(["__request_target", "__route"]);
        let [n_rproto] = symbols.locals(["__route_protocol"]);
        let [n_rtarg] = symbols.locals(["__route_target"]);
        let [n_select, n_target] = symbols.locals(["__selection", "__target"]);
        let n_trange = symbols.local("__target_range");
        let [v_error] = symbols.locals(["error"]);
        let [n_call, n_error, n_fail] = symbols.locals(["__call", "__error", "__failure"]);
        let [n_ghead, n_input, n_result] = symbols.locals(["__guard_head", "__input", "__outcome"]);
        let [n_scope, n_slot] = symbols.locals(["__scope", "__slot"]);
        let [n_source, v_len, v_head] = symbols.locals(["__source", "content_length", "head"]);
        let [l_driver, l_req, l_out, l_input] =
            spec.state_generics
                .lives(["__sark_driver", "__sark_request", "o", "req"]);
        let [g_e, g_f, g_g, g_i, g_t] = spec
            .state_generics
            .idents(["__E", "__F", "__G", "__I", "__T"]);

        let ns_core = &paths.core;
        let ns_http = &paths.http;

        let method = spec.state_generics.ident("__SARK_METHOD");
        let path = spec.state_generics.ident("__SARK_PATH");
        let auth = spec.state_generics.ident("__SARK_AUTHORITY");
        let proto = spec.state_generics.ident("__SARK_PROTOCOL");
        let prefix = spec.state_generics.ident("__SARK_PREFIX");
        let head_ty = &head.names.prepared_ident;
        let plan = &head.names.head_plan_ident;
        let cause = &head.names.cause;
        let route = &head.names.head_route_ident;
        let matcher = &head.full_head_matcher;
        let arms = &head.planned_route_arms;
        let key = &methods.head_method_key;
        let app = match &spec.state {
            spec::State::Bound(_) => quote!(#receiver.#f_state),
            spec::State::Empty(_) => quote!(#ns_core::manifold::EmptyState::REF),
        };

        let state = state::State::build(paths, spec, head, entries);
        let exec = &state.prepared;
        let request = &state.request;
        let guarded = &state.guarded;
        let routes_f = &state.routes;
        let state_ty = spec.state.ty();
        let error_id = format_ident!("{}ProjectError", spec.name);
        let body_id = format_ident!("{}Projected", spec.name);
        let error_ty = quote!(#error_id<#l_input>);
        let body_ty = quote!(#body_id<#l_input, #l_driver>);
        let vis = &spec.vis;
        let mut slots = TokenStream::new();
        let mut variants = TokenStream::new();
        let mut bodies = TokenStream::new();
        let mut errors = TokenStream::new();
        let mut rejects = TokenStream::new();
        let mut bounds = TokenStream::new();
        let mut never = TokenStream::new();
        let mut admits = TokenStream::new();
        let mut dispatch = TokenStream::new();
        let mut project = TokenStream::new();
        for item in entries {
            let entry = item.entry;
            let route = &entry.route;
            let schema = quote!(<#route as #ns_core::route::Spec>::Request);
            let variant = &item.prepared;
            let slot = &item.request;
            let parsed = quote! {
                #ns_core::request::raw::Parsed<
                    #ns_core::request::Head<
                        #ns_http::field::metadata::section::storage::Bytes,
                        <#schema as #ns_core::route::Schema>::Frame,
                    >,
                    #schema,
                >
            };
            let length = quote!(::core::option::Option<::core::primitive::usize>);
            let duplex_length =
                (item.kind == spec::Kind::Duplex).then(|| quote!(content_length: #length,));
            slots.extend(quote! {
                #[doc(hidden)]
                #vis struct #slot { head: #parsed, #duplex_length }
                impl #ns_core::decode::state::Slot<#exec> for #slot {
                    fn prepare(self) -> #exec { #exec::#variant(self) }
                }
            });
            variants.extend(quote!(#variant(#parsed, #length),));
            bodies.extend(quote! {
                #variant(#ns_core::decode::Call<
                    #parsed,
                    #ns_core::body::Scoped<#l_input, 'd, <#schema as #ns_core::route::Schema>::Body>,
                >),
            });
            errors.extend(quote! {
                #variant(<#schema as #ns_core::route::Project>::Error<#l_input>),
            });
            rejects.extend(quote! {
                Self::#variant(#n_error) =>
                    <#schema as #ns_core::route::Project>::reject(#n_error),
            });
            bounds.extend(quote! {
                <#schema as #ns_core::route::Project>::Error<#l_input>:
                    #ns_core::route::Never,
            });
            never.extend(quote! {
                Self::#variant(#n_error) => #ns_core::route::Never::never(#n_error),
            });
            if !entry.guards.is_empty() {
                let open = format_ident!("{variant}Open");
                let finished = format_ident!("{variant}Finished");
                let (enter, leave) = match item.kind {
                    spec::Kind::Duplex => (quote!(duplex), quote!(duplex)),
                    spec::Kind::Sync | spec::Kind::Fiber | spec::Kind::Stream => {
                        (quote!(open), quote!(finished))
                    }
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
                let rejection = handle::guard_rejection(
                    paths,
                    symbols,
                    &entry.guards,
                    state_ty,
                    &app,
                    quote!(#n_encode),
                    quote! {
                        ::core::mem::drop(#n_encode);
                        let #v_head = #v_head.into_inner();
                        return ::core::ops::ControlFlow::Continue(#put);
                    },
                );
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
                            ::core::ops::ControlFlow::Break(match #rejection {
                                ::core::result::Result::Ok(#n_prep) => {
                                    ::core::result::Result::Ok(#ns_core::decode::Lineage::resolve_head(
                                        #n_prep, #v_head.into_owner(),
                                    ))
                                }
                                ::core::result::Result::Err((#n_error, #n_encode)) => {
                                    ::core::result::Result::Err((
                                        #ns_core::decode::dispatch::Error::Encode(#n_error),
                                        #n_encode,
                                    ))
                                }
                            })
                        })
                    }
                };
                for (phase, resolve) in [(&open, enter), (&finished, leave)] {
                    admits.extend(quote! {
                        #guarded::#phase(#n_slot, #n_body) => {
                            let #n_result = #guard;
                            let (#n_slot, #n_body) =
                                <#g_g as #ns_core::decode::state::admit::Resolve<#g_f>>::#resolve(
                                    #n_result, #n_body,
                                )?;
                            ::core::ops::ControlFlow::Continue(#request::#phase(#n_slot, #n_body))
                        },
                    });
                }
            }
            dispatch.extend(match item.kind {
                spec::Kind::Sync => quote! {
                    #body_id::#variant(#n_call) => {
                        let (#v_head, #n_body) =
                            #n_call.into_parts();
                        #v_head.scope(|#v_head| {
                            let #n_encode = #ns_core::decode::Encode::begin_head(
                                #n_encode,
                                #v_head.owner().owner().view(),
                            );
                            let #n_prep = match <#schema as #ns_core::route::Project>
                                ::try_view(&#v_head.borrow(), #n_body)
                            {
                                ::core::result::Result::Ok(#n_req) =>
                                    #ns_http::payload::response::dispatch::Complete::complete(
                                        <#route as #ns_core::manifold::Route<#state_ty>>
                                            ::invoke(#n_req, #app),
                                        #n_encode,
                                    ).map_err(|(#n_error, #n_target)| (
                                        #ns_core::decode::dispatch::Error::Encode(#n_error),
                                        #n_target,
                                    )),
                                ::core::result::Result::Err(#n_error) =>
                                    ::core::result::Result::Err((
                                        #ns_core::decode::dispatch::Error::Invalid(
                                            #error_id::#variant(#n_error),
                                        ),
                                        #ns_core::decode::Cancel::cancel(#n_encode),
                                    )),
                            }?;
                            ::core::result::Result::Ok(#ns_core::decode::Lineage::resolve_head(
                                #n_prep,
                                #v_head.into_owner(),
                            ))
                        })
                    },
                },
                spec::Kind::Fiber | spec::Kind::Stream | spec::Kind::Duplex => quote! {
                    #body_id::#variant(_) =>
                        ::core::result::Result::Err(
                            (
                                #ns_core::decode::dispatch::Error::Unsupported,
                                #ns_core::decode::Cancel::cancel(#n_encode),
                            ),
                        ),
                },
            });
            project.extend(quote! {
                #exec::#variant(#n_slot) => {
                    <<#schema as #ns_core::route::Schema>::Body
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
                        #n_fail.map(|(#n_input, #n_slot)| (#exec::#variant(#n_slot), #n_input))
                    })
                },
            });
        }
        let flow = &state.definition;
        let plan_definition = &head.plan;
        let definitions = quote! {
            #plan_definition
            #slots
            #[doc(hidden)]
            #vis enum #head_ty { #variants }
            #flow

            #[doc(hidden)]
            #vis enum #body_id<#l_input, 'd>
            where
                'd: #l_input,
            {
                #bodies
            }

            #[doc(hidden)]
            #vis enum #error_id<#l_input> {
                #errors
            }

            impl<#l_input> #error_id<#l_input> {
                fn reject(self) -> #ns_core::route::Reject {
                    match self {
                        #rejects
                    }
                }
            }

            impl<#l_input> #ns_core::route::Never for #error_id<#l_input>
            where
                #bounds
            {
                fn never<#g_t>(self) -> #g_t {
                    match self {
                        #never
                    }
                }
            }
        };
        let target = quote! {
            let #n_hbytes = #n_fields.as_bytes();
            let #n_trange = match #n_plan.form() {
                #ns_http::field::metadata::block::Form::Connect => #n_authrg.clone(),
                _ => #n_ptarg.clone(),
            }.ok_or(#ns_core::decode::HeadError::BadRequest)?;
            let #n_reqtgt = #n_hbytes.get(#n_trange.clone())
                .ok_or(#ns_core::decode::HeadError::BadRequest)?;
            let #n_rtarg: &[::core::primitive::u8] = if #n_plan.form()
                == #ns_http::field::metadata::block::Form::Connect
            {
                b"*"
            } else {
                #n_reqtgt
            };
            let #n_rproto = #n_pproto.as_ref().map(|#n_range| {
                #n_hbytes.get(#n_range.clone())
                    .ok_or(#ns_core::decode::HeadError::BadRequest)
            }).transpose()?;
        };
        let full = quote! {
            const #method: &[::core::primitive::u8] = #ns_http::field::head::Name::Method.wire();
            const #path: &[::core::primitive::u8] = #ns_http::field::head::Name::Path.wire();
            const #auth: &[::core::primitive::u8] = #ns_http::field::head::Name::Authority.wire();
            const #proto: &[::core::primitive::u8] = #ns_http::field::head::Name::Protocol.wire();
            let mut #n_mbytes = ::core::option::Option::None;
            let mut #n_ptarg = ::core::option::Option::None;
            let mut #n_authrg = ::core::option::Option::None;
            let mut #n_hostrg = ::core::option::Option::None;
            let mut #n_pproto = ::core::option::Option::None;
            let mut #n_iter = #n_fields.ranges();
            let #n_first = loop {
                let (#n_field, #n_range) = match #n_iter.next() {
                    ::core::option::Option::Some(#n_next) => #n_next,
                    ::core::option::Option::None => {
                        break ::core::option::Option::None;
                    }
                };
                if #n_field.name.first() != ::core::option::Option::Some(&b':') {
                    break ::core::option::Option::Some((#n_field, #n_range));
                }
                match #n_field.name {
                    #method => #n_mbytes = ::core::option::Option::Some(#n_field.value),
                    #path => #n_ptarg = ::core::option::Option::Some(#n_range),
                    #auth => #n_authrg = ::core::option::Option::Some(#n_range),
                    #proto => #n_pproto = ::core::option::Option::Some(#n_range),
                    _ => {}
                }
            };
            let #n_mbytes = #ns_http::field::metadata::map::record::Span::from_slice(
                #n_mbytes.ok_or(#ns_core::decode::HeadError::BadRequest)?,
            );
            #target
            #key
            let #n_method = #n_mkey.ok_or(#ns_core::decode::HeadError::NotFound)?;
            let #n_target = #n_rtarg;
            #matcher
            ::core::result::Result::Err(#ns_core::decode::HeadError::NotFound)
        };
        let selected = quote! {
            #target
            if ::core::matches!(#n_proute, #route::Pending) {
                #n_proute = match #n_pmeth.and_then(|#n_method| {
                    #plan::select(#n_method, #n_rtarg, #n_rproto)
                }) {
                    ::core::option::Option::Some(#n_route) => #n_route,
                    ::core::option::Option::None => #route::Missing,
                };
            }
            let #n_fstart = if #n_plan.form()
                == #ns_http::field::metadata::block::Form::Connect
            {
                0
            } else {
                #n_trange.end
            };
            let #n_iter = match #n_fields.iter_from(#n_fstart) {
                ::core::result::Result::Ok(#n_fields) => #n_fields,
                ::core::result::Result::Err(#n_cause) => {
                    return ::core::result::Result::Err(
                        #ns_core::decode::HeadError::Storage(#n_cause),
                    );
                }
            };
            match #n_proute {
                #( #arms, )*
                #route::Pending => {
                    ::core::result::Result::Err(#ns_core::decode::HeadError::BadRequest)
                }
                #route::Missing => {
                    ::core::result::Result::Err(#ns_core::decode::HeadError::NotFound)
                }
            }
        };
        let bound = egress(paths, spec, entries, &l_out);
        let (guard, guards) = if !admits.is_empty() {
            (
                Some(quote! {
                    #[inline(always)]
                    fn try_admit<
                        #l_req,
                        #l_driver: #l_req,
                        #g_e: #ns_core::decode::Encode<
                            #l_req,
                            #l_driver,
                        >,
                        #g_f: #ns_core::decode::state::admit::Bodies,
                        #g_g: #ns_core::decode::state::admit::Resolve<#g_f>,
                    >(
                        &self,
                        #n_req: #ns_core::decode::request::Guarded<#guarded<#g_f>>,
                        #n_encode: #g_e,
                    ) -> #ns_core::decode::Admission<#l_req, #l_driver, Self, #g_e, #g_f, #g_g> {
                        let (#n_req, #n_method) = #n_req.into_parts();
                        match #n_req {
                            #admits
                        }
                    }
                }),
                quote!(+ #ns_core::decode::Guarded),
            )
        } else {
            (None, TokenStream::new())
        };
        let contract = quote! {
            for<#l_out, #l_input> #ns_core::decode::Decode<
                Head = #head_ty,
                Prepared = #exec,
                Routes = #routes_f,
                Plan = #plan,
                Cause = #cause,
                Error<#l_input> = #error_ty,
                Egress<#l_out> = #bound,
            >
            #guards
            + #ns_core::decode::Full
        };
        let full = quote! {
            #[inline(always)]
            fn try_prefixed<const #prefix: ::core::primitive::usize>(
                &self,
                #n_head: #ns_http::field::metadata::section::state::RetainedHead<
                    #ns_http::field::metadata::packed::Decoded<#ns_http::o3::buffer::pool::Span, #prefix>,
                    #ns_http::field::metadata::section::state::Selected<
                        #ns_http::field::metadata::section::rules::RetainRaw<#prefix>,
                    >,
                >,
            ) -> ::core::result::Result<Self::Head, #ns_core::decode::HeadError<Self::Cause>> {
                let #n_fields = #n_head.fields();
                let #n_plan = *#n_head.control();
                #full
            }
        };
        let tokens = quote! {
            type Head = #head_ty;
            type Prepared = #exec;
            type Routes = #routes_f;
            type Plan = #plan;
            type Cause = #cause;
            type Error<#l_input> = #error_ty;
            type Projected<#l_input, #l_driver> = #body_ty
            where
                #l_driver: #l_input;
            type Egress<#l_out> = #bound;

            fn reject<#l_input>(#v_error: Self::Error<#l_input>) -> #ns_core::route::Reject {
                #v_error.reject()
            }

            #[inline(always)]
            fn try_full(
                &self,
                #n_head: #ns_http::field::metadata::section::state::RetainedHead<
                    #ns_http::field::metadata::packed::Decoded,
                    #ns_http::field::metadata::section::state::Selected<
                        #ns_http::field::metadata::section::rules::RetainRaw,
                    >,
                >,
            ) -> ::core::result::Result<
                Self::Head,
                #ns_core::decode::HeadError<Self::Cause>,
            > {
                #ns_core::decode::Full::try_prefixed::<0>(self, #n_head)
            }

            #[inline(always)]
            fn try_head(
                &self,
                #n_head: #ns_http::field::metadata::section::state::RetainedHead<
                    <#plan as #ns_http::field::metadata::section::Policy>::Block,
                    #ns_http::field::metadata::section::state::Selected<#plan>,
                >,
            ) -> ::core::result::Result<
                Self::Head,
                #ns_core::decode::HeadError<Self::Cause>,
            > {
                let #n_fields = #n_head.fields();
                let #n_plan = *#n_head.control();
                let #n_select = #n_head.selection();
                let (
                    mut #n_proute,
                    #n_ptarg,
                    #n_authrg,
                    #n_hostrg,
                    #n_pproto,
                    #n_pmeth,
                ) = #n_select.as_ref().parts();
                #selected
            }

            #[inline(always)]
            fn try_project<
                #l_driver,
                #g_i: #ns_http::owner::handoff::Project<#l_driver>,
                #g_t,
                #g_f: for<#l_req> ::core::ops::FnOnce(
                    #ns_core::decode::Scoped<
                        #l_req,
                        #l_driver,
                        Self,
                        #g_i::Proof,
                    >,
                ) -> #g_t,
            >(
                &self,
                #n_prep: Self::Prepared,
                #n_body: #ns_core::body::Input<#g_i>,
                #n_proj: #g_f,
            ) -> #ns_core::body::Projection<
                #g_t,
                #g_i::Owner,
                (Self::Prepared, #ns_core::body::Input<#g_i>),
            >
            {
                match #n_prep {
                    #project
                }
            }

            #[inline(always)]
            fn try_dispatch<
                #l_req,
                #l_driver,
                #g_e: #ns_core::decode::Encode<
                    #l_req,
                    #l_driver,
                >,
            >(
                &self,
                #n_req: Self::Projected<#l_req, #l_driver>,
                #n_encode: #g_e,
            ) -> ::core::result::Result<
                #g_e::Prepared,
                (
                    #ns_core::decode::dispatch::Error<
                        <#g_e as #ns_core::decode::Encode<
                            #l_req, #l_driver,
                        >>::Error,
                        ::core::convert::Infallible,
                        Self::Error<#l_req>,
                    >,
                    #g_e::Target,
                ),
            > {
                match #n_req {
                    #dispatch
                }
            }
        };
        Self {
            contract,
            guard,
            method: tokens,
            full,
            prepared: state.prepared,
            definitions,
        }
    }
}
