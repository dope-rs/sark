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
mod body;
mod prepared;
pub(super) mod state;

pub(in crate::app::serve) fn egress<M>(
    paths: &Paths,
    spec: &spec::Spec,
    entries: &[serve::RouteSpec<'_, M>],
    lifetime: &syn::Lifetime,
) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let state = spec.state.ty();
    use quote::quote;

    let bounds = entries.iter().map(|item| {
        let route = &item.entry.route;
        match item.kind {
            spec::Kind::Fiber => quote! {
                <<<#route as #ns_core::route::Task<'d, #state>>::Output
                    as #ns_http::payload::response::dispatch::IntoOutput<'d, 'd>>::Output
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
    pub(super) contract: proc_macro2::TokenStream,
    pub(super) guard: Option<proc_macro2::TokenStream>,
    pub(super) method: proc_macro2::TokenStream,
    pub(super) prepared: prepared::Prepared,
}

impl Decode {
    pub(super) fn build<M>(
        paths: &Paths,
        spec: &spec::Spec,
        methods: &dispatch::Methods,
        head: &head::Head,
        entries: &[serve::RouteSpec<'_, M>],
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
        let [n_httpm, n_method] = symbols.locals(["__http_method", "__method"]);
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
        let [n_trange, n_visit] = symbols.locals(["__target_range", "__visit"]);
        let [v_dkey, v_error] = symbols.locals(["decoded_method_key", "error"]);
        let [l_driver, l_req, l_out, l_input] =
            spec.state_generics
                .lives(["__sark_driver", "__sark_request", "o", "req"]);
        let [g_e, g_f, g_g, g_i, g_t, g_v] = spec
            .state_generics
            .idents(["__E", "__F", "__G", "__I", "__T", "__V"]);
        use quote::quote;

        let ns_core = &paths.core;
        let ns_http = &paths.http;

        let method = spec.state_generics.ident("__SARK_METHOD");
        let path = spec.state_generics.ident("__SARK_PATH");
        let auth = spec.state_generics.ident("__SARK_AUTHORITY");
        let proto = spec.state_generics.ident("__SARK_PROTOCOL");
        let head_ty = &head.names.prepared_ident;
        let plan = &head.names.head_plan_ident;
        let cause = &head.names.cause;
        let route = &head.names.head_route_ident;
        let matcher = &head.full_head_matcher;
        let arms = &head.planned_route_arms;
        let key = &methods.decoded_method_key;
        let state = match &spec.state {
            spec::State::Bound(_) => quote!(self.#f_state),
            spec::State::Empty(_) => quote!(#ns_core::manifold::EmptyState::REF),
        };
        let prepared = prepared::Prepared::build(paths, spec, head, entries, &state);
        let exec = &prepared.state.prepared;
        let request = &prepared.state.request;
        let guarded = &prepared.state.guarded;
        let routes_f = &prepared.state.routes;
        let admits = &prepared.guard_arms;
        let dispatch = &prepared.dispatch_arms;
        let project = &prepared.projection_arms;
        let error_ty = &prepared.error_type;
        let body_ty = &prepared.body_type;
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
            let mut #n_httpm = ::core::option::Option::None;
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
                    #method => {
                        let #n_method = #ns_http::method::Method::from_bytes(
                            #n_field.value,
                        ).map_err(#ns_core::decode::HeadError::Method)?;
                        #n_httpm = ::core::option::Option::Some(#n_method);
                    }
                    #path => #n_ptarg = ::core::option::Option::Some(#n_range),
                    #auth => #n_authrg = ::core::option::Option::Some(#n_range),
                    #proto => #n_pproto = ::core::option::Option::Some(#n_range),
                    _ => {}
                }
            };
            let #n_httpm = #n_httpm.ok_or(#ns_core::decode::HeadError::BadRequest)?;
            #target
            #key
            let #n_method = #v_dkey.ok_or(#ns_core::decode::HeadError::NotFound)?;
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
        let (guard, guards) = if entries.iter().any(|item| !item.entry.guards.is_empty()) {
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
                        #g_f: #ns_core::decode::state::Set,
                        #g_g: #ns_core::decode::state::Set,
                        #g_v: #ns_core::decode::state::admit::TryVisit<
                            #exec,
                            #g_f,
                            #g_g,
                            <#g_e as #ns_core::decode::Encode<
                                #l_req, #l_driver,
                            >>::Error,
                        >,
                    >(
                        &self,
                        #n_req: #ns_core::decode::request::Guarded<#guarded<#g_f>>,
                        #n_encode: #g_e,
                        #n_visit: &mut #g_v,
                    ) -> ::core::ops::ControlFlow<
                        #g_v::Break<#g_e::Prepared, #g_e::Target>,
                        #request<#g_g>,
                    > {
                        let (#n_req, #n_method) = #n_req.into_parts();
                        match #n_req {
                            #( #admits )*
                        }
                    }
                }),
                quote!(+ #ns_core::decode::Guarded),
            )
        } else {
            (None, proc_macro2::TokenStream::new())
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
                let #n_fields = #n_head.fields();
                let #n_plan = *#n_head.control();
                #full
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
                    #( #project )*
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
                    #( #dispatch, )*
                }
            }
        };
        Self {
            contract,
            guard,
            method: tokens,
            prepared,
        }
    }
}
