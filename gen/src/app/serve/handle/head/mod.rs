use crate::{
    app::{
        route::Method,
        serve::{self, duplex, handle::dispatch},
        spec,
    },
    runtime::paths::Paths,
};

mod names;
mod plan;

pub(super) use names::Names;

pub(super) struct Head {
    pub(super) names: Names,
    pub(super) full_head_matcher: proc_macro2::TokenStream,
    pub(super) planned_route_arms: Vec<proc_macro2::TokenStream>,
    pub(super) plan: proc_macro2::TokenStream,
}

impl Head {
    pub(super) fn build<M>(
        paths: &Paths,
        spec: &spec::Spec,
        methods: &dispatch::Methods,
        entries: &[serve::RouteSpec<'_, M>],
        connect: duplex::Connect,
    ) -> Self {
        let symbols = &spec.state_generics;
        let [n_authrg, n_caps] = symbols.locals(["__authority_range", "__captures"]);
        let [n_len] = symbols.locals(["__content_length"]);
        let [n_plan, n_error] = symbols.locals(["__control_plan", "__error"]);
        let [n_field, n_iter] = symbols.locals(["__field", "__field_iter"]);
        let [n_first] = symbols.locals(["__first_regular"]);
        let [n_head, n_hbytes] = symbols.locals(["__head", "__head_bytes"]);
        let [n_hostrg] = symbols.locals(["__host_range"]);
        let [n_httpm, n_framing] = symbols.locals(["__http_method", "__framing"]);
        let [n_name, n_pend] = symbols.locals(["__name", "__path_end"]);
        let [n_ptarg] = symbols.locals(["__planned_target"]);
        let [n_query] = symbols.locals(["__query"]);
        let [n_qrange, n_range] = symbols.locals(["__query_range", "__range"]);
        let [n_raw, n_rawhdr] = symbols.locals(["__raw", "__raw_headers"]);
        let [n_reqtgt, n_slot] = symbols.locals(["__request_target", "__slot"]);
        let [n_tag, n_trange] = symbols.locals(["__tag", "__target_range"]);
        let [n_value] = symbols.locals(["__value"]);
        use quote::{format_ident, quote};

        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let vis = &spec.vis;
        let length = quote!(const { #ns_http::field::head::Name::ContentLength.wire() });
        let host = quote!(const { #ns_http::field::head::Name::Host.wire() });
        let prepared = format_ident!("{}Head", spec.name);
        let policy = format_ident!("{}Policy", spec.name);
        let choice = format_ident!("{}HeadRoute", spec.name);
        let select = format_ident!("{}HeadSelection", spec.name);
        let cause_id = format_ident!("{}HeadCause", spec.name);
        let aliases = plan::Plan::aliases(spec);
        let defs: Vec<_> = entries
            .iter()
            .zip(&aliases)
            .map(|(item, (route, _))| {
                let variant = &item.prepared;
                quote! {
                    #variant {
                        path_end: ::core::primitive::usize,
                        captures: <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::RawParams
                            as #ns_core::route::RawParams>::Captures,
                    },
                }
            })
            .collect();
        let causes: Vec<_> = entries
            .iter()
            .filter_map(|item| {
                let variant = &item.prepared;
                let route = &item.entry.route;
                item.entry.request.any().then(|| {
                    quote! {
                        #variant(
                            <<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::Error,
                        ),
                    }
                })
            })
            .collect();
        let cause = if causes.is_empty() {
            quote!(::core::convert::Infallible)
        } else {
            quote!(#cause_id)
        };
        let cause_ty = (!causes.is_empty()).then(|| {
            quote! {
                #[doc(hidden)]
                #[derive(::core::fmt::Debug)]
                #vis enum #cause_id {
                    #( #causes )*
                }
            }
        });
        let schema = |index: usize| {
            let item = &entries[index];
            let variant = &item.prepared;
            match item.entry.request.any() {
                true => quote! {
                    return ::core::result::Result::Err(
                        #ns_core::decode::HeadError::Schema(
                            #cause_id::#variant(#n_error),
                        ),
                    );
                },
                false => quote!(match #n_error {}),
            }
        };
        let finish = |index: usize, params: proc_macro2::TokenStream, planned: bool| {
            let item = &entries[index];
            let route = &item.entry.route;
            let variant = &item.prepared;
            let slot = &item.head;
            let schema = schema(index);
            let bytes =
                (!planned).then(|| quote!(let #n_head = #ns_core::request::Head::from(#n_head);));
            let query = item.entry.request.query.then(|| {
                quote! {
                    let #n_qrange = if ::core::matches!(
                        #n_plan.form(),
                        #ns_http::field::metadata::block::Form::Path
                            | #ns_http::field::metadata::block::Form::ExtendedConnect
                    ) && #n_pend < #n_reqtgt.len() {
                        // The matched path ends before this admitted target range ends.
                        ::core::option::Option::Some(
                            (#n_trange.start + #n_pend + 1)..#n_trange.end,
                        )
                    } else {
                        ::core::option::Option::None
                    };
                    if let ::core::option::Option::Some(#n_query) = #n_qrange {
                        if let ::core::result::Result::Err(#n_error) =
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::Query
                                as #ns_core::route::field::query::Fields<
                                    <#route as #ns_core::route::Spec>::Request
                                >>::parse(
                                    #ns_core::route::field::query::Input::new(
                                        &mut #n_rawhdr,
                                        #n_hbytes,
                                        #n_query,
                                    ),
                                )
                        {
                            #schema
                        }
                    }
                }
            });
            quote! {
                #query
                // SAFETY: native iteration or the closed policy's Commit supplied
                // these ranges for this original head and its unchanged target.
                let #n_head = unsafe {
                    #ns_core::request::raw::Bind::try_head(
                        #n_head,
                        #n_ptarg.clone(),
                        ::core::option::Option::Some(#n_pend),
                        #n_authrg.clone(),
                        #n_hostrg.clone(),
                    )
                }
                .map_err(#ns_core::decode::HeadError::Bind)?;
                #bytes
                return ::core::result::Result::Ok(
                    #prepared::#variant(#slot {
                        // SAFETY: this route produced both field sets from this unchanged head.
                        head: unsafe {
                            #ns_core::request::raw::Parsed::new(
                                #n_head, #params, #n_rawhdr,
                            )
                        },
                        method: #n_httpm,
                        content_length: #n_len,
                    }),
                );
            }
        };
        let set = |index: usize| {
            let route = &entries[index].entry.route;
            let schema = schema(index);
            quote! {
                let #n_value =
                    #ns_core::request::field::SliceValue::try_new(
                    #n_hbytes,
                    #n_range,
                ).map_err(|_| #ns_core::decode::HeadError::BadRequest)?;
                if let ::core::result::Result::Err(#n_error) =
                    <<<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Schema>::Header
                        as #ns_core::route::field::header::Fields<
                            <#route as #ns_core::route::Spec>::Request,
                        >>::set(
                        &mut #n_rawhdr,
                        #n_slot,
                        &#n_value,
                )
                {
                    #schema
                }
            }
        };
        let prepare = |index: usize| {
            let route = &entries[index].entry.route;
            let setter = set(index);
            quote! {
                for (#n_field, #n_range) in
                    #n_first.into_iter().chain(#n_iter)
                {
                    if #n_field.name == #host {
                        #n_hostrg = ::core::option::Option::Some(#n_range.clone());
                    }
                    if let ::core::option::Option::Some(#n_slot) =
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Header
                            as #ns_core::route::field::header::Fields<
                                <#route as #ns_core::route::Spec>::Request,
                            >>::slice(
                                #n_field.name,
                            )
                    {
                        #setter
                    }
                }
            }
        };
        let tagged = |index: usize| {
            let route = &entries[index].entry.route;
            let setter = set(index);
            let named = |name: &proc_macro2::TokenStream| {
                quote! {
                    if let ::core::option::Option::Some(#n_slot) =
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Header
                            as #ns_core::route::field::header::Fields<
                                <#route as #ns_core::route::Spec>::Request,
                            >>::slice(#name)
                    {
                        #setter
                    }
                }
            };
            let set_len = named(&length);
            let set_host = named(&host);
            quote! {
                for #n_field in #n_iter {
                    let (#n_tag, #n_range, _) =
                        #n_field.map_err(#ns_core::decode::HeadError::Storage)?;
                    if #n_tag == #ns_http::field::metadata::section::storage::Tag::CONTENT_LENGTH {
                        #set_len
                        continue;
                    }
                    if #n_tag == #ns_http::field::metadata::section::storage::Tag::AUTHORITY
                        || #n_tag == #ns_http::field::metadata::section::storage::Tag::PROTOCOL
                    {
                        continue;
                    }
                    if #n_tag == #ns_http::field::metadata::section::storage::Tag::HOST {
                        #set_host
                        continue;
                    }
                    let ::core::option::Option::Some(#n_slot) = #n_tag.user_slot() else {
                        return ::core::result::Result::Err(
                            #ns_core::decode::HeadError::BadRequest,
                        );
                    };
                    let #n_slot =
                        <<<#route as #ns_core::route::Spec>::Request
                            as #ns_core::route::Schema>::Header
                            as #ns_core::route::field::header::Slot>::from_tag(#n_slot)
                            .map_err(#ns_core::decode::HeadError::Slot)?;
                    #setter
                }
            }
        };
        let arm = |index: usize, captures: proc_macro2::TokenStream, planned: bool| {
            let route = &entries[index].entry.route;
            let raw = if spec.route_plan.has_params(index) {
                quote! {
                    <<<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Schema>::RawParams
                        as #ns_core::route::Params>::from_match(
                        #captures,
                    )
                }
            } else {
                quote!(())
            };
            let finish = finish(index, quote!(#n_raw), planned);
            let body = if planned {
                tagged(index)
            } else {
                prepare(index)
            };
            quote! {
                let #n_raw = #raw;
                let mut #n_rawhdr =
                    <<<#route as #ns_core::route::Spec>::Request
                        as #ns_core::route::Schema>::RawHeaders
                        as ::core::default::Default>::default();
                let #n_len = #n_head
                    .framing()
                    .and_then(|#n_framing| #n_framing.content_length())
                    .map(<::core::primitive::usize as ::core::convert::TryFrom<
                        ::core::primitive::u64,
                    >>::try_from)
                    .transpose()
                    .map_err(|_| #ns_core::decode::HeadError::ContentLength(
                        #ns_http::field::text::IntError::Overflow,
                    ))?;
                #body
                #finish
            }
        };
        let matcher = spec
            .route_plan
            .compile(paths, &spec.state_generics, |index, captures| {
                arm(index, captures, false)
            });
        let arms: Vec<proc_macro2::TokenStream> = entries
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let entry = item.entry;
                let variant = &item.prepared;
                let method = entry.method.http_token(paths);
                let form = match entry.method {
                    Method::Connect if entry.protocol.is_some() => quote! {
                        #ns_http::field::metadata::block::Form::ExtendedConnect
                    },
                    Method::Connect => quote! {
                        #ns_http::field::metadata::block::Form::Connect
                    },
                    Method::Options if entry.path.value() == "*" => quote! {
                        #ns_http::field::metadata::block::Form::Asterisk
                    },
                    Method::Get
                    | Method::Post
                    | Method::Put
                    | Method::Patch
                    | Method::Delete
                    | Method::Head
                    | Method::Options => quote! {
                        #ns_http::field::metadata::block::Form::Path
                    },
                };
                let guard = quote! {
                    if #n_plan.form() != #form {
                        return ::core::result::Result::Err(
                            #ns_core::decode::HeadError::NotFound,
                        );
                    }
                };
                let body = arm(index, quote!(#n_caps), true);
                quote! {
                    #choice::#variant {
                        path_end: #n_pend,
                        captures: #n_caps,
                    } => {
                        #guard
                        let #n_httpm = #method;
                        #body
                    }
                }
            })
            .collect();
        let heads = spec
            .route_plan
            .compile(paths, &spec.state_generics, |index, captures| {
                let variant = &entries[index].prepared;
                quote! {
                    return ::core::option::Option::Some(
                        #choice::#variant {
                            path_end: #n_pend,
                            captures: #captures,
                        },
                    );
                }
            });
        let dispo: Vec<proc_macro2::TokenStream> = entries
            .iter()
            .zip(&aliases)
            .map(|(item, (route, _))| {
                let variant = &item.prepared;
                quote! {
                    #choice::#variant { .. } => {
                        let ::core::option::Option::Some(#n_slot) =
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::Header
                                as #ns_core::route::field::header::Fields<
                                    <#route as #ns_core::route::Spec>::Request,
                                >>::span(
                                    #n_name,
                                )
                        else {
                            return #ns_http::field::metadata::section::rules::Disposition::Discard;
                        };
                        let #n_slot =
                            <<<#route as #ns_core::route::Spec>::Request
                                as #ns_core::route::Schema>::Header
                                as #ns_core::route::field::header::Slot>::into_tag(#n_slot);
                        #ns_http::field::metadata::section::rules::Disposition::Tagged(
                            #ns_http::field::metadata::section::storage::Tag::user(#n_slot),
                        )
                    }
                }
            })
            .collect();
        let names = Names {
            prepared_ident: prepared,
            head_plan_ident: policy,
            head_route_ident: choice,
            head_selection_ident: select,
            cause,
        };
        let built = plan::Plan {
            spec,
            aliases: &aliases,
            variants: &defs,
            names: &names,
            matcher: &heads,
            connect,
            arms: &dispo,
            method: &methods.head_method_key,
        }
        .build(paths);
        Self {
            names,
            full_head_matcher: matcher,
            planned_route_arms: arms,
            plan: quote! {
                #cause_ty
                #built
            },
        }
    }
}
