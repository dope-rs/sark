use crate::{
    app::{route, serve::duplex, spec},
    runtime::paths::Paths,
};

pub(super) struct Plan<'a> {
    pub(super) spec: &'a spec::Spec,
    pub(super) aliases: &'a [(syn::Ident, &'a syn::TypePath)],
    pub(super) variants: &'a [proc_macro2::TokenStream],
    pub(super) names: &'a super::Names,
    pub(super) matcher: &'a proc_macro2::TokenStream,
    pub(super) connect: duplex::Connect,
    pub(super) arms: &'a [proc_macro2::TokenStream],
    pub(super) method: &'a proc_macro2::TokenStream,
}

impl Plan<'_> {
    pub(super) fn aliases(spec: &spec::Spec) -> Vec<(syn::Ident, &syn::TypePath)> {
        spec.route_specs
            .iter()
            .enumerate()
            .map(|(index, entry)| (quote::format_ident!("Route{index}"), &entry.route))
            .collect()
    }

    pub(super) fn build(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let symbols = &self.spec.state_generics;
        let [n_field, n_input] = symbols.locals(["__field", "__input"]);
        let [n_known, n_method] = symbols.locals(["__known", "__method"]);
        let [n_mbytes] = symbols.locals(["__method_bytes"]);
        let [n_mkey, n_name] = symbols.locals(["__method_key", "__name"]);
        let [n_proto, n_kept] = symbols.locals(["__protocol", "__retained"]);
        let [n_route, n_rproto] = symbols.locals(["__route", "__route_protocol"]);
        let [n_tag, n_target] = symbols.locals(["__tag", "__target"]);
        let [n_value, v_route, v_this] = symbols.locals(["__value", "route", "this"]);
        use quote::quote;

        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let vis = &self.spec.vis;
        let (aliases, sources): (Vec<&syn::Ident>, Vec<&&syn::TypePath>) = self
            .aliases
            .iter()
            .map(|(alias, source)| (alias, source))
            .unzip();
        let captures = (0..self.aliases.len())
            .map(|index| self.spec.route_plan.capture_type(paths, index))
            .collect::<Vec<_>>();
        let generics = quote!(<#(#aliases: #ns_core::route::Spec<
            Request: #ns_core::route::Schema<
                RawParams: #ns_core::route::RawParams<Captures = #captures>,
            >,
        >,)*>);
        let args = quote!(<#(#aliases,)*>);
        let variants = self.variants;
        let plan = &self.names.head_plan_ident;
        let module = crate::app::scope::module(&self.spec.name);
        let route = &self.names.head_route_ident;
        let select = &self.names.head_selection_ident;
        let matcher = self.matcher;
        let retain = quote! {
            false #(
                || <<#aliases as #ns_core::route::Spec>::Request
                    as #ns_core::route::Schema>::AUTHORITY
            )*
        };
        let connect = self.spec.route_specs.iter().any(|entry| {
            matches!(entry.method, route::Method::Connect) && entry.protocol.is_none()
        });
        let protocol = match self.connect {
            duplex::Connect::Enabled => quote! {
                #ns_http::field::metadata::section::rules::Disposition::Tagged(
                    #ns_http::field::metadata::section::storage::Tag::PROTOCOL,
                )
            },
            duplex::Connect::Disabled => {
                quote!(#ns_http::field::metadata::section::rules::Disposition::Discard)
            }
        };
        let arms = self.arms;
        let method = self.method;
        let (key, proto) = match variants.is_empty() {
            true => (quote!(_), quote!(_)),
            false => (quote!(#n_method), quote!(#n_rproto)),
        };

        quote! {
            #vis type #plan = #module::#plan<#(#sources,)*>;
            #vis type #select = #module::#select<#(#sources,)*>;
            type #route = #module::#route<#(#sources,)*>;

            mod #module {
            #[doc(hidden)]
            pub(super) enum #route #generics {
                Pending,
                Missing,
                #( #variants )*
            }

            impl #generics ::core::marker::Copy for #route #args {}

            impl #generics ::core::clone::Clone for #route #args {
                fn clone(&self) -> Self {
                    *self
                }
            }

            #[doc(hidden)]
            pub struct #plan #generics {
                method: ::core::option::Option<
                    #ns_http::method::Known,
                >,
                target: ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                authority: ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                host: ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                protocol: ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                route: #route #args,
            }

            #[doc(hidden)]
            pub struct #select #generics {
                plan: #plan #args,
            }

            impl #generics #select #args {
                pub(super) fn parts(&self) -> (
                    #route #args,
                    ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                    ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                    ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                    ::core::option::Option<::core::ops::Range<::core::primitive::usize>>,
                    ::core::option::Option<#ns_http::method::Known>,
                ) {
                    (
                        self.plan.route,
                        self.plan.target.clone(),
                        self.plan.authority.clone(),
                        self.plan.host.clone(),
                        self.plan.protocol.clone(),
                        self.plan.method,
                    )
                }
            }

            impl #generics #plan #args {
                pub(super) fn select(
                    #key: #ns_http::method::Known,
                    #n_target: &[::core::primitive::u8],
                    #proto: ::core::option::Option<&[::core::primitive::u8]>,
                ) -> ::core::option::Option<#route #args> {
                    if #n_target.first() != ::core::option::Option::Some(&b'/')
                        && #n_target != b"*"
                    {
                        return ::core::option::Option::None;
                    }
                    #matcher
                    ::core::option::Option::None
                }

                fn connect(&mut self) {
                    if ::core::matches!(self.route, #route::Pending)
                        && self.method == ::core::option::Option::Some(
                            #ns_http::method::Known::Connect,
                        )
                        && self.target.is_none()
                        && self.protocol.is_none()
                    {
                        self.route = match Self::select(
                            #ns_http::method::Known::Connect,
                            b"*",
                            ::core::option::Option::None,
                        ) {
                            ::core::option::Option::Some(#v_route) => #v_route,
                            ::core::option::Option::None => #route::Missing,
                        };
                    }
                }

                fn resolve(
                    &mut self,
                    #n_kept: &[::core::primitive::u8],
                ) -> #ns_http::field::metadata::block::Validation {
                    if !::core::matches!(self.route, #route::Pending) {
                        return #ns_http::field::metadata::block::Validation::Valid;
                    }
                    let (#n_method, #n_target) = match (self.method, self.target.clone()) {
                        (
                            ::core::option::Option::Some(#n_method),
                            ::core::option::Option::Some(#n_target),
                        ) => (#n_method, #n_target),
                        _ => return #ns_http::field::metadata::block::Validation::Valid,
                    };
                    let #n_rproto = match self.protocol.as_ref() {
                        ::core::option::Option::Some(#n_proto) => {
                            let ::core::option::Option::Some(#n_proto) =
                                #n_kept.get(#n_proto.clone())
                            else {
                                return #ns_http::field::metadata::block::Validation::Malformed;
                            };
                            ::core::option::Option::Some(#n_proto)
                        }
                        ::core::option::Option::None => ::core::option::Option::None,
                    };
                    let ::core::option::Option::Some(#n_target) = #n_kept.get(#n_target) else {
                        return #ns_http::field::metadata::block::Validation::Malformed;
                    };
                    self.route = match Self::select(
                        #n_method,
                        #n_target,
                        #n_rproto,
                    ) {
                        ::core::option::Option::Some(#n_route) => #n_route,
                        ::core::option::Option::None => #route::Pending,
                    };
                    #ns_http::field::metadata::block::Validation::Valid
                }
            }

            impl #generics ::core::default::Default for #plan #args {
                fn default() -> Self {
                    Self {
                        method: ::core::option::Option::None,
                        target: ::core::option::Option::None,
                        authority: ::core::option::Option::None,
                        host: ::core::option::Option::None,
                        protocol: ::core::option::Option::None,
                        route: #route::Pending,
                    }
                }
            }

            impl #generics #ns_http::field::metadata::section::Policy for #plan #args {
                type Selection = #select #args;
                type Block = #ns_http::field::metadata::section::storage::Fields;

                fn finish(mut self) -> Self::Selection {
                    self.connect();
                    #select {
                        plan: self,
                    }
                }
            }

            impl #generics #ns_http::field::metadata::section::rules::Plan for #plan #args {
                type Inspect = #ns_http::field::metadata::section::rules::Inspect;
                const SKIP_DISCARDED: ::core::primitive::bool = true;

                fn disposition(
                    #n_input: #ns_http::field::metadata::section::rules::Candidate<'_, Self>,
                ) -> #ns_http::field::metadata::section::rules::Disposition<Self::Inspect> {
                    let (#v_this, #n_name, #n_known) = #n_input.classify();
                    match #n_known {
                        #ns_http::field::head::Name::Method =>
                            return #ns_http::field::metadata::section::rules::Disposition::Inspect(
                                #ns_http::field::metadata::section::rules::Inspect::Method,
                            ),
                        #ns_http::field::head::Name::Path => {
                            return #ns_http::field::metadata::section::rules::Disposition::Tagged(
                                #ns_http::field::metadata::section::storage::Tag::PATH,
                            );
                        }
                        #ns_http::field::head::Name::Authority => {
                            return if #connect || #retain {
                                #ns_http::field::metadata::section::rules::Disposition::Tagged(
                                    #ns_http::field::metadata::section::storage::Tag::AUTHORITY,
                                )
                            } else {
                                #ns_http::field::metadata::section::rules::Disposition::Discard
                            };
                        }
                        #ns_http::field::head::Name::Protocol => {
                            return #protocol;
                        }
                        #ns_http::field::head::Name::Host
                            if #retain => {
                                return #ns_http::field::metadata::section::rules::Disposition::Tagged(
                                    #ns_http::field::metadata::section::storage::Tag::HOST,
                                );
                            }
                        #ns_http::field::head::Name::ContentLength => {
                            return #ns_http::field::metadata::section::rules::Disposition::Tagged(
                                #ns_http::field::metadata::section::storage::Tag::CONTENT_LENGTH,
                            );
                        }
                        #ns_http::field::head::Name::Host
                        | #ns_http::field::head::Name::Te
                        | #ns_http::field::head::Name::Regular => {}
                        _ => return #ns_http::field::metadata::section::rules::Disposition::Discard,
                    }
                    #v_this.connect();
                    match #v_this.route {
                        #( #arms )*
                        #route::Pending | #route::Missing =>
                            #ns_http::field::metadata::section::rules::Disposition::Discard,
                    }
                }

                fn inspect(
                    #ns_http::field::metadata::section::rules::Inspect::Method: Self::Inspect,
                    #n_input: #ns_http::field::metadata::section::rules::Observation<
                        '_,
                        Self,
                    >,
                ) -> #ns_http::field::metadata::block::Validation {
                    let (
                        #v_this,
                        #n_field,
                        #n_kept,
                    ) = #n_input.into_parts();
                    let #n_mbytes = #n_field.value;
                    #method
                    #v_this.method = #n_mkey;
                    #v_this.resolve(#n_kept)
                }

                fn commit(
                    #n_input: #ns_http::field::metadata::section::rules::Commit<'_, Self>,
                ) -> #ns_http::field::metadata::block::Validation {
                    let (#v_this, #n_tag, #n_value, #n_kept) = #n_input.into_parts();
                    match #n_tag {
                        #ns_http::field::metadata::section::storage::Tag::PATH => {
                            #v_this.target = ::core::option::Option::Some(#n_value);
                            return #v_this.resolve(#n_kept);
                        }
                        #ns_http::field::metadata::section::storage::Tag::AUTHORITY =>
                            #v_this.authority = ::core::option::Option::Some(#n_value),
                        #ns_http::field::metadata::section::storage::Tag::HOST =>
                            #v_this.host = ::core::option::Option::Some(#n_value),
                        #ns_http::field::metadata::section::storage::Tag::PROTOCOL => {
                            #v_this.protocol = ::core::option::Option::Some(#n_value);
                            return #v_this.resolve(#n_kept);
                        }
                        _ => {}
                    }
                    #ns_http::field::metadata::block::Validation::Valid
                }

            }
            }
        }
    }
}
