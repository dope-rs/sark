use crate::{
    macros::tokens::local,
    request::query,
    runtime::paths::Paths,
    types::{self, lengths},
};

pub(crate) fn emit_query(
    paths: &Paths,
    kind: types::ValueKind,
    field: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let [entry, span, value, cause] = ["entry", "span", "value", "cause"].map(local);
    match kind {
        types::ValueKind::Bytes => quote::quote! {{
            let #span = #entry.try_span()
                .map_err(|#cause| #ns_core::route::Error::Query {
                        field: #field,
                        cause: #ns_core::request::field::Error::Span(#cause),
                })?;
            ::core::option::Option::Some(#span)
        }},
        _ => {
            quote::quote! {{
                ::core::option::Option::Some(#entry.try_value()
                    .and_then(|#value| #ns_core::request::field::Value::parse_value(&#value))
                    .map_err(|#cause| #ns_core::route::Error::Query {
                        field: #field,
                        cause: #cause,
                    })?)
            }}
        }
    }
}

pub(super) struct FieldSpec {
    pub(super) slot: u16,
    pub(super) variant: proc_macro2::Ident,
    pub(super) ident: proc_macro2::Ident,
    pub(super) bytes: Vec<u8>,
    pub(super) span: proc_macro2::Span,
    pub(super) kind: types::ValueKind,
}

pub(crate) struct FieldPlan {
    entries: Vec<FieldSpec>,
}

impl FieldPlan {
    pub(crate) fn collect<'a>(
        entries: impl IntoIterator<Item = (&'a proc_macro2::Ident, &'a syn::LitStr, types::ValueKind)>,
    ) -> syn::Result<Self> {
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(idx, (ident, name, kind))| {
                let slot = match u16::try_from(idx) {
                    Ok(slot) => slot,
                    Err(error) => return Err(syn::Error::new_spanned(ident, error)),
                };
                Ok(FieldSpec {
                    slot,
                    variant: quote::format_ident!("S{}", idx),
                    ident: ident.clone(),
                    bytes: name.value().into_bytes(),
                    span: name.span(),
                    kind,
                })
            })
            .collect::<syn::Result<_>>()?;
        Ok(Self { entries })
    }

    pub(super) fn entries(&self) -> &[FieldSpec] {
        &self.entries
    }

    pub(crate) fn slots(
        &self,
        paths: &Paths,
        vis: &syn::Visibility,
        kind: &proc_macro2::Ident,
        identity: &proc_macro2::Ident,
        raw_headers: &proc_macro2::Ident,
    ) -> (
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    ) {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let [v_tag, v_name, v_heads, v_slot, v_value, v_cause] =
            ["tag", "name", "headers", "slot", "value", "cause"].map(local);
        let variants: Vec<_> = self.entries.iter().map(|field| &field.variant).collect();
        let indices: Vec<_> = self
            .entries
            .iter()
            .map(|field| u32::from(field.slot))
            .collect();
        let admitted = self.entries.last().map(|field| {
            let index = u32::from(field.slot);
            quote::quote_spanned! {field.ident.span()=>
                const _: #ns_http::field::metadata::section::storage::Slot =
                    #ns_http::field::metadata::section::storage::Slot::exact::<#index>();
            }
        });
        let declaration = quote::quote! {
            #admitted
            #[repr(u16)]
            #[derive(::core::clone::Clone, ::core::marker::Copy)]
            #vis enum #kind { #( #variants, )* }
        };
        let def = quote::quote! {
            impl #ns_core::route::field::header::Slot for #kind {
                fn into_tag(self) -> #ns_http::field::metadata::section::storage::Slot {
                    match self {
                        #( Self::#variants =>
                            #ns_http::field::metadata::section::storage::Slot::exact::<#indices>(), )*
                    }
                }

                fn from_tag(
                    #v_tag: #ns_http::field::metadata::section::storage::Slot,
                ) -> ::core::result::Result<
                    Self,
                    #ns_core::route::field::header::Tag,
                > {
                    match #v_tag.get() {
                        #( #indices => ::core::result::Result::Ok(Self::#variants), )*
                        _ => ::core::result::Result::Err(#ns_core::route::field::header::Tag::new(#v_tag.get() as ::core::primitive::u16)),
                    }
                }
            }
        };
        let raw_arms = self.header_arms(kind, true);
        let raw = quote::quote! {
            match #v_name.len() {
                #( #raw_arms )*
                _ => {}
            }
            ::core::option::Option::None
        };
        let buf_arms = self.header_arms(kind, false);
        let span = quote::quote! {
            match #v_name.len() {
                #( #buf_arms )*
                _ => {}
            }
            ::core::option::Option::None
        };
        let sets: Vec<_> = self
            .entries
            .iter()
            .map(|spec| {
                let slot = &spec.variant;
                let ident = &spec.ident;
                let field = quote::format_ident!("Header{}", spec.slot);
                let parsed = match spec.kind {
                    types::ValueKind::Bytes => quote::quote! {
                        #ns_core::request::field::HeaderValue::try_span(#v_value)
                    },
                    types::ValueKind::Encoding => quote::quote! {
                        #ns_http::field::head::AcceptEncoding::try_parse(#v_value.as_ref())
                            .map_err(#ns_core::request::field::Error::AcceptEncoding)
                    },
                    types::ValueKind::Usize | types::ValueKind::U64 | types::ValueKind::Bool => {
                        quote::quote! {
                            #ns_core::request::field::Value::parse_value(#v_value)
                        }
                    }
                };
                let parsed = quote::quote! {
                    #parsed.map_err(|#v_cause| #ns_core::route::Error::Header {
                        field: #identity::#field,
                        cause: #v_cause,
                    })?
                };
                let assign = if matches!(spec.kind, types::ValueKind::Encoding) {
                    quote::quote! {
                        Self::set_encoding(#v_heads, Self::#slot, #v_value, #parsed)?;
                    }
                } else {
                    quote::quote! {
                        if #v_heads.#ident.is_none() {
                            #v_heads.#ident = ::core::option::Option::Some(#parsed);
                        }
                    }
                };
                quote::quote! { #kind::#slot => { #assign } }
            })
            .collect();
        let set = quote::quote! {
            match #v_slot {
                #( #sets, )*
            }
            ::core::result::Result::Ok(())
        };
        let encoding = self
            .entries
            .iter()
            .find(|spec| matches!(spec.kind, types::ValueKind::Encoding))
            .map(|spec| {
                let ident = &spec.ident;
                let slot = &spec.variant;
                let v_encoding = local("encoding");
                let other: Vec<_> = self
                    .entries
                    .iter()
                    .filter(|entry| entry.slot != spec.slot)
                    .map(|entry| &entry.variant)
                    .collect();
                let fallback = (!other.is_empty()).then(|| {
                    quote::quote! {
                        #(Self::#other)|* => Self::set(#v_heads, #v_slot, #v_value),
                    }
                });
                quote::quote! {
                    const ENCODING: ::core::primitive::bool = true;

                    fn set_encoding<__V: #ns_core::request::field::HeaderValue>(
                        #v_heads: &mut #raw_headers,
                        #v_slot: Self,
                        #v_value: &__V,
                        #v_encoding: #ns_http::field::head::AcceptEncoding,
                    ) -> ::core::result::Result<(), #ns_core::route::Error<#identity>> {
                        match #v_slot {
                            Self::#slot => {
                                match &mut #v_heads.#ident {
                                    ::core::option::Option::Some(#v_value) => #v_value.append(#v_encoding),
                                    ::core::option::Option::None => #v_heads.#ident = ::core::option::Option::Some(#v_encoding),
                                }
                                ::core::result::Result::Ok(())
                            }
                            #fallback
                        }
                    }
                }
            });
        (declaration, def, raw, span, set, quote::quote!(#encoding))
    }

    fn header_arms(&self, kind: &proc_macro2::Ident, fold: bool) -> Vec<proc_macro2::TokenStream> {
        let name = local("name");
        lengths::LengthArms::collect(self.entries.iter().map(|field| {
            let var = &field.variant;
            let bytes = &field.bytes;
            let lit = syn::LitByteStr::new(bytes.as_slice(), proc_macro2::Span::call_site());
            let cond = if fold {
                quote::quote! { #name.as_ref().eq_ignore_ascii_case(#lit) }
            } else {
                quote::quote! { #name.eq_ascii(#lit) }
            };
            (
                bytes.len(),
                quote::quote! { if #cond { return ::core::option::Option::Some(#kind::#var); } },
            )
        }))
        .emit()
    }

    pub(crate) fn parse_query(
        &self,
        paths: &Paths,
        identity: &proc_macro2::Ident,
    ) -> proc_macro2::TokenStream {
        let [name, headers] = ["name", "headers"].map(local);
        let arms = lengths::LengthArms::collect(self.entries.iter().map(|spec| {
            let field = quote::format_ident!("Query{}", spec.slot);
            let ident = &spec.ident;
            let bytes = &spec.bytes;
            let lit = syn::LitByteStr::new(bytes.as_slice(), proc_macro2::Span::call_site());
            let parsed = emit_query(paths, spec.kind, quote::quote!(#identity::#field));
            (
                bytes.len(),
                quote::quote! {
                    if #name == #lit {
                        if #headers.#ident.is_none() {
                            #headers.#ident = #parsed;
                        }
                    }
                },
            )
        }))
        .emit();
        let scan = quote::quote! {
            match #name.len() {
                #( #arms )*
                _ => {}
            }
        };
        query::build(paths, scan)
    }
}
