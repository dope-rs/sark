mod hidden;
mod params;
mod query;
mod raw_body;

use hidden::Hidden;
use params::Params;
use proc_macro2::{Span, TokenStream};
use query::Query;
use quote::{format_ident, quote};
use raw_body::BodyPlan;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Fields, Ident, ItemStruct, Result, Token};

use crate::codegen::header::{HeaderApplyMode, HeaderEmitter, HeaderParserConfig, HeaderValueMode};
use crate::codegen::value::FieldPlan;
use crate::model::{HeaderAttrField, PathAttrField, QueryAttrField};
use crate::util::{AttributeSliceExt, FieldAttr, TypeExt};

pub(super) struct Mode {
    ordered: bool,
    value: HeaderValueMode,
    apply: HeaderApplyMode,
}

impl Parse for Mode {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.is_empty() {
            return Ok(Self {
                ordered: false,
                value: HeaderValueMode::Full,
                apply: HeaderApplyMode::Full,
            });
        }
        let mut ordered = false;
        let mut value = HeaderValueMode::Full;
        let mut apply = HeaderApplyMode::Full;
        while !input.is_empty() {
            if input.peek(Ident) && !input.peek2(Token![=]) {
                let ident = input.parse::<Ident>()?;
                if ident != "ordered" {
                    return Err(Error::new_spanned(
                        ident,
                        "#[sark_gen::request] supports only `ordered`, `value = full|skip`, and `apply = full|skip`",
                    ));
                }
                ordered = true;
            } else {
                let key = input.parse::<Ident>()?;
                input.parse::<Token![=]>()?;
                let mode = input.parse::<Ident>()?;
                if key != "value" && key != "apply" {
                    return Err(Error::new_spanned(
                        key,
                        "#[sark_gen::request] supports only `ordered`, `value = full|skip`, and `apply = full|skip`",
                    ));
                }
                match (key.to_string().as_str(), mode.to_string().as_str()) {
                    ("value", "full") => value = HeaderValueMode::Full,
                    ("value", "skip") => value = HeaderValueMode::Skip,
                    ("apply", "full") => apply = HeaderApplyMode::Full,
                    ("apply", "skip") => apply = HeaderApplyMode::Skip,
                    ("value", _) => {
                        return Err(Error::new_spanned(
                            mode,
                            "#[sark_gen::request] value mode must be `full` or `skip`",
                        ));
                    }
                    ("apply", _) => {
                        return Err(Error::new_spanned(
                            mode,
                            "#[sark_gen::request] apply mode must be `full` or `skip`",
                        ));
                    }
                    _ => unreachable!(),
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        if matches!(value, HeaderValueMode::Skip) {
            apply = HeaderApplyMode::Skip;
        }
        Ok(Self {
            ordered,
            value,
            apply,
        })
    }
}

impl Mode {
    pub(super) fn empty() -> Self {
        Self {
            ordered: false,
            value: HeaderValueMode::Full,
            apply: HeaderApplyMode::Full,
        }
    }

    pub(super) fn expand(self, mut st: ItemStruct) -> Result<TokenStream> {
        let ordered = self.ordered;
        let parser_cfg = HeaderParserConfig {
            value: self.value,
            apply: self.apply,
        };
        let name = st.ident.clone();
        let vis = st.vis.clone();
        let inner_name = format_ident!("{}View", name);
        let raw_headers_ident = format_ident!("{}HeadersRaw", name);
        let header_slot_ident = format_ident!("{}HeaderSlot", name);
        let raw_params_ident = format_ident!("{}ParamsRaw", name);
        let headers_inner_ident = format_ident!("{}Headers", name);
        let params_inner_ident = format_ident!("{}Params", name);
        let mut body_plan = BodyPlan::from_attrs(&mut st.attrs)?;
        let Fields::Named(named) = &mut st.fields else {
            return Err(Error::new(
                Span::call_site(),
                "#[sark_gen::request] requires a named-field struct",
            ));
        };

        let mut header_fields = Vec::<HeaderAttrField>::new();
        let mut query_fields = Vec::<QueryAttrField>::new();
        let mut path_fields = Vec::<PathAttrField>::new();
        let mut ctor_init_ref = Vec::new();

        for field in &mut named.named {
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| Error::new(Span::call_site(), "named field required"))?;
            if ident == "body" {
                return Err(Error::new_spanned(
                    &field.ident,
                    "#[sark_gen::request] reserves field name `body`",
                ));
            }
            let header = field.attrs.field_attr("header");
            let query = field.attrs.field_attr("query");
            let path = field.attrs.field_attr("path");
            let is_raw_body = field
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("raw_body"));
            let is_body_len = field
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("body_len"));
            let kinds = [
                header.is_some(),
                query.is_some(),
                path.is_some(),
                is_raw_body,
                is_body_len,
            ]
            .into_iter()
            .filter(|v| *v)
            .count();
            if kinds > 1 {
                return Err(Error::new_spanned(
                    field,
                    "request field must have exactly one of #[header(...)], #[query(...)], #[path(...)], #[raw_body], or #[body_len]",
                ));
            }
            if let Some(FieldAttr {
                name: header,
                default,
            }) = header
            {
                header_fields.push(HeaderAttrField {
                    ident: ident.clone(),
                    header,
                    default,
                    ty: field.ty.clone(),
                });
                ctor_init_ref.push(quote!(#ident: headers.#ident));
            } else if let Some(FieldAttr {
                name: query,
                default,
            }) = query
            {
                query_fields.push(QueryAttrField {
                    ident: ident.clone(),
                    query,
                    default,
                    ty: field.ty.clone(),
                });
                ctor_init_ref.push(quote!(#ident: headers.#ident));
            } else if let Some(FieldAttr {
                name: path,
                default,
            }) = path
            {
                path_fields.push(PathAttrField {
                    ident: ident.clone(),
                    path,
                    default,
                    ty: field.ty.clone(),
                });
                ctor_init_ref.push(quote!(#ident: params.#ident));
            } else if is_raw_body {
                body_plan.register_raw(ident.clone(), &field.ty)?;
            } else if is_body_len {
                body_plan.register_length(ident.clone(), &field.ty)?;
            } else {
                return Err(Error::new_spanned(
                    field,
                    "#[sark_gen::request] fields require #[header(...)], #[query(...)], #[path(...)], #[raw_body], or #[body_len]",
                ));
            }
            field.attrs.retain(|attr| {
                !attr.path().is_ident("header")
                    && !attr.path().is_ident("query")
                    && !attr.path().is_ident("path")
                    && !attr.path().is_ident("raw_body")
                    && !attr.path().is_ident("body_len")
            });
        }

        body_plan.append_json_field(named, &vis);
        ctor_init_ref.extend(body_plan.constructor_fields());

        let borrowed_byte_fields: Vec<Ident> = header_fields
            .iter()
            .filter(|field| field.ty.has_retained_bytes())
            .map(|field| field.ident.clone())
            .chain(
                query_fields
                    .iter()
                    .filter(|field| field.ty.has_retained_bytes())
                    .map(|field| field.ident.clone()),
            )
            .chain(
                path_fields
                    .iter()
                    .filter(|field| field.ty.has_retained_bytes())
                    .map(|field| field.ident.clone()),
            )
            .collect();
        let mut borrowed_st = st.clone();
        let Fields::Named(borrowed_named) = &mut borrowed_st.fields else {
            unreachable!("named fields validated above")
        };
        for field in &mut borrowed_named.named {
            body_plan.rewrite_raw_field(field);
            if field
                .ident
                .as_ref()
                .is_some_and(|ident| borrowed_byte_fields.contains(ident))
            {
                field.ty.rewrite_retained_to_borrowed();
            }
        }
        borrowed_named.named.push(syn::parse_quote! {
            #[doc(hidden)]
            pub __sark_req_marker: ::core::marker::PhantomData<&'req ()>
        });
        ctor_init_ref.push(quote!(__sark_req_marker: ::core::marker::PhantomData));
        borrowed_st.ident = inner_name.clone();
        if !borrowed_st
            .generics
            .params
            .iter()
            .any(|p| matches!(p, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "req"))
        {
            borrowed_st
                .generics
                .params
                .insert(0, syn::parse_quote!('req));
        }
        st.attrs.push(syn::parse_quote!(#[allow(dead_code)]));
        borrowed_st
            .attrs
            .push(syn::parse_quote!(#[allow(dead_code)]));

        let parsed_body_impl = body_plan.parsed_body_impl();

        let header_tokens = if header_fields.is_empty() && query_fields.is_empty() {
            quote! {
                #[allow(non_camel_case_types, dead_code)]
                #[derive(Default)]
                struct #raw_headers_ident;

                #[allow(non_camel_case_types, dead_code)]
                struct #headers_inner_ident<'req> {
                    marker: ::core::marker::PhantomData<&'req ()>,
                }
            }
        } else {
            Hidden {
                inner_name: &headers_inner_ident,
                raw_name: &raw_headers_ident,
                headers: &header_fields,
                queries: &query_fields,
                ordered_query_state: ordered && !query_fields.is_empty(),
            }
            .build()?
        };

        let header_plan = FieldPlan::collect(
            header_fields
                .iter()
                .map(|f| (&f.ident, f.header.value().into_bytes(), &f.ty)),
        )?;
        let query_plan = FieldPlan::collect(
            query_fields
                .iter()
                .map(|f| (&f.ident, f.query.value().into_bytes(), &f.ty)),
        )?;

        let (
            header_slot_ty,
            header_slot_enum,
            header_slot_probe_fn,
            header_set_fn,
            header_set_name_fn,
            header_slot_u8_fn,
            header_set_u8_fn,
        ) = if header_fields.is_empty() {
            (
                quote! { #header_slot_ident },
                quote! {
                    #[derive(Clone, Copy)]
                    struct #header_slot_ident;
                },
                quote! { None },
                quote! { Ok(()) },
                quote! { Ok(()) },
                quote! { None },
                quote! { Ok(()) },
            )
        } else {
            let (slot_enum, slot_probe_fn, set_fn, set_name_fn, slot_probe_u8_fn, set_u8_fn) =
                header_plan.slots(&header_slot_ident, true);
            (
                quote! { #header_slot_ident },
                slot_enum,
                slot_probe_fn,
                set_fn,
                set_name_fn,
                slot_probe_u8_fn,
                set_u8_fn,
            )
        };
        let header_emitter = HeaderEmitter::new(&header_plan, parser_cfg);
        let header_scan_fn = quote! {
            Ok(sark::sark_core::http::head::HeaderLineScan::find(rest, 0))
        };
        let header_contig_fn = header_emitter.contiguous()?;
        let header_apply_fn = header_emitter.apply()?;
        let query = Query::new(&query_fields);
        let query_set_name_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            query.set_name_direct()?
        } else {
            query_plan.set_name(false)
        };
        let query_parse_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            Query::parse_direct()
        } else {
            query_plan.parse_query()
        };
        let query_set_slice_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            query.set_slice_direct()?
        } else {
            query_plan.set_slice()
        };
        let need_header = !(header_fields.is_empty() && query_fields.is_empty());
        let need_known_header = header_emitter.needs_known_header();
        let need_query = !query_fields.is_empty();

        let build_headers = if header_fields.is_empty() && query_fields.is_empty() {
            quote! {
                let _ = headers;
                Ok(#headers_inner_ident {
                    marker: ::core::marker::PhantomData,
                })
            }
        } else {
            quote!(#headers_inner_ident::<'req>::from_raw(req, headers))
        };
        let params_tokens = Params {
            vis: &vis,
            inner_ident: &params_inner_ident,
            raw_ident: &raw_params_ident,
            fields: &path_fields,
        }
        .build()?;
        let body_policy = body_plan.policy();

        let header_methods = if header_fields.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                fn header_slot_bytes(name: &[u8]) -> Option<Self::HeaderSlot> {
                    #header_slot_probe_fn
                }

                fn header_slot_u8(name: &[u8]) -> Option<u8> {
                    #header_slot_u8_fn
                }

                fn scan_header_contig(
                    rest: &[u8],
                ) -> sark::error::Result<Option<sark::sark_core::http::head::HeaderLineScan>> {
                    #header_scan_fn
                }

                #[allow(unreachable_code)]
                fn apply_header_contig<I: sark::sark_core::http::head::HeadInput + ?Sized>(
                    headers: &mut Self::RawHeaders,
                    input: &I,
                    rest: &[u8],
                    line_start: usize,
                    scan: &mut sark_core::http::codec::HeaderScan,
                    flags: &mut sark::sark_core::http::head::Flags,
                    header_count: &mut usize,
                    max_header_count: usize,
                ) -> sark::error::Result<Option<usize>> {
                    #header_contig_fn
                }

                fn apply_header<I: sark::sark_core::http::head::HeadInput + ?Sized>(
                    headers: &mut Self::RawHeaders,
                    input: &I,
                    line: &[u8],
                    line_start: usize,
                    colon_idx: usize,
                    pretrim_start: Option<usize>,
                    pretrim_end: Option<usize>,
                    scan: &mut sark_core::http::codec::HeaderScan,
                    flags: &mut sark::sark_core::http::head::Flags,
                    scan_info: Option<&sark::sark_core::http::head::HeaderLineScan>,
                ) -> sark::error::Result<()> {
                    #header_apply_fn
                }

                fn set_header_raw<V: sark::service::HeaderValue>(
                    headers: &mut Self::RawHeaders,
                    slot: Self::HeaderSlot,
                    value: &V,
                ) -> sark::error::Result<()> {
                    #header_set_fn
                }

                fn set_header_name_raw<V: sark::service::HeaderValue>(
                    headers: &mut Self::RawHeaders,
                    name: &[u8],
                    value: &V,
                ) -> sark::error::Result<()> {
                    #header_set_name_fn
                }

                fn set_header_u8<V: sark::service::HeaderValue>(
                    headers: &mut Self::RawHeaders,
                    slot: u8,
                    value: &V,
                ) -> sark::error::Result<()> {
                    #header_set_u8_fn
                }
            }
        };

        let query_methods = if query_fields.is_empty() {
            TokenStream::new()
        } else {
            quote! {
                fn set_query_name_raw<V: sark::service::HeaderValue>(
                    headers: &mut Self::RawHeaders,
                    name: &[u8],
                    value: &V,
                ) -> sark::error::Result<()> {
                    #query_set_name_fn
                }

                fn set_query_slice_raw(
                    headers: &mut Self::RawHeaders,
                    name: &[u8],
                    input: &[u8],
                    range: std::ops::Range<usize>,
                ) -> sark::error::Result<()> {
                    #query_set_slice_fn
                }

                fn parse_query_raw(
                    headers: &mut Self::RawHeaders,
                    input: &[u8],
                    range: std::ops::Range<usize>,
                ) -> sark::error::Result<()> {
                    #query_parse_fn
                }
            }
        };

        let parsed_body_param_ty = body_plan.parsed_body_param_ty();
        let body_bind = body_plan.parsed_body_bind();
        let from_parts_impl = quote! {
            impl<'req> #inner_name<'req> {
                #[allow(unused_variables, dead_code)]
                #vis fn from_parts(
                    params: #params_inner_ident<'req>,
                    headers: #headers_inner_ident<'req>,
                    parsed_body: #parsed_body_param_ty,
                    req: &sark::request::Ref<'req>,
                ) -> Self {
                    let _ = &params;
                    let _ = &headers;
                    let _ = req;
                    #body_bind
                    Self { #(#ctor_init_ref,)* }
                }
            }
        };

        Ok(quote! {
            #st
            #borrowed_st
            #header_tokens
            #params_tokens
            #from_parts_impl
            #header_slot_enum

            impl sark::service::RouteRequestImpl for #name {
                type HeaderSlot = #header_slot_ty;
                type RawHeaders = #raw_headers_ident;
                type RawParams = #raw_params_ident;
                type Params<'req> = #params_inner_ident<'req>;
                type Headers<'req> = #headers_inner_ident<'req>;
                #parsed_body_impl

                const NEED_HEADER: bool = #need_header;
                const NEED_KNOWN_HEADER: bool = #need_known_header;
                const NEED_QUERY: bool = #need_query;
                const BODY_POLICY: sark::service::BodyPolicy = #body_policy;

                #header_methods
                #query_methods

                fn build_headers<'req>(
                    req: &sark::request::Ref<'req>,
                    headers: Self::RawHeaders,
                ) -> sark::error::Result<Self::Headers<'req>> {
                    #build_headers
                }

            }
        })
    }
}
