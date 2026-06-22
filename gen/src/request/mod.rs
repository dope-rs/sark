mod hidden;
mod params;
mod query;
mod raw_body;

use hidden::Hidden;
use params::Params;
use proc_macro2::{Span, TokenStream};
use query::Query;
use quote::{format_ident, quote};
use raw_body::RawBody;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Fields, Ident, ItemStruct, Result, Token, Type};

use crate::codegen::header::{Emit, HeaderApplyMode, HeaderParserCfg, HeaderValueMode};
use crate::codegen::value::Value;
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
    pub(super) fn expand(self, mut st: ItemStruct) -> Result<TokenStream> {
        let ordered = self.ordered;
        let parser_cfg = HeaderParserCfg {
            value: self.value,
            apply: self.apply,
        };
        let value_mode_name = match self.value {
            HeaderValueMode::Full => "full",
            HeaderValueMode::Skip => "skip",
        };
        let apply_mode_name = match self.apply {
            HeaderApplyMode::Full => "full",
            HeaderApplyMode::Skip => "skip",
        };
        let name = st.ident.clone();
        let vis = st.vis.clone();
        let inner_name = format_ident!("{}Inner", name);
        let headers_ident = format_ident!("{}Headers", name);
        let raw_headers_ident = format_ident!("{}HeadersRaw", name);
        let header_slot_ident = format_ident!("{}HeaderSlot", name);
        let params_ident = format_ident!("{}Params", name);
        let raw_params_ident = format_ident!("{}ParamsRaw", name);
        let headers_inner_ident = format_ident!("{}HeadersInner", name);
        let params_inner_ident = format_ident!("{}ParamsInner", name);
        let mut body_ty: Option<Type> = None;
        let mut kept = Vec::with_capacity(st.attrs.len());
        for attr in st.attrs.drain(..) {
            if attr.path().is_ident("json_body") {
                if body_ty.is_some() {
                    return Err(Error::new_spanned(attr, "duplicate #[json_body(...)]"));
                }
                body_ty = Some(attr.parse_args::<Type>()?);
            } else {
                kept.push(attr);
            }
        }
        st.attrs = kept;
        let Fields::Named(named) = &mut st.fields else {
            return Err(Error::new(
                Span::call_site(),
                "#[sark_gen::request] requires a named-field struct",
            ));
        };

        let mut header_fields = Vec::<HeaderAttrField>::new();
        let mut query_fields = Vec::<QueryAttrField>::new();
        let mut path_fields = Vec::<PathAttrField>::new();
        let mut ctor_init = Vec::new();
        let mut ctor_init_ref = Vec::new();
        let mut raw_body_field = None::<(Ident, Type)>;
        let mut stream_body_field = None::<(Ident, Type)>;

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
            let is_stream_body = field
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("stream_body"));
            let kinds = [
                header.is_some(),
                query.is_some(),
                path.is_some(),
                is_raw_body,
                is_stream_body,
            ]
            .into_iter()
            .filter(|v| *v)
            .count();
            if kinds > 1 {
                return Err(Error::new_spanned(
                    field,
                    "request field must have exactly one of #[header(...)], #[query(...)], #[path(...)], #[raw_body], or #[stream_body]",
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
                ctor_init.push(quote!(#ident: headers.#ident));
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
                ctor_init.push(quote!(#ident: headers.#ident));
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
                ctor_init.push(quote!(#ident: params.#ident));
                ctor_init_ref.push(quote!(#ident: params.#ident));
            } else if is_raw_body {
                if raw_body_field.is_some() {
                    return Err(Error::new_spanned(field, "duplicate #[raw_body]"));
                }
                let raw_body_expr = RawBody::field_expr(&field.ty)?;
                raw_body_field = Some((ident.clone(), field.ty.clone()));
                ctor_init.push(quote!(#ident: #raw_body_expr));
                ctor_init_ref.push(quote!(#ident: #raw_body_expr));
            } else if is_stream_body {
                if stream_body_field.is_some() {
                    return Err(Error::new_spanned(field, "duplicate #[stream_body]"));
                }
                stream_body_field = Some((ident.clone(), field.ty.clone()));
                ctor_init
                    .push(quote!(#ident: ::sark::request::BodyLen::from_declared(raw_body.len())));
                ctor_init_ref.push(quote!(#ident: ::sark::request::BodyLen::from_declared(req.declared_body_len())));
            } else {
                return Err(Error::new_spanned(
                    field,
                    "#[sark_gen::request] fields require #[header(...)], #[query(...)], #[path(...)], #[raw_body], or #[stream_body]",
                ));
            }
            field.attrs.retain(|attr| {
                !attr.path().is_ident("header")
                    && !attr.path().is_ident("query")
                    && !attr.path().is_ident("path")
                    && !attr.path().is_ident("raw_body")
                    && !attr.path().is_ident("stream_body")
            });
        }

        let has_local_field = header_fields.iter().any(|f| f.ty.has_local_frame_bytes())
            || query_fields.iter().any(|f| f.ty.has_local_frame_bytes())
            || path_fields.iter().any(|f| f.ty.has_local_frame_bytes());
        if has_local_field {
            for field in &mut named.named {
                field.ty.rewrite_local_to_ref();
            }
        } else {
            named.named.push(syn::parse_quote! {
                #[doc(hidden)]
                pub __sark_req_marker: ::core::marker::PhantomData<&'req ()>
            });
            ctor_init.push(quote!(__sark_req_marker: ::core::marker::PhantomData));
            ctor_init_ref.push(quote!(__sark_req_marker: ::core::marker::PhantomData));
        }
        st.ident = inner_name.clone();
        if !st
            .generics
            .params
            .iter()
            .any(|p| matches!(p, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "req"))
        {
            st.generics.params.insert(0, syn::parse_quote!('req));
        }
        let alias_decl = quote! {
            #[allow(non_camel_case_types, dead_code)]
            #vis type #name = #inner_name<'static>;
        };

        let from_parts = if let Some(body_ty) = &body_ty {
            named.named.push(syn::parse_quote!(#vis body: #body_ty));
            ctor_init.push(quote!(body));
            ctor_init_ref.push(quote!(body));
            quote! {
                #vis fn from_parts(
                    params: #params_ident,
                    headers: #headers_ident,
                    body: #body_ty,
                    raw_body: sark::request::Body,
                ) -> Self {
                    let _ = &params;
                    let _ = &headers;
                    let _ = &raw_body;
                    Self { #(#ctor_init,)* }
                }
            }
        } else {
            quote! {
                #vis fn from_parts(
                    params: #params_ident,
                    headers: #headers_ident,
                    raw_body: sark::request::Body,
                ) -> Self {
                    let _ = &params;
                    let _ = &headers;
                    let _ = &raw_body;
                    Self { #(#ctor_init,)* }
                }
            }
        };

        let header_tokens = if header_fields.is_empty() && query_fields.is_empty() {
            quote! {
                type #headers_ident = sark::service::NoHeaders;
                type #raw_headers_ident = sark::service::NoHeaders;
                #[allow(non_camel_case_types, unused_lifetimes, dead_code)]
                type #headers_inner_ident<'req> = sark::service::NoHeaders;
            }
        } else {
            Hidden {
                name: &headers_ident,
                inner_name: &headers_inner_ident,
                raw_name: &raw_headers_ident,
                headers: &header_fields,
                queries: &query_fields,
                ordered_query_state: ordered && !query_fields.is_empty(),
            }
            .build()?
        };

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
                quote! { () },
                quote! { type #header_slot_ident = (); },
                quote! { None },
                quote! { Ok(()) },
                quote! { Ok(()) },
                quote! { None },
                quote! { Ok(()) },
            )
        } else {
            let (slot_enum, slot_probe_fn, set_fn, set_name_fn, slot_probe_u8_fn, set_u8_fn) =
                Value::build_slots(
                    &header_slot_ident,
                    header_fields
                        .iter()
                        .map(|f| (&f.ident, f.header.value().into_bytes(), &f.ty)),
                    true,
                )?;
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
        let header_scan_fn = Emit::scan()?;
        let header_contig_fn = Emit::contig(
            header_fields
                .iter()
                .map(|f| (&f.ident, f.header.value().into_bytes(), &f.ty)),
            parser_cfg,
        )?;
        let header_apply_fn = Emit::apply(
            header_fields
                .iter()
                .map(|f| (&f.ident, f.header.value().into_bytes(), &f.ty)),
            parser_cfg,
        )?;
        let query = Query::new(&query_fields);
        let query_set_name_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            query.set_name_direct()?
        } else {
            Value::build_set_name(
                query_fields
                    .iter()
                    .map(|f| (&f.ident, f.query.value().into_bytes(), &f.ty)),
                false,
            )?
        };
        let query_parse_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            Query::parse_direct()?
        } else {
            Value::build_parse_query(
                query_fields
                    .iter()
                    .map(|f| (&f.ident, f.query.value().into_bytes(), &f.ty)),
            )?
        };
        let query_set_slice_fn = if query_fields.is_empty() {
            quote! { Ok(()) }
        } else if ordered {
            query.set_slice_direct()?
        } else {
            Value::build_set_slice(
                query_fields
                    .iter()
                    .map(|f| (&f.ident, f.query.value().into_bytes(), &f.ty)),
            )?
        };
        let need_header = !(header_fields.is_empty() && query_fields.is_empty());
        let need_known_header = header_fields.iter().any(|f| {
            let name = f.header.value();
            name.eq_ignore_ascii_case("host")
                || name.eq_ignore_ascii_case("connection")
                || name.eq_ignore_ascii_case("content-length")
                || name.eq_ignore_ascii_case("transfer-encoding")
                || name.eq_ignore_ascii_case("expect")
        });
        let need_query = !query_fields.is_empty();

        let build_headers = if header_fields.is_empty() && query_fields.is_empty() {
            quote!(Ok(sark::service::NoHeaders))
        } else {
            let raw_field_ident: Vec<&Ident> = header_fields
                .iter()
                .map(|f| &f.ident)
                .chain(query_fields.iter().map(|f| &f.ident))
                .collect();
            let typed_field_expr = Hidden::header_query_exprs(&header_fields, &query_fields)?;
            quote! {
                let #raw_headers_ident { #( #raw_field_ident, )* .. } = headers;
                Ok(#headers_ident {
                    #( #raw_field_ident: #typed_field_expr, )*
                    __sark_m: ::core::marker::PhantomData,
                })
            }
        };
        let build_headers_ref = if header_fields.is_empty() && query_fields.is_empty() {
            quote!(Ok(sark::service::NoHeaders))
        } else {
            quote!(#headers_inner_ident::<'req>::from_raw_ref(req, headers))
        };
        let params_tokens = Params {
            vis: &vis,
            ident: &params_ident,
            inner_ident: &params_inner_ident,
            raw_ident: &raw_params_ident,
            fields: &path_fields,
        }
        .build()?;
        let need_path = !path_fields.is_empty();
        let streaming_body = stream_body_field.is_some();

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

        let parsed_body_param_ty = match &body_ty {
            Some(ty) => quote!(#ty),
            None => quote!(()),
        };
        let body_bind = if body_ty.is_some() {
            quote!(let body = parsed_body;)
        } else {
            TokenStream::new()
        };
        let raw_body_bind = if raw_body_field.is_some() {
            quote!(let raw_body = req.body_owned();)
        } else {
            TokenStream::new()
        };
        let from_parts_ref_impl = quote! {
            impl<'req> #inner_name<'req> {
                #[allow(unused_variables, dead_code)]
                #vis fn from_parts_ref(
                    params: #params_inner_ident<'req>,
                    headers: #headers_inner_ident<'req>,
                    parsed_body: #parsed_body_param_ty,
                    req: &sark::request::Ref<'req, #headers_inner_ident<'req>>,
                ) -> Self {
                    let _ = &params;
                    let _ = &headers;
                    let _ = req;
                    #body_bind
                    #raw_body_bind
                    Self { #(#ctor_init_ref,)* }
                }
            }
        };

        Ok(quote! {
            #st
            #alias_decl
            #header_tokens
            #params_tokens
            #from_parts_ref_impl
            #header_slot_enum

            impl #name {
                #vis const NEED_PATH: bool = #need_path;
                #vis const VALUE_PARSER: &'static str = #value_mode_name;
                #vis const APPLY_PARSER: &'static str = #apply_mode_name;
                #vis const STREAMING_BODY: bool = #streaming_body;

                #from_parts
            }

            impl sark::service::RouteRequestImpl for #name {
                type HeaderSlot = #header_slot_ty;
                type RawHeaders = #raw_headers_ident;
                type RawParams = #raw_params_ident;
                type ParamsInner<'req> = #params_inner_ident<'req>;
                type HeadersInner<'req> = #headers_inner_ident<'req>;

                const NEED_HEADER: bool = #need_header;
                const NEED_KNOWN_HEADER: bool = #need_known_header;
                const NEED_QUERY: bool = #need_query;

                #header_methods
                #query_methods

                fn build_headers(
                    req: &sark::Request,
                    headers: Self::RawHeaders,
                ) -> sark::error::Result<Self::HeadersInner<'static>> {
                    #build_headers
                }

                fn build_headers_ref<'req>(
                    req: &sark::request::Ref<'req, ()>,
                    headers: Self::RawHeaders,
                ) -> sark::error::Result<Self::HeadersInner<'req>> {
                    #build_headers_ref
                }

            }
        })
    }
}
