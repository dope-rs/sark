#![allow(clippy::too_many_arguments)]

use proc_macro2::{Ident, TokenStream};
use quote::quote;

pub(crate) struct Cfg<'a> {
    pub(crate) name: &'a Ident,

    pub(crate) request_ident: &'a Ident,

    pub(crate) params_ident: &'a Ident,
    pub(crate) raw_params_ident: &'a Ident,
    pub(crate) headers_ident: &'a Ident,
    pub(crate) raw_headers_ident: &'a Ident,
    pub(crate) params_has_lifetime: bool,
    pub(crate) headers_has_lifetime: bool,
    pub(crate) params_inner_ident: Option<&'a Ident>,
    pub(crate) headers_inner_ident: Option<&'a Ident>,

    pub(crate) header_slot_ty: &'a TokenStream,

    pub(crate) static_response: bool,
    pub(crate) response_body_kind: crate::model::ResponseKind,
    pub(crate) request_body_kind: crate::model::RequestKind,
    pub(crate) response_ty_override: Option<&'a TokenStream>,
    pub(crate) parsed_body_ty: Option<&'a TokenStream>,
    pub(crate) parse_body_body: Option<&'a TokenStream>,
    pub(crate) max_body: Option<&'a syn::Expr>,
    pub(crate) head_skip: crate::model::HeadSkip,
}

impl Cfg<'_> {
    pub(crate) fn build(&self) -> TokenStream {
        let Cfg {
            name,
            request_ident,
            raw_params_ident,
            raw_headers_ident,
            params_has_lifetime,
            headers_has_lifetime,
            params_inner_ident,
            headers_inner_ident,
            header_slot_ty,
            static_response,
            response_body_kind,
            request_body_kind,
            response_ty_override,
            parsed_body_ty,
            parse_body_body,
            max_body,
            ..
        } = *self;
        let response_body_kind_tokens = match response_body_kind {
            crate::model::ResponseKind::Inline => {
                quote!(sark::http::body_kind::ResponseKind::Inline)
            }
            crate::model::ResponseKind::Static => {
                quote!(sark::http::body_kind::ResponseKind::Static)
            }
            crate::model::ResponseKind::Stream => {
                quote!(sark::http::body_kind::ResponseKind::Stream)
            }
        };
        let request_body_kind_tokens = match request_body_kind {
            crate::model::RequestKind::Inline => {
                quote!(sark::http::body_kind::RequestKind::Inline)
            }
            crate::model::RequestKind::Spilled => {
                quote!(sark::http::body_kind::RequestKind::Spilled)
            }
        };
        let params_ident = self.params_ident;
        let headers_ident = self.headers_ident;
        let params_gat = match (params_has_lifetime, params_inner_ident) {
            (true, Some(inner)) => quote! { type Params<'__req> = #inner<'__req>; },
            (true, None) => quote! { type Params<'__req> = #params_ident<'__req>; },
            (false, _) => quote! { type Params<'__req> = #params_ident; },
        };
        let headers_gat = match (headers_has_lifetime, headers_inner_ident) {
            (true, Some(inner)) => quote! { type Headers<'__req> = #inner<'__req>; },
            (true, None) => quote! { type Headers<'__req> = #headers_ident<'__req>; },
            (false, _) => quote! { type Headers<'__req> = #headers_ident; },
        };

        let default_response_ty = quote!(sark_core::http::ServeInner<'__req>);
        let response_ty = response_ty_override.unwrap_or(&default_response_ty);
        let default_parsed_body_ty = quote!(());
        let parsed_body_ty_token = parsed_body_ty.unwrap_or(&default_parsed_body_ty);
        let default_parse_body_body = quote!({
            let _ = raw;
            Ok(())
        });
        let parse_body_body_token = parse_body_body.unwrap_or(&default_parse_body_body);
        let max_body_token = match max_body {
            Some(expr) => quote! { const MAX_BODY: usize = #expr; },
            None => quote! {},
        };
        let emit_date_token = if self.head_skip.date {
            quote! { const EMIT_DATE: bool = false; }
        } else {
            quote! {}
        };
        let emit_server_token = if self.head_skip.server {
            quote! { const EMIT_SERVER: bool = false; }
        } else {
            quote! {}
        };

        quote! {
            impl sark::service::RouteSpec for #name {
                type Request = #request_ident;
                #params_gat
                type RawParams = #raw_params_ident;
                #headers_gat
                type RawHeaders = #raw_headers_ident;
                type HeaderSlot = #header_slot_ty;
                type Response<'__req> = #response_ty;
                type ParsedBody<'__req> = #parsed_body_ty_token;

                fn parse_body<'__req>(
                    raw: &'__req [u8],
                ) -> sark::error::Result<Self::ParsedBody<'__req>> {
                    #parse_body_body_token
                }
                const STATIC_RESPONSE: bool = #static_response;
                const RESPONSE_BODY_KIND: sark::http::body_kind::ResponseKind = #response_body_kind_tokens;
                const REQUEST_BODY_KIND: sark::http::body_kind::RequestKind = #request_body_kind_tokens;
                const STREAMING_BODY: bool = <#request_ident>::STREAMING_BODY;
                #max_body_token
                #emit_date_token
                #emit_server_token

                type Captures =
                    <#raw_params_ident as sark::service::RouteParams>::Captures;

                fn from_captures<P: sark::service::PathProbe>(
                    path: &P,
                    captures: Self::Captures,
                ) -> Option<Self::RawParams> {
                    <#raw_params_ident as sark::service::RouteParams>::from_captures(
                        path, captures,
                    )
                }
            }
        }
    }
}
