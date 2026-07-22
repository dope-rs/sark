#![allow(clippy::too_many_arguments)]

use proc_macro2::{Ident, TokenStream};
use quote::quote;

pub(crate) struct Config<'a> {
    pub(crate) name: &'a Ident,

    pub(crate) request_ident: &'a Ident,

    pub(crate) raw_params_ident: &'a Ident,
    pub(crate) raw_headers_ident: &'a Ident,
    pub(crate) params_inner_ident: &'a Ident,
    pub(crate) headers_inner_ident: &'a Ident,

    pub(crate) header_slot_ty: &'a TokenStream,

    pub(crate) static_response: bool,
    pub(crate) kind_ty: &'a TokenStream,
    pub(crate) native_response_ty: Option<&'a syn::Type>,
    pub(crate) native_response_ty_static: Option<&'a syn::Type>,
    pub(crate) async_response_ty: Option<&'a syn::Type>,
    pub(crate) parsed_body_ty: Option<&'a TokenStream>,
    pub(crate) parse_body_body: Option<&'a TokenStream>,
    pub(crate) max_body: Option<&'a syn::Expr>,
    pub(crate) head_skip: crate::model::HeadSkip,
}

impl Config<'_> {
    pub(crate) fn build(&self) -> TokenStream {
        let Config {
            name,
            request_ident,
            raw_params_ident,
            raw_headers_ident,
            params_inner_ident,
            headers_inner_ident,
            header_slot_ty,
            static_response,
            kind_ty,
            native_response_ty,
            native_response_ty_static,
            async_response_ty,
            parsed_body_ty,
            parse_body_body,
            max_body,
            ..
        } = *self;

        let default_response_ty = quote!(sark_core::http::Serve<'__req>);
        let native_response_shape = native_response_ty.map(|ty| {
            quote! {
                <#ty as sark::service::manifold::NativeResponse<'__req>>::Shape
            }
        });
        let response_ty = native_response_shape
            .as_ref()
            .unwrap_or(&default_response_ty);
        let default_async_response_ty: syn::Type = syn::parse_quote!(sark_core::http::Response);
        let async_response_ty = async_response_ty.unwrap_or(&default_async_response_ty);
        let response_body_kind_tokens = if let Some(ty) = native_response_ty_static {
            quote! {
                <#ty as sark::service::manifold::NativeResponse<'static>>::BODY_KIND
            }
        } else {
            quote! {
                <#async_response_ty as sark_core::http::OwnedShape>::BODY_KIND
            }
        };
        let stream_ty = native_response_ty_static
            .map(|ty| {
                quote! {
                    <#ty as sark::service::manifold::NativeResponse<'static>>::Stream
                }
            })
            .unwrap_or_else(|| quote!(sark_core::http::NeverStream));
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
                type Kind = #kind_ty;
                type Request = #request_ident;
                type Params<'__req> = #params_inner_ident<'__req>;
                type RawParams = #raw_params_ident;
                type Headers<'__req> = #headers_inner_ident<'__req>;
                type RawHeaders = #raw_headers_ident;
                type HeaderSlot = #header_slot_ty;
                type Response<'__req> = #response_ty;
                type AsyncResponse = #async_response_ty;
                type Stream = #stream_ty;
                type ParsedBody<'__req> = #parsed_body_ty_token;

                fn parse_body<'__req>(
                    raw: &'__req [u8],
                ) -> sark::error::Result<Self::ParsedBody<'__req>> {
                    #parse_body_body_token
                }
                const STATIC_RESPONSE: bool = #static_response;
                const RESPONSE_BODY_KIND: sark::http::body_kind::ResponseKind = #response_body_kind_tokens;
                #max_body_token
                #emit_date_token
                #emit_server_token

                type Captures =
                    <#raw_params_ident as sark::service::RawRouteParams>::Captures;

                fn from_captures<P: sark::service::PathProbe>(
                    path: &P,
                    captures: Self::Captures,
                ) -> Option<Self::RawParams> {
                    <#raw_params_ident as sark::service::RawRouteParams>::from_captures(
                        path, captures,
                    )
                }
            }
        }
    }
}
