#![allow(clippy::too_many_arguments)]

use proc_macro2::{Ident, TokenStream};
use quote::{ToTokens, format_ident, quote};

pub(crate) struct Runtime;

impl Runtime {
    pub(crate) fn build<StateTy: ToTokens>(
        name: &Ident,
        state_ty: &StateTy,
        params_ident: &Ident,
        headers_ident: &Ident,
        call_with_parts_try_expr: Option<&TokenStream>,
        borrowed_call_cfg: Option<(&Ident, &Ident)>,
        handler_sync: bool,
        is_stream: bool,
    ) -> TokenStream {
        if !handler_sync {
            return TokenStream::new();
        }
        let state_has_lifetime = quote!(#state_ty).to_string().contains("'state");
        let state_lt_use: TokenStream = if state_has_lifetime {
            quote!(<'state>)
        } else {
            TokenStream::new()
        };
        let state_outlives: TokenStream = if state_has_lifetime {
            quote!('state: 'a,)
        } else {
            TokenStream::new()
        };
        let borrowed_impl = if let Some((request_inner_ident, hidden_fn_ref)) = borrowed_call_cfg {
            let hidden_fn_sync = format_ident!("{}_sync", hidden_fn_ref);
            quote! {
                impl #state_lt_use sark::service::manifold::Route<#state_ty> for #name {
                    fn invoke<'req, 'a>(
                        &'a self,
                        params: <Self as sark::service::RouteSpec>::Params<'req>,
                        req: &sark::request::Ref<'req, <Self as sark::service::RouteSpec>::Headers<'req>>,
                        headers: <Self as sark::service::RouteSpec>::Headers<'req>,
                        parsed_body: <Self as sark::service::RouteSpec>::ParsedBody<'req>,
                        state: &'a #state_ty,
                    ) -> <Self as sark::service::RouteSpec>::Response<'req>
                    where
                        'req: 'a,
                        #state_outlives
                    {
                        let request = #request_inner_ident::<'req>::from_parts_ref(
                            params,
                            headers,
                            parsed_body,
                            req,
                        );
                        #hidden_fn_sync(request, state)
                    }
                }
            }
        } else {
            TokenStream::new()
        };
        let stream_impl = if is_stream && let Some(expr) = call_with_parts_try_expr {
            quote! {
                impl sark::service::manifold::StreamRoute<#state_ty> for #name {
                    fn invoke(
                        &self,
                        params: #params_ident,
                        req: sark::Request,
                        headers: #headers_ident,
                        parsed_body: <Self as sark::service::RouteSpec>::ParsedBody<'static>,
                        state: &#state_ty,
                    ) -> <Self as sark::service::RouteSpec>::Response<'static> {
                        #expr
                    }
                }
            }
        } else {
            TokenStream::new()
        };
        quote! {
            #borrowed_impl
            #stream_impl
        }
    }
}
