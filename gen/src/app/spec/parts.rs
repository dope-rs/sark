use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{Type, TypePath};

use super::RouteKind;

pub(super) struct ArmsData {
    pub(super) parts_aliases: Vec<TokenStream>,
    pub(super) route_bounds: Vec<TokenStream>,
    pub(super) parts_vars: Vec<TokenStream>,
    pub(super) parts_header_bytes_arms: Vec<TokenStream>,
    pub(super) parts_query_name_arms: Vec<TokenStream>,
    pub(super) parts_query_slice_arms: Vec<TokenStream>,
    pub(super) parts_query_parse_arms: Vec<TokenStream>,
}

pub(super) fn build_arms(
    routes: &[TypePath],
    kinds: &[RouteKind],
    state_ty: &Type,
    params_alias: &[Ident],
    headers_alias: &[Ident],
    key_vars: &[Ident],
    parts_ident: &Ident,
) -> ArmsData {
    let parts_aliases: Vec<TokenStream> = routes
        .iter()
        .zip(params_alias.iter())
        .zip(headers_alias.iter())
        .map(|((route, p), h)| {
            quote! {
                type #p = <#route as sark::service::RouteSpec>::RawParams;
                type #h = <#route as sark::service::RouteSpec>::RawHeaders;
            }
        })
        .collect();

    let route_bounds: Vec<TokenStream> = routes
        .iter()
        .zip(kinds.iter())
        .map(|(route, kind)| {
            let (kind, invoke) = match kind {
                RouteKind::Sync => (
                    quote!(sark::service::manifold::Sync),
                    quote!(sark::service::manifold::Route<#state_ty>),
                ),
                RouteKind::Fiber => (
                    quote!(sark::service::manifold::NativeFiber),
                    quote!(sark::service::manifold::TaskRoute<'d, #state_ty>),
                ),
                RouteKind::Stream => (
                    quote!(sark::service::manifold::NativeStream),
                    quote! {
                        sark::service::manifold::Route<#state_ty>
                            + sark::service::manifold::TaskRoute<'d, #state_ty>
                    },
                ),
            };
            quote! {
                #route: sark::service::RouteSpec<Kind = #kind>
                    + #invoke,
            }
        })
        .collect();

    let parts_vars: Vec<TokenStream> = routes
        .iter()
        .zip(params_alias.iter())
        .zip(headers_alias.iter())
        .zip(key_vars.iter())
        .map(|(((_route, p), h), key)| {
            quote! {
                #key {
                    params: #p,
                    headers: #h,
                },
            }
        })
        .collect();

    let parts_header_bytes_arms: Vec<TokenStream> = routes
        .iter()
        .zip(key_vars.iter())
        .map(|(route, key)| {
            quote! {
                #parts_ident::#key { params: _, headers } => {
                    if let Some(slot) = <<#route as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::header_slot_bytes(name) {
                        <<#route as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::set_header_raw(headers, slot, value)?;
                    }
                }
            }
        })
        .collect();

    let parts_query_name_arms: Vec<TokenStream> = routes
        .iter()
        .zip(key_vars.iter())
        .map(|(route, key)| {
            quote! {
                #parts_ident::#key { params: _, headers } => <<#route as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::set_query_name_raw(headers, name, value)?,
            }
        })
        .collect();

    let parts_query_slice_arms: Vec<TokenStream> = routes
        .iter()
        .zip(key_vars.iter())
        .map(|(route, key)| {
            quote! {
                #parts_ident::#key { params: _, headers } => <<#route as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::set_query_slice_raw(headers, name, input, range)?,
            }
        })
        .collect();

    let parts_query_parse_arms: Vec<TokenStream> = routes
        .iter()
        .zip(key_vars.iter())
        .map(|(route, key)| {
            quote! {
                #parts_ident::#key { params: _, headers } => <<#route as sark::service::RouteSpec>::Request as sark::service::RouteRequestImpl>::parse_query_raw(headers, input, range)?,
            }
        })
        .collect();

    ArmsData {
        parts_aliases,
        route_bounds,
        parts_vars,
        parts_header_bytes_arms,
        parts_query_name_arms,
        parts_query_slice_arms,
        parts_query_parse_arms,
    }
}
