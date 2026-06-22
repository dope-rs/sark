mod parts;

use parts::build_arms;
use proc_macro2::{Ident, TokenStream};
use quote::format_ident;
use syn::{Index, LitStr, Type, TypePath, Visibility};

use super::plan::Meta;
use crate::model::{AppDispatchInput, AppRouteInput};
use crate::route_compiler::Method;

pub(super) struct Entry {
    pub(super) route: TypePath,
    pub(super) path: LitStr,
    pub(super) meta: Meta,
    pub(super) wraps: Vec<TypePath>,
    pub(super) kind: crate::model::RouteKind,
    pub(super) capacity: Option<syn::LitInt>,
}

pub(super) struct Gen {
    pub(super) vis: Visibility,
    pub(super) name: Ident,
    pub(super) state_ty: Type,
    pub(super) route_specs: Vec<Entry>,
    pub(super) routes: Vec<TypePath>,
    pub(super) idx: Vec<Index>,
    pub(super) plan_ident: Ident,
    pub(super) key_ident: Ident,
    pub(super) parts_ident: Ident,
    pub(super) key_vars: Vec<Ident>,
    pub(super) parts_aliases: Vec<TokenStream>,
    pub(super) route_bounds: Vec<TokenStream>,
    pub(super) parts_vars: Vec<TokenStream>,
    pub(super) parts_header_bytes_arms: Vec<TokenStream>,
    pub(super) parts_query_name_arms: Vec<TokenStream>,
    pub(super) parts_query_slice_arms: Vec<TokenStream>,
    pub(super) parts_query_parse_arms: Vec<TokenStream>,
}

impl Gen {
    pub(super) fn new(input: AppDispatchInput) -> syn::Result<Self> {
        let vis = input.vis;
        let name = input.name;
        let state_ty = input.state_ty;
        let route_specs: Vec<Entry> = input
            .routes
            .into_iter()
            .map(build_route_entry)
            .collect::<syn::Result<_>>()?;
        let routes: Vec<TypePath> = route_specs
            .iter()
            .map(|entry| entry.route.clone())
            .collect();
        let idx: Vec<Index> = (0..routes.len()).map(Index::from).collect();
        let plan_ident = format_ident!("{}HeadPlan", name);
        let key_ident = format_ident!("{}RouteKey", name);
        let parts_ident = format_ident!("{}ParseParts", name);
        let key_vars: Vec<_> = (0..routes.len()).map(|i| format_ident!("R{}", i)).collect();
        let params_alias: Vec<_> = (0..routes.len())
            .map(|i| format_ident!("{}ParamsTy{}", name, i))
            .collect();
        let headers_alias: Vec<_> = (0..routes.len())
            .map(|i| format_ident!("{}HeadersTy{}", name, i))
            .collect();
        let kinds: Vec<crate::model::RouteKind> =
            route_specs.iter().map(|entry| entry.kind).collect();
        let arms = build_arms(
            &routes,
            &kinds,
            &state_ty,
            &params_alias,
            &headers_alias,
            &key_vars,
            &parts_ident,
        );

        Ok(Self {
            vis,
            name,
            state_ty,
            route_specs,
            routes,
            idx,
            plan_ident,
            key_ident,
            parts_ident,
            key_vars,
            parts_aliases: arms.parts_aliases,
            route_bounds: arms.route_bounds,
            parts_vars: arms.parts_vars,
            parts_header_bytes_arms: arms.parts_header_bytes_arms,
            parts_query_name_arms: arms.parts_query_name_arms,
            parts_query_slice_arms: arms.parts_query_slice_arms,
            parts_query_parse_arms: arms.parts_query_parse_arms,
        })
    }
}

fn build_route_entry(entry: AppRouteInput) -> syn::Result<Entry> {
    let path = entry.path;
    let method = Method::parse(&entry.method.to_string()).ok_or_else(|| {
        syn::Error::new_spanned(
            &entry.method,
            "unsupported method; use one of GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS",
        )
    })?;
    let meta = Meta { method };
    Ok(Entry {
        route: entry.route,
        path,
        meta,
        wraps: entry.wraps,
        kind: entry.kind,
        capacity: entry.capacity,
    })
}
