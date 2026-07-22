mod parts;

use parts::build_arms;
use proc_macro2::{Ident, TokenStream};
use quote::format_ident;
use syn::{GenericArgument, Index, LitStr, PathArguments, Type, TypePath, Visibility};

use super::plan::Meta;
use crate::model::{AppDispatchInput, AppRouteInput};
use crate::route_compiler::Method;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteKind {
    Sync,
    Fiber,
    Stream,
}

pub(super) struct Entry {
    pub(super) route: TypePath,
    pub(super) path: LitStr,
    pub(super) meta: Meta,
    pub(super) wraps: Vec<TypePath>,
    pub(super) kind: RouteKind,
    pub(super) capacity: Option<syn::Expr>,
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
        let state_ty = normalize_state_lifetimes(input.state_ty);
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
        let kinds: Vec<RouteKind> = route_specs.iter().map(|entry| entry.kind).collect();
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

fn normalize_state_lifetimes(mut ty: Type) -> Type {
    struct Rewrite;
    impl syn::visit_mut::VisitMut for Rewrite {
        fn visit_lifetime_mut(&mut self, lt: &mut syn::Lifetime) {
            if lt.ident != "static" {
                lt.ident = format_ident!("d");
            }
        }
    }
    syn::visit_mut::VisitMut::visit_type_mut(&mut Rewrite, &mut ty);
    ty
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
    let (route, kind, capacity) = unpack_route(entry.route)?;
    Ok(Entry {
        route,
        path,
        meta,
        wraps: entry.wraps,
        kind,
        capacity,
    })
}

fn unpack_route(route: TypePath) -> syn::Result<(TypePath, RouteKind, Option<syn::Expr>)> {
    let Some(segment) = route.path.segments.last() else {
        return Ok((route, RouteKind::Sync, None));
    };
    let kind = match segment.ident.to_string().as_str() {
        "__SarkAsyncRoute" => RouteKind::Fiber,
        "__SarkStreamRoute" => RouteKind::Stream,
        _ => return Ok((route, RouteKind::Sync, None)),
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            route,
            "invalid route storage metadata",
        ));
    };
    let mut arguments = arguments.args.iter();
    let Some(GenericArgument::Type(Type::Path(route_type))) = arguments.next() else {
        return Err(syn::Error::new_spanned(
            route,
            "invalid route type metadata",
        ));
    };
    let Some(GenericArgument::Const(capacity)) = arguments.next() else {
        return Err(syn::Error::new_spanned(
            route,
            "invalid route capacity metadata",
        ));
    };
    if arguments.next().is_some() {
        return Err(syn::Error::new_spanned(
            route,
            "invalid route storage metadata",
        ));
    }
    let capacity = match capacity {
        syn::Expr::Block(block) if block.block.stmts.len() == 1 => {
            let syn::Stmt::Expr(capacity, None) = &block.block.stmts[0] else {
                return Err(syn::Error::new_spanned(block, "invalid route capacity"));
            };
            capacity.clone()
        }
        capacity => capacity.clone(),
    };
    Ok((route_type.clone(), kind, Some(capacity)))
}
