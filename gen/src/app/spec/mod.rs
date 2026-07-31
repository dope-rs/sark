use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::{Error, GenericArgument, Index, LitStr, PathArguments, Type, TypePath, Visibility};

use super::plan::Meta;
use crate::{
    model::{AppDispatchInput, AppRouteInput},
    route_compiler::Method,
};

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
    pub(super) route_bounds: Vec<TokenStream>,
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
        let route_bounds = build_route_bounds(&route_specs, &state_ty);

        Ok(Self {
            vis,
            name,
            state_ty,
            route_specs,
            routes,
            idx,
            route_bounds,
        })
    }
}

fn build_route_bounds(entries: &[Entry], state_ty: &Type) -> Vec<TokenStream> {
    entries
        .iter()
        .map(|entry| {
            let route = &entry.route;
            let (kind, invoke) = match entry.kind {
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
                    quote!(sark::service::manifold::Route<#state_ty>),
                ),
            };
            quote! {
                #route: sark::service::RouteSpec<Kind = #kind> + #invoke,
            }
        })
        .collect()
}

fn normalize_state_lifetimes(mut ty: Type) -> Type {
    use syn::visit_mut::VisitMut;
    struct Rewrite;
    impl VisitMut for Rewrite {
        fn visit_lifetime_mut(&mut self, lt: &mut syn::Lifetime) {
            if lt.ident != "static" {
                lt.ident = format_ident!("d");
            }
        }
    }
    VisitMut::visit_type_mut(&mut Rewrite, &mut ty);
    ty
}

fn build_route_entry(entry: AppRouteInput) -> syn::Result<Entry> {
    let path = entry.path;
    let method = Method::parse(&entry.method.to_string()).ok_or_else(|| {
        Error::new_spanned(
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
    use syn::Expr;
    let Some(segment) = route.path.segments.last() else {
        return Ok((route, RouteKind::Sync, None));
    };
    let kind = match segment.ident.to_string().as_str() {
        "__SarkAsyncRoute" => RouteKind::Fiber,
        "__SarkStreamRoute" => RouteKind::Stream,
        _ => return Ok((route, RouteKind::Sync, None)),
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(route, "invalid route storage metadata"));
    };
    let mut arguments = arguments.args.iter();
    let Some(GenericArgument::Type(Type::Path(route_type))) = arguments.next() else {
        return Err(Error::new_spanned(route, "invalid route type metadata"));
    };
    let Some(GenericArgument::Const(capacity)) = arguments.next() else {
        return Err(Error::new_spanned(route, "invalid route capacity metadata"));
    };
    if arguments.next().is_some() {
        return Err(Error::new_spanned(route, "invalid route storage metadata"));
    }
    let capacity = match capacity {
        Expr::Block(block) if block.block.stmts.len() == 1 => {
            use syn::Stmt;
            let Stmt::Expr(capacity, None) = &block.block.stmts[0] else {
                return Err(Error::new_spanned(block, "invalid route capacity"));
            };
            capacity.clone()
        }
        capacity => capacity.clone(),
    };
    Ok((route_type.clone(), kind, Some(capacity)))
}
