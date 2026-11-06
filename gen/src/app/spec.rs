use std::{iter, ops};

use syn::{ext::IdentExt, visit::Visit};

use crate::{
    app::{inputs, route},
    handler::names,
    runtime::paths::Paths,
    types,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Sync,
    Fiber,
    Stream,
    Duplex,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Homogeneity {
    Uniform(Kind),
    Mixed,
}

impl Homogeneity {
    fn include(&mut self, kind: Kind) {
        if let Self::Uniform(current) = self
            && *current != kind
        {
            *self = Self::Mixed;
        }
    }
}

pub(super) enum Mode {
    Sync,
    Fiber {
        capacity: syn::Expr,
        body: inputs::Body,
        stream: bool,
    },
    Stream {
        capacity: syn::Expr,
    },
    Duplex,
}

pub(super) struct Entry {
    pub(super) route: syn::TypePath,
    pub(super) path: syn::LitStr,
    pub(super) method: route::Method,
    pub(super) upgrade: bool,
    pub(super) protocol: Option<syn::LitStr>,
    pub(super) guards: Vec<syn::TypePath>,
    pub(super) request: inputs::Request,
    pub(super) mode: Mode,
}

pub(in crate::app) struct Entries {
    first: Entry,
    rest: Vec<Entry>,
    pub(in crate::app) homogeneity: Homogeneity,
}

impl ops::Index<usize> for Entries {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        match index {
            0 => &self.first,
            index => &self.rest[index - 1],
        }
    }
}

impl Entries {
    pub(in crate::app) fn iter(&self) -> impl Iterator<Item = &Entry> {
        iter::once(&self.first).chain(self.rest.iter())
    }

    pub(in crate::app) const fn len(&self) -> usize {
        self.rest.len() + 1
    }

    pub(in crate::app) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Entry> {
        iter::once(&mut self.first).chain(self.rest.iter_mut())
    }
}

pub(super) enum State {
    Bound(syn::Type),
    Empty(syn::Type),
}

#[derive(Clone, Copy)]
pub(super) enum Driver {
    Absent,
    Present,
}

impl State {
    pub(super) const fn ty(&self) -> &syn::Type {
        match self {
            Self::Bound(ty) | Self::Empty(ty) => ty,
        }
    }
}

pub(super) struct Spec {
    pub(super) vis: syn::Visibility,
    pub(super) name: proc_macro2::Ident,
    pub(super) state: State,
    pub(super) driver: Driver,
    pub(super) state_generics: types::Generics,
    pub(super) route_specs: Entries,
    pub(super) route_bounds: Vec<proc_macro2::TokenStream>,
    pub(super) route_plan: route::Plan,
}

struct Route {
    route: syn::TypePath,
    method: proc_macro2::Ident,
    protocol: Option<syn::LitStr>,
    path: syn::LitStr,
    guards: Vec<syn::TypePath>,
    request: inputs::Request,
    kind: inputs::Kind,
}

#[derive(Clone, Copy)]
pub(super) enum StateScope {
    Driver,
    Router,
}

impl Spec {
    pub(super) fn bounds(&mut self, paths: &Paths) {
        self.route_bounds = self
            .route_specs
            .iter()
            .map(|entry| build_route_bound(paths, &self.state_generics, entry, self.state.ty()))
            .collect();
    }

    pub(super) fn new(paths: &Paths, input: inputs::Input, scope: StateScope) -> syn::Result<Self> {
        let ns_core = &paths.core;
        let inputs::Input {
            vis,
            name,
            mut generics,
            state_ty,
            entries,
        } = input;
        if state_ty.is_none() && !generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &generics,
                "state parameters require an application state type",
            ));
        }
        let bound = types::Generics::bind_driver(&mut generics);
        let (state, mut driver) = match state_ty {
            Some(state_ty) => {
                let driver = driver(&state_ty, scope)?;
                (State::Bound(normalize_state(state_ty, scope)), driver)
            }
            None => (
                State::Empty(normalize_state(
                    syn::parse_quote!(#ns_core::manifold::EmptyState),
                    scope,
                )),
                Driver::Absent,
            ),
        };
        if bound.is_some() {
            match scope {
                StateScope::Driver => driver = Driver::Present,
                StateScope::Router => {
                    return Err(syn::Error::new_spanned(
                        generics,
                        "router state bounds cannot bind a driver lifetime",
                    ));
                }
            }
        }
        let state_ty = state.ty();
        let mut family = types::Generics::new(generics);
        family.include(types::Lifetimes::new(state_ty).named());
        family
            .lifetimes
            .retain(|lifetime| lifetime.ident.unraw() != "d");
        family.reserve().visit_type(state_ty);
        let mut routes = Vec::new();
        Self::flatten("", &[], entries, &mut routes);
        for route in &routes {
            let mut names = family.reserve();
            names.visit_type_path(&route.route);
            for guard in &route.guards {
                names.visit_type_path(guard);
            }
            match &route.kind {
                inputs::Kind::Async { capacity, .. } | inputs::Kind::Stream { capacity } => {
                    names.visit_expr(capacity);
                }
                inputs::Kind::Sync => {}
            }
        }
        let mut built = routes
            .into_iter()
            .map(|route| build_route_entry(route, scope));
        let (first, first_k) = match built.next() {
            Some(first) => first?,
            None => {
                return Err(syn::Error::new_spanned(
                    &name,
                    "an application requires at least one route",
                ));
            }
        };
        let mut mix = Homogeneity::Uniform(first_k);
        let mut rest = Vec::new();
        for entry in built {
            let (entry, kind) = entry?;
            mix.include(kind);
            rest.push(entry);
        }
        let specs = Entries {
            first,
            rest,
            homogeneity: mix,
        };
        if specs.len() > u32::MAX as usize {
            return Err(syn::Error::new_spanned(
                &name,
                "an application route count must fit in u32",
            ));
        }
        let plan = route::Plan::new(&specs)?;

        Ok(Self {
            vis,
            name,
            state,
            driver,
            state_generics: family,
            route_specs: specs,
            route_bounds: Vec::new(),
            route_plan: plan,
        })
    }

    fn flatten(
        prefix: &str,
        guards: &[syn::TypePath],
        entries: Vec<inputs::Entry>,
        out: &mut Vec<Route>,
    ) {
        for entry in entries {
            match entry {
                inputs::Entry::Service(service) => {
                    let inputs::Service {
                        method,
                        protocol,
                        path,
                        request,
                        route,
                    } = *service;
                    use syn::LitStr;
                    let full = format!("{prefix}{}", path.value());
                    out.push(Route {
                        route: route.ty,
                        method,
                        protocol,
                        path: LitStr::new(&full, path.span()),
                        guards: guards.to_vec(),
                        request,
                        kind: route.kind,
                    });
                }
                inputs::Entry::Scope {
                    prefix: path,
                    guards: added,
                    children,
                } => {
                    let path = format!("{prefix}{}", path.value());
                    let mut guards = guards.to_vec();
                    guards.extend(added);
                    Self::flatten(&path, &guards, children, out);
                }
            }
        }
    }
}

fn build_route_bound(
    paths: &Paths,
    generics: &types::Generics,
    entry: &Entry,
    state_ty: &syn::Type,
) -> proc_macro2::TokenStream {
    let l_env = generics.lifetime("env");
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_fiber = &paths.fiber;
    let ns_h1 = &paths.h1;
    use quote::quote;
    let route = &entry.route;
    let guards = &entry.guards;
    let request = (!entry.request.any()).then(|| {
        quote! {
            <#route as #ns_core::route::Spec>::Request:
                #ns_core::route::Schema<Error = ::core::convert::Infallible>,
        }
    });
    let path = if entry.request.path {
        quote! {
            <<#route as #ns_core::route::Spec>::Request
                as #ns_core::route::Schema>::RawParams:
                #ns_core::route::Params,
        }
    } else {
        quote! {
            <#route as #ns_core::route::Spec>::Request:
                #ns_core::route::Schema<RawParams = ()>,
        }
    };
    let header = if entry.request.header {
        quote!()
    } else {
        quote! {
            <#route as #ns_core::route::Spec>::Request:
                #ns_core::route::Schema<
                    Header = #ns_core::route::field::header::Discard
                >,
        }
    };
    let query = if entry.request.query {
        quote!()
    } else {
        quote! {
            <#route as #ns_core::route::Spec>::Request:
                #ns_core::route::Schema<
                    Query = #ns_core::route::field::query::Discard
                >,
        }
    };
    let upgrade = entry.upgrade.then(|| {
        quote! {
            <#route as #ns_core::duplex::Route<#l_env, #state_ty>>::Session:
                #ns_h1::route::duplex::WebSocket,
        }
    });
    let (kind, invoke, extra) = match &entry.mode {
        Mode::Sync => (
            quote!(#ns_http::payload::response::dispatch::Sync),
            quote!(),
            quote!(),
        ),
        Mode::Fiber { stream, .. } => (
            quote!(#ns_fiber::manifold::runtime::NativeFiber),
            quote!(+ #ns_fiber::manifold::runtime::TaskRoute<'d, #state_ty>),
            fiber_bound(paths, route, state_ty, *stream),
        ),
        Mode::Stream { .. } => (
            quote!(#ns_http::payload::response::dispatch::Polled),
            quote!(+ #ns_core::manifold::Route<#state_ty>),
            quote!(),
        ),
        Mode::Duplex => (
            quote!(#ns_core::duplex::Shape),
            quote!(+ #ns_core::duplex::Route<#l_env, #state_ty>),
            quote! {
                <#route as #ns_core::duplex::Route<#l_env, #state_ty>>::Session:
                    #ns_core::duplex::Session<'d>,
            },
        ),
    };
    quote! {
        #route: #ns_core::route::Spec<Kind = #kind> #invoke,
        #request
        #path
        #header
        #query
        #( #guards: #ns_core::route::guard::Guard<#state_ty>, )*
        #upgrade
        #extra
    }
}

pub(super) fn fiber_bound(
    paths: &Paths,
    route: &syn::TypePath,
    state: &syn::Type,
    stream: bool,
) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let ns_fiber = &paths.fiber;
    let ns_http = &paths.http;
    let output = quote::quote!(
        <<#route as #ns_core::route::Task<'d, #state>>::Output
            as #ns_http::payload::response::dispatch::IntoOutput<'d, 'd>>::Output
    );
    if stream {
        quote::quote! {
            #route: #ns_core::route::Task<'d, #state>,
            #ns_core::manifold::task::response::Stream<'d, #route, #state>:
                #ns_fiber::dope_fiber::abi::Stream<
                    'd,
                    Item: ::core::convert::TryInto<
                        #ns_http::payload::response::dispatch::chunk::Chunk<'d>,
                        Error: ::core::fmt::Debug,
                    >,
                >,
        }
    } else {
        quote::quote!(#output: #ns_http::payload::response::dispatch::Complete<'d, 'd>,)
    }
}

/// Recognizes `'_` as the driver lifetime.
/// Named lifetimes belong to the caller; `d` is reserved for the expansion.
fn driver(ty: &syn::Type, scope: StateScope) -> syn::Result<Driver> {
    let lives = types::Lifetimes::new(ty);
    if lives
        .named()
        .iter()
        .any(|lifetime| lifetime.ident.unraw() == "d")
    {
        return Err(syn::Error::new_spanned(
            ty,
            "write the driver lifetime as `'_`; `d` is reserved for the expansion",
        ));
    }
    if !matches!(scope, StateScope::Driver) {
        return Ok(Driver::Absent);
    }
    match lives.placeholders() {
        0 => Ok(Driver::Absent),
        1 => Ok(Driver::Present),
        _ => Err(syn::Error::new_spanned(
            ty,
            "a state type carries at most one elided lifetime: the driver",
        )),
    }
}

/// Maps anonymous state lifetimes to the expansion's driver scope.
fn normalize_state(ty: syn::Type, scope: StateScope) -> syn::Type {
    match scope {
        StateScope::Driver => types::Lifetimes::new(&ty).anonymous("d"),
        StateScope::Router => types::Lifetimes::new(&ty).distinct("__state_data").0,
    }
}

fn build_route_entry(entry: Route, scope: StateScope) -> syn::Result<(Entry, Kind)> {
    use crate::app::route::Method;
    let path = entry.path;
    let method = Method::parse(&entry.method)?;
    let upgrade = entry.method == "WS";
    if entry.protocol.is_some() && method != Method::Connect {
        return Err(syn::Error::new_spanned(
            entry.protocol,
            "only CONNECT routes can name an extended protocol",
        ));
    }
    let route = names::route(entry.route);
    if method == Method::Connect {
        if entry.protocol.is_none() && path.value() != "*" {
            return Err(syn::Error::new_spanned(
                &path,
                "classic CONNECT routes use the authority-form wildcard `*`",
            ));
        }
        if entry.protocol.is_some() && !path.value().starts_with('/') {
            return Err(syn::Error::new_spanned(
                &path,
                "extended CONNECT routes use an origin-form path beginning with `/`",
            ));
        }
    }
    let (mode, kind) = if method == Method::Connect || upgrade {
        match (scope, entry.kind) {
            (StateScope::Driver, inputs::Kind::Sync) => (Mode::Duplex, Kind::Duplex),
            (StateScope::Router, inputs::Kind::Sync) => {
                return Err(syn::Error::new_spanned(
                    &route,
                    "fiber-free routers support synchronous routes only",
                ));
            }
            (
                StateScope::Driver | StateScope::Router,
                inputs::Kind::Async { capacity, body, .. },
            ) => {
                return Err(syn::Error::new_spanned(
                    capacity,
                    format!(
                        "CONNECT session construction is synchronous; async `{}` bodies and task capacity are invalid",
                        body.as_str(),
                    ),
                ));
            }
            (StateScope::Driver | StateScope::Router, inputs::Kind::Stream { capacity }) => {
                return Err(syn::Error::new_spanned(
                    capacity,
                    "CONNECT session construction is synchronous; stream capacity is invalid",
                ));
            }
        }
    } else {
        match (scope, entry.kind) {
            (StateScope::Driver, inputs::Kind::Sync) => (Mode::Sync, Kind::Sync),
            (
                StateScope::Driver,
                inputs::Kind::Async {
                    capacity,
                    body,
                    stream,
                },
            ) => (
                Mode::Fiber {
                    capacity,
                    body,
                    stream,
                },
                Kind::Fiber,
            ),
            (StateScope::Driver, inputs::Kind::Stream { capacity }) => {
                (Mode::Stream { capacity }, Kind::Stream)
            }
            (StateScope::Router, inputs::Kind::Sync) => (Mode::Sync, Kind::Sync),
            (StateScope::Router, inputs::Kind::Async { capacity, body, .. }) => {
                return Err(syn::Error::new_spanned(
                    capacity,
                    format!(
                        "fiber-free routers do not support async `{}` body routes",
                        body.as_str()
                    ),
                ));
            }
            (StateScope::Router, inputs::Kind::Stream { capacity }) => {
                return Err(syn::Error::new_spanned(
                    capacity,
                    "fiber-free routers do not support stream routes",
                ));
            }
        }
    };
    Ok((
        Entry {
            route,
            path,
            method,
            upgrade,
            protocol: entry.protocol,
            guards: entry.guards,
            request: entry.request,
            mode,
        },
        kind,
    ))
}
