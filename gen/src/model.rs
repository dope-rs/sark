use syn::{Ident, LitStr, Type, TypePath, Visibility};

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum ResponseKind {
    #[default]
    Inline,
    Static,
    Stream,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum RequestKind {
    #[default]
    Inline,
    Spilled,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteKind {
    Sync,
    Fiber,
    Stream,
}

pub(super) struct AppRouteInput {
    pub(super) route: TypePath,
    pub(super) method: Ident,
    pub(super) path: LitStr,
    pub(super) wraps: Vec<TypePath>,
    pub(super) kind: RouteKind,
    pub(super) capacity: Option<syn::LitInt>,
}

pub(super) struct AppDispatchInput {
    pub(super) vis: Visibility,
    pub(super) name: Ident,
    pub(super) state_ty: Type,
    pub(super) routes: Vec<AppRouteInput>,
}

pub(super) struct DefineRouteInput {
    pub(super) vis: Visibility,
    pub(super) name: Ident,
    pub(super) state_ty: Type,
    pub(super) entries: Vec<DefineRouteEntry>,
}

pub(super) enum DefineRouteEntry {
    Service {
        method: Ident,
        path: LitStr,
        ty: TypePath,
        kind: RouteKind,
        capacity: Option<syn::LitInt>,
    },
    Scope {
        prefix: LitStr,
        wraps: Vec<TypePath>,
        children: Vec<DefineRouteEntry>,
    },
}

#[derive(Clone)]
pub(super) struct HeaderAttrField {
    pub(super) ident: Ident,
    pub(super) header: LitStr,
    pub(super) default: Option<LitStr>,
    pub(super) ty: Type,
}

#[derive(Clone)]
pub(super) struct QueryAttrField {
    pub(super) ident: Ident,
    pub(super) query: LitStr,
    pub(super) default: Option<LitStr>,
    pub(super) ty: Type,
}

#[derive(Clone)]
pub(super) struct PathAttrField {
    pub(super) ident: Ident,
    pub(super) path: LitStr,
    pub(super) default: Option<LitStr>,
    pub(super) ty: Type,
}
