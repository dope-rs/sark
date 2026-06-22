use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Fields, FnArg, Ident, ItemStruct, LitStr, Result, ReturnType, Token, Type,
    Visibility, braced,
};

use crate::model::{
    DefineRouteEntry, DefineRouteInput, HeaderAttrField, PathAttrField, QueryAttrField,
    RequestKind, ResponseKind,
};
use crate::util::{AttributeSliceExt, FieldAttr};

pub(super) type RouteCfg = (
    Option<Type>,
    Option<Type>,
    bool,
    Option<Type>,
    bool,
    Option<ResponseKind>,
    Option<RequestKind>,
    Option<syn::Expr>,
);
type StructRouteCfg = (
    Vec<HeaderAttrField>,
    Vec<QueryAttrField>,
    Vec<PathAttrField>,
    Type,
    Option<Type>,
    bool,
    Option<Type>,
    bool,
    ResponseKind,
    RequestKind,
    Option<syn::Expr>,
);

impl Parse for DefineRouteInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis = input.parse::<Visibility>()?;
        let name = input.parse::<Ident>()?;
        input.parse::<Token![:]>()?;
        let state_ty = input.parse::<Type>()?;
        input.parse::<Token![=>]>()?;
        let body;
        braced!(body in input);
        let entries = parse_define_route_entries(&body)?;
        Ok(Self {
            vis,
            name,
            state_ty,
            entries,
        })
    }
}

fn parse_define_route_entries(input: ParseStream<'_>) -> Result<Vec<DefineRouteEntry>> {
    let mut out = Vec::new();
    while !input.is_empty() {
        out.push(parse_define_route_entry(input)?);
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(out)
}

fn parse_define_route_entry(input: ParseStream<'_>) -> Result<DefineRouteEntry> {
    if input.peek(syn::Ident)
        && input
            .fork()
            .parse::<Ident>()
            .map(|i| i == "scope")
            .unwrap_or(false)
    {
        let _ = input.parse::<Ident>()?;
        let prefix = input.parse::<LitStr>()?;
        let mut wraps = Vec::new();
        if input.peek(syn::Ident)
            && input
                .fork()
                .parse::<Ident>()
                .map(|i| i == "with")
                .unwrap_or(false)
        {
            input.parse::<Ident>()?;
            let wrap_body;
            syn::parenthesized!(wrap_body in input);
            let punctuated: syn::punctuated::Punctuated<syn::TypePath, Token![,]> =
                Punctuated::parse_terminated(&wrap_body)?;
            wraps = punctuated.into_iter().collect();
        }
        input.parse::<Token![=>]>()?;
        let children = if input.peek(syn::token::Bracket) {
            let body;
            syn::bracketed!(body in input);
            let mut services = Vec::new();
            while !body.is_empty() {
                let method = body.parse::<Ident>()?;
                let path = body.parse::<LitStr>()?;
                body.parse::<Token![=>]>()?;
                let (kind, capacity) = DefineRouteEntry::parse_route_opts(&body)?;
                let ty = body.parse::<syn::TypePath>()?;
                services.push(DefineRouteEntry::Service {
                    method,
                    path,
                    ty,
                    kind,
                    capacity,
                });
                if body.peek(Token![,]) {
                    body.parse::<Token![,]>()?;
                }
            }
            services
        } else if input.peek(syn::token::Brace) {
            let body;
            braced!(body in input);
            parse_define_route_entries(&body)?
        } else {
            return Err(input.error("expected `[ ... ]` or `{ ... }` after scope `=>`"));
        };
        Ok(DefineRouteEntry::Scope {
            prefix,
            wraps,
            children,
        })
    } else {
        let method = input.parse::<Ident>()?;
        let path = input.parse::<LitStr>()?;
        input.parse::<Token![=>]>()?;
        let (kind, capacity) = DefineRouteEntry::parse_route_opts(input)?;
        let ty = input.parse::<syn::TypePath>()?;
        Ok(DefineRouteEntry::Service {
            method,
            path,
            ty,
            kind,
            capacity,
        })
    }
}

impl DefineRouteEntry {
    fn parse_route_opts(
        input: ParseStream<'_>,
    ) -> Result<(crate::model::RouteKind, Option<syn::LitInt>)> {
        let is_async = input.parse::<Option<Token![async]>>()?.is_some();
        let is_stream = if input.peek(syn::Ident)
            && input
                .fork()
                .parse::<Ident>()
                .map(|i| i == "stream")
                .unwrap_or(false)
        {
            input.parse::<Ident>()?;
            true
        } else {
            false
        };
        let kind = match (is_async, is_stream) {
            (false, false) => crate::model::RouteKind::Sync,
            (true, false) => crate::model::RouteKind::Fiber,
            (_, true) => crate::model::RouteKind::Stream,
        };
        if matches!(kind, crate::model::RouteKind::Sync) {
            return Ok((kind, None));
        }
        let capacity = if input.peek(syn::token::Paren) {
            let opts;
            syn::parenthesized!(opts in input);
            let key = opts.parse::<Ident>()?;
            if key != "capacity" {
                return Err(syn::Error::new(
                    key.span(),
                    "unknown option (expected `capacity`)",
                ));
            }
            opts.parse::<Token![=]>()?;
            let cap = opts.parse::<syn::LitInt>()?;
            if !opts.is_empty() {
                return Err(opts.error("trailing tokens after `capacity = N`"));
            }
            Some(cap)
        } else {
            None
        };
        Ok((kind, capacity))
    }
}

pub(super) fn parse_struct_route_cfg(st: &mut ItemStruct) -> Result<StructRouteCfg> {
    let (
        state_ty,
        body_ty,
        raw_body,
        request_ty,
        static_response,
        response_body_kind,
        request_body_kind,
        max_body,
    ) = take_route_cfg_attrs(&mut st.attrs)?;
    let state_ty = state_ty.unwrap_or_else(|| syn::parse_quote!(()));
    let response_body_kind = response_body_kind.unwrap_or_default();
    let request_body_kind = request_body_kind.unwrap_or_default();
    let mut headers = Vec::new();
    let mut queries = Vec::new();
    let mut paths = Vec::new();
    if let Fields::Named(named) = &st.fields {
        for field in &named.named {
            let Some(ident) = field.ident.clone() else {
                continue;
            };
            if let Some(FieldAttr {
                name: header,
                default,
            }) = field.attrs.field_attr("header")
            {
                headers.push(HeaderAttrField {
                    ident,
                    header,
                    default,
                    ty: field.ty.clone(),
                });
            } else if let Some(FieldAttr {
                name: query,
                default,
            }) = field.attrs.field_attr("query")
            {
                queries.push(QueryAttrField {
                    ident,
                    query,
                    default,
                    ty: field.ty.clone(),
                });
            } else if let Some(FieldAttr {
                name: path,
                default,
            }) = field.attrs.field_attr("path")
            {
                paths.push(PathAttrField {
                    ident,
                    path,
                    default,
                    ty: field.ty.clone(),
                });
            }
        }
    }
    st.fields = Fields::Unit;
    Ok((
        headers,
        queries,
        paths,
        state_ty,
        body_ty,
        raw_body,
        request_ty,
        static_response,
        response_body_kind,
        request_body_kind,
        max_body,
    ))
}

pub(super) fn take_route_cfg_attrs(attrs: &mut Vec<Attribute>) -> Result<RouteCfg> {
    let mut state_ty: Option<Type> = None;
    let mut body_ty: Option<Type> = None;
    let mut raw_body = false;
    let mut request_ty: Option<Type> = None;
    let mut static_response = false;
    let mut response_body_kind: Option<ResponseKind> = None;
    let mut request_body_kind: Option<RequestKind> = None;
    let mut max_body: Option<syn::Expr> = None;
    let mut kept = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if attr.path().is_ident("state") {
            state_ty = Some(attr.parse_args::<Type>()?);
        } else if attr.path().is_ident("body") {
            body_ty = Some(attr.parse_args::<Type>()?);
        } else if attr.path().is_ident("raw_body") {
            raw_body = true;
        } else if attr.path().is_ident("request") {
            if request_ty.is_some() {
                return Err(syn::Error::new_spanned(attr, "duplicate #[request(...)]"));
            }
            request_ty = Some(attr.parse_args::<Type>()?);
        } else if attr.path().is_ident("static_response") {
            static_response = true;
        } else if attr.path().is_ident("response_body") {
            let kind_ident: Ident = attr.parse_args()?;
            response_body_kind = Some(match kind_ident.to_string().as_str() {
                "Inline" => ResponseKind::Inline,
                "Static" => ResponseKind::Static,
                "Stream" => ResponseKind::Stream,
                other => {
                    return Err(syn::Error::new_spanned(
                        kind_ident,
                        format!(
                            "unknown response_body kind `{other}`; expected Inline | Static | Stream"
                        ),
                    ));
                }
            });
        } else if attr.path().is_ident("request_body") {
            let kind_ident: Ident = attr.parse_args()?;
            request_body_kind = Some(match kind_ident.to_string().as_str() {
                "Inline" => RequestKind::Inline,
                "Spilled" => RequestKind::Spilled,
                other => {
                    return Err(syn::Error::new_spanned(
                        kind_ident,
                        format!("unknown request_body kind `{other}`; expected Inline | Spilled"),
                    ));
                }
            });
        } else if attr.path().is_ident("max_body") {
            if max_body.is_some() {
                return Err(syn::Error::new_spanned(attr, "duplicate #[max_body(...)]"));
            }
            max_body = Some(attr.parse_args::<syn::Expr>()?);
        } else {
            kept.push(attr);
        }
    }
    *attrs = kept;
    Ok((
        state_ty,
        body_ty,
        raw_body,
        request_ty,
        static_response,
        response_body_kind,
        request_body_kind,
        max_body,
    ))
}

fn outer_type_name(ty: &Type) -> Option<String> {
    if let Type::Path(tp) = ty {
        return tp.path.segments.last().map(|s| s.ident.to_string());
    }
    None
}

pub(super) fn infer_response_body_kind(output: &ReturnType) -> ResponseKind {
    let ty = match output {
        ReturnType::Type(_, ty) => &**ty,
        _ => return ResponseKind::Inline,
    };
    match outer_type_name(ty).as_deref() {
        Some("Stream") => ResponseKind::Stream,
        Some("Static") => ResponseKind::Static,
        _ => ResponseKind::Inline,
    }
}

pub(super) fn infer_request_body_kind(inputs: &Punctuated<FnArg, Token![,]>) -> RequestKind {
    if let Some(FnArg::Typed(pat)) = inputs.first()
        && matches!(outer_type_name(&pat.ty).as_deref(), Some("Spilled"))
    {
        return RequestKind::Spilled;
    }
    RequestKind::Inline
}
