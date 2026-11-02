use syn::ext::IdentExt as _;

use crate::{macros::tokens::local, request::bodies, runtime::paths::Paths, types};

struct FieldAttr {
    name: syn::LitStr,
    default: Option<syn::LitStr>,
}

fn contains(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(name))
}

fn is_only(attrs: &[syn::Attribute], name: &str) -> bool {
    contains(attrs, name)
        && [
            "header",
            "query",
            "path",
            "raw_body",
            "body_len",
            "authority",
        ]
        .into_iter()
        .filter(|source| contains(attrs, source))
        .count()
            == 1
}

fn parse(attrs: &[syn::Attribute], name: &str) -> syn::Result<Option<FieldAttr>> {
    let mut found = None;
    for attr in attrs {
        use syn::{Expr, Lit, punctuated::Punctuated};
        if !attr.path().is_ident(name) {
            continue;
        }
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                format!("a request field may have only one `#[{name}]` attribute"),
            ));
        }
        let args = attr.parse_args_with(Punctuated::<Expr, syn::Token![,]>::parse_terminated)?;
        let Some(first) = args.first() else {
            return Err(syn::Error::new_spanned(
                attr,
                format!("`#[{name}]` requires a string name"),
            ));
        };
        let Expr::Lit(first) = first else {
            return Err(syn::Error::new_spanned(
                first,
                format!("`#[{name}]` name must be a string literal"),
            ));
        };
        let Lit::Str(base) = &first.lit else {
            return Err(syn::Error::new_spanned(
                first,
                format!("`#[{name}]` name must be a string literal"),
            ));
        };
        let mut default = None;
        for expr in args.iter().skip(1) {
            let Expr::Assign(assign) = expr else {
                return Err(syn::Error::new_spanned(
                    expr,
                    format!("`#[{name}]` options after the name must be assignments"),
                ));
            };
            let Expr::Path(option) = assign.left.as_ref() else {
                return Err(syn::Error::new_spanned(
                    assign,
                    format!("`#[{name}]` accepts only `default = \"...\"` after the name"),
                ));
            };
            if !option.path.is_ident("default") {
                return Err(syn::Error::new_spanned(
                    option,
                    format!("unknown `#[{name}]` option; expected `default`"),
                ));
            }
            if default.is_some() {
                return Err(syn::Error::new_spanned(
                    assign,
                    format!("duplicate `#[{name}]` default"),
                ));
            }
            let Expr::Lit(value) = assign.right.as_ref() else {
                return Err(syn::Error::new_spanned(
                    assign,
                    format!("`#[{name}]` default must be a string literal"),
                ));
            };
            let Lit::Str(value) = &value.lit else {
                return Err(syn::Error::new_spanned(
                    value,
                    format!("`#[{name}]` default must be a string literal"),
                ));
            };
            default = Some(value.clone());
        }
        found = Some(FieldAttr {
            name: base.clone(),
            default,
        });
    }
    Ok(found)
}

fn path(attrs: &[syn::Attribute], ident: &syn::Ident) -> syn::Result<Option<syn::LitStr>> {
    let mut found = None;
    for attr in attrs {
        let path = match &attr.meta {
            syn::Meta::Path(path) => path,
            syn::Meta::List(options) => {
                if !options.path.is_ident("path") {
                    continue;
                }
                return Err(syn::Error::new_spanned(
                    options,
                    "`#[path]` does not accept options; the route schema requires this capture",
                ));
            }
            syn::Meta::NameValue(option) => {
                if !option.path.is_ident("path") {
                    continue;
                }
                return Err(syn::Error::new_spanned(
                    option,
                    "`#[path]` does not accept options; the route schema requires this capture",
                ));
            }
        };
        if !path.is_ident("path") {
            continue;
        }
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "a request field may have only one `#[path]` attribute",
            ));
        }
        let name = ident.unraw().to_string();
        found = Some(syn::LitStr::new(&name, ident.span()));
    }
    Ok(found)
}

pub(super) struct Field {
    pub(super) ident: syn::Ident,
    pub(super) name: syn::LitStr,
    pub(super) default: Option<syn::LitStr>,
    pub(super) ty: syn::Type,
}

pub(super) struct Path {
    pub(super) ident: syn::Ident,
    pub(super) path: syn::LitStr,
    pub(super) ty: syn::Type,
}

pub(super) struct Fields {
    pub(super) headers: Vec<Field>,
    pub(super) queries: Vec<Field>,
    pub(super) paths: Vec<Path>,
    pub(super) peer: Option<syn::Type>,
    pub(super) init: Vec<proc_macro2::TokenStream>,
    pub(super) body: bodies::Plan,
}

impl Fields {
    pub(super) fn collect(
        paths: &Paths,
        named: &syn::FieldsNamed,
        body: bodies::Plan,
    ) -> syn::Result<Self> {
        let [v_heads, v_params, v_auth] = ["headers", "params", "__sark_authority"].map(local);
        named.named.iter().try_for_each(|field| {
            if !is_only(&field.attrs, "authority") {
                return Ok(());
            }
            let invalid = || syn::Error::new_spanned(&field.ty, "#[authority] requires Shared");
            let view = types::View::new(&field.ty);
            if view.is_optional() {
                return Err(invalid());
            }
            match view.kind()? {
                types::ValueKind::Bytes => Ok(()),
                types::ValueKind::Usize | types::ValueKind::U64 | types::ValueKind::Bool => {
                    Err(invalid())
                }
            }
        })?;
        let body =
            named.named.iter().try_fold(body, |body, field| {
                if is_only(&field.attrs, "raw_body") {
                    body.try_raw(
                        paths,
                        field.ident.clone().ok_or_else(|| {
                            syn::Error::new_spanned(field, "named field required")
                        })?,
                        &field.ty,
                    )
                } else if is_only(&field.attrs, "body_len") {
                    body.try_len(
                        field.ident.clone().ok_or_else(|| {
                            syn::Error::new_spanned(field, "named field required")
                        })?,
                        &field.ty,
                    )
                } else {
                    Ok(body)
                }
            })?;
        let mut headers = Vec::new();
        let mut queries = Vec::new();
        let mut paths = Vec::new();
        let mut peer = None;
        let mut init = Vec::new();

        for field in &named.named {
            use syn::Error;

            let ident = field.ident.clone().ok_or_else(|| {
                use proc_macro2::Span;

                Error::new(Span::call_site(), "named field required")
            })?;
            if ident.unraw() == "body" {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "#[sark_gen::request] reserves field name `body`",
                ));
            }
            let header = parse(&field.attrs, "header")?;
            let query = parse(&field.attrs, "query")?;
            let path = path(&field.attrs, &ident)?;
            let raw = contains(&field.attrs, "raw_body");
            let len = contains(&field.attrs, "body_len");
            let auth = contains(&field.attrs, "authority");
            let kinds = [
                header.is_some(),
                query.is_some(),
                path.is_some(),
                raw,
                len,
                auth,
            ]
            .into_iter()
            .filter(|value| *value)
            .count();
            if kinds > 1 {
                return Err(syn::Error::new_spanned(
                    field,
                    "request field must have exactly one request source attribute",
                ));
            }
            if let Some(FieldAttr {
                name: header,
                default,
            }) = header
            {
                if headers.iter().any(|field: &Field| {
                    field
                        .name
                        .value()
                        .eq_ignore_ascii_case(header.value().as_str())
                }) {
                    return Err(syn::Error::new_spanned(
                        &header,
                        "duplicate request header name",
                    ));
                }
                headers.push(Field {
                    ident: ident.clone(),
                    name: header,
                    default,
                    ty: field.ty.clone(),
                });
                init.push(quote::quote!(#ident: #v_heads.#ident));
            } else if let Some(FieldAttr {
                name: query,
                default,
            }) = query
            {
                if queries
                    .iter()
                    .any(|field: &Field| field.name.value() == query.value())
                {
                    return Err(syn::Error::new_spanned(
                        &query,
                        "duplicate request query name",
                    ));
                }
                queries.push(Field {
                    ident: ident.clone(),
                    name: query,
                    default,
                    ty: field.ty.clone(),
                });
                init.push(quote::quote!(#ident: #v_heads.#ident));
            } else if let Some(path) = path {
                paths.push(Path {
                    ident: ident.clone(),
                    path,
                    ty: field.ty.clone(),
                });
                init.push(quote::quote!(#ident: #v_params.#ident));
            } else if raw || len {
            } else if auth {
                if let Some(first) = &peer {
                    let mut error = syn::Error::new_spanned(
                        field,
                        "request can contain only one #[authority] field",
                    );
                    error.combine(syn::Error::new_spanned(
                        first,
                        "first #[authority] field declared here",
                    ));
                    return Err(error);
                }
                peer = Some(field.ty.clone());
                init.push(quote::quote!(#ident: #v_auth));
            } else {
                return Err(syn::Error::new_spanned(
                    field,
                    "#[sark_gen::request] fields require a request source attribute",
                ));
            }
        }

        paths.sort_by_key(|field| field.path.value());
        for (left, right) in paths.iter().zip(paths.iter().skip(1)) {
            if left.path.value() == right.path.value() {
                return Err(syn::Error::new_spanned(
                    &right.ident,
                    "duplicate request path parameter name",
                ));
            }
        }

        Ok(Self {
            headers,
            queries,
            paths,
            peer,
            init,
            body,
        })
    }

    pub(super) fn commit(attrs: &mut Vec<syn::Attribute>, named: &mut syn::FieldsNamed) {
        attrs.retain(|attr| !attr.path().is_ident("json_body"));
        for field in &mut named.named {
            field.attrs.retain(|attr| {
                !attr.path().is_ident("header")
                    && !attr.path().is_ident("query")
                    && !attr.path().is_ident("path")
                    && !attr.path().is_ident("raw_body")
                    && !attr.path().is_ident("body_len")
                    && !attr.path().is_ident("authority")
            });
        }
    }
}
