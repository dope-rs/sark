use crate::{
    json::scalar::{Classified, Scalar},
    runtime::paths::Paths,
};

#[derive(Clone, Copy, Default)]
pub(super) struct FieldMode {
    pub(super) raw: bool,
    pub(super) plain: bool,
    pub(super) nested: bool,
    pub(super) seq: bool,
    pub(super) flatten: bool,
}

impl FieldMode {
    pub(super) fn from_field(
        field: &syn::Field,
        plain: bool,
    ) -> syn::Result<(Self, Option<String>)> {
        let mut mode = Self {
            plain,
            ..Self::default()
        };
        let mut name = None;
        for attr in &field.attrs {
            if attr.path().is_ident("raw") {
                mode.raw = true;
                continue;
            }
            if attr.path().is_ident("plain") {
                mode.plain = true;
                continue;
            }
            if attr.path().is_ident("field") {
                use syn::{Meta, punctuated::Punctuated};
                let items =
                    attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)?;
                for item in items {
                    use syn::Error;
                    match item {
                        Meta::Path(path) if path.is_ident("raw") => mode.raw = true,
                        Meta::Path(path) if path.is_ident("plain") => mode.plain = true,
                        Meta::Path(path) if path.is_ident("nested") => mode.nested = true,
                        Meta::Path(path) if path.is_ident("seq") => mode.seq = true,
                        Meta::Path(path) if path.is_ident("flatten") => mode.flatten = true,
                        Meta::NameValue(nv) if nv.path.is_ident("name") => {
                            use syn::{Expr, Lit};
                            let Expr::Lit(expr) = nv.value else {
                                return Err(Error::new_spanned(
                                    nv.value,
                                    "#[field(name = ...)] expects a string literal",
                                ));
                            };
                            if !expr.attrs.is_empty() {
                                return Err(Error::new_spanned(
                                    expr,
                                    "#[field(name = ...)] expects an unadorned string literal",
                                ));
                            }
                            let Lit::Str(s) = expr.lit else {
                                return Err(Error::new_spanned(
                                    expr.lit,
                                    "#[field(name = ...)] expects a string literal",
                                ));
                            };
                            name = Some(s.value());
                        }
                        other => {
                            return Err(Error::new_spanned(
                                other,
                                "#[field(...)] supports only `raw`, `plain`, `nested`, `seq`, `flatten`, or `name = \"...\"`",
                            ));
                        }
                    }
                }
            }
        }
        if mode.flatten && (mode.raw || mode.plain || mode.nested || mode.seq || name.is_some()) {
            return Err(syn::Error::new_spanned(
                field,
                "`flatten` cannot be combined with other field modes or a wire name",
            ));
        }
        Ok((mode, name))
    }

    pub(super) fn task_type(
        self,
        paths: &Paths,
        ty: &syn::Type,
        life: &syn::Lifetime,
    ) -> syn::Result<syn::Type> {
        let ns_json = &paths.json;
        use crate::types;
        let view = types::View::new(ty);
        if self.seq {
            let elem = view.vector().ok_or_else(|| {
                syn::Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field")
            })?;
            let elem = if self.nested {
                syn::parse_quote!(
                    <#elem as #ns_json::segment::IntoRequest>::Request<#life>
                )
            } else {
                Self::to_task(paths, elem, life)
            };
            return Ok(syn::parse_quote!(::std::vec::Vec<#elem>));
        }
        if self.nested {
            return Ok(syn::parse_quote!(
                <#ty as #ns_json::segment::IntoRequest>::Request<#life>
            ));
        }
        if let Some(inner) = view.option() {
            let inner = Self::to_task(paths, inner, life);
            return Ok(syn::parse_quote!(::core::option::Option<#inner>));
        }
        Ok(Self::to_task(paths, ty, life))
    }

    fn to_task(paths: &Paths, ty: &syn::Type, life: &syn::Lifetime) -> syn::Type {
        let ns_http = &paths.http;
        if crate::types::View::new(ty).is_named("Shared") {
            syn::parse_quote!(#ns_http::request::Bytes<#life>)
        } else {
            ty.clone()
        }
    }
}

fn request_scalar_type(paths: &Paths, ty: &syn::Type, mode: FieldMode) -> syn::Type {
    let ns_json = &paths.json;
    if crate::types::View::new(ty).is_named("Shared") {
        if mode.raw || mode.plain {
            return syn::parse_quote!(&'req [::core::primitive::u8]);
        }
        return syn::parse_quote!(#ns_json::Bytes<'req>);
    }
    ty.clone()
}

pub(super) fn request_type(
    paths: &Paths,
    ty: &syn::Type,
    mode: FieldMode,
) -> syn::Result<syn::Type> {
    let ns_json = &paths.json;
    use crate::types;
    let view = types::View::new(ty);
    if mode.seq {
        let elem = view
            .vector()
            .ok_or_else(|| syn::Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field"))?;
        let elem = if mode.nested {
            syn::parse_quote!(<#elem as #ns_json::parsing::Request>::View<'req>)
        } else {
            request_scalar_type(paths, elem, mode)
        };
        return Ok(syn::parse_quote!(::std::vec::Vec<#elem>));
    }
    if mode.nested {
        return Ok(syn::parse_quote!(
            <#ty as #ns_json::parsing::Request>::View<'req>
        ));
    }
    if let Some(inner) = view.option() {
        let inner = request_scalar_type(paths, inner, mode);
        return Ok(syn::parse_quote!(::core::option::Option<#inner>));
    }
    Ok(request_scalar_type(paths, ty, mode))
}

/// Preserves the admitted byte domain in the generated public field.
pub(super) fn proof_type(paths: &Paths, ty: &syn::Type, mode: FieldMode) -> syn::Result<syn::Type> {
    let ns_json = &paths.json;
    let view = crate::types::View::new(ty);
    if mode.nested || mode.flatten {
        return Ok(ty.clone());
    }
    if mode.seq {
        let elem = view
            .vector()
            .ok_or_else(|| syn::Error::new_spanned(ty, "sequence requires Vec"))?;
        let elem = proof_type(paths, elem, FieldMode { seq: false, ..mode })?;
        return Ok(syn::parse_quote!(::std::vec::Vec<#elem>));
    }
    if let Some(inner) = view.option() {
        let inner = proof_type(paths, inner, mode)?;
        return Ok(syn::parse_quote!(::core::option::Option<#inner>));
    }
    let class = Classified::of(ty)?;
    let proof = match class.scalar {
        Scalar::Shared | Scalar::Bytes | Scalar::Borrowed | Scalar::Token => proof_name(mode),
        Scalar::String if mode.raw || mode.plain => proof_name(mode),
        _ => return Ok(ty.clone()),
    };
    Ok(syn::parse_quote!(#ns_json::#proof<#ty>))
}

pub(super) fn proof_name(mode: FieldMode) -> syn::Ident {
    let name = if mode.raw {
        "Raw"
    } else if mode.plain {
        "Plain"
    } else {
        "Text"
    };
    syn::Ident::new(name, proc_macro2::Span::call_site())
}
