use proc_macro2::Ident;
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Expr, ExprAssign, GenericArgument, LitStr, PathArguments, Result, Token, Type,
};

#[derive(Clone, Copy)]
pub(super) enum ValueKind {
    Range,
    Local,
    Usize,
    U64,
    Bool,
    Custom,
}

pub(super) struct FieldAttr {
    pub(super) name: LitStr,
    pub(super) default: Option<LitStr>,
}

pub(super) trait TypeExt {
    fn option_inner(&self) -> Option<&Type>;
    fn vec_inner(&self) -> Option<&Type>;
    fn value_inner(&self) -> &Type;
    fn value_optional(&self) -> bool;
    fn is_plain_ident(&self, want: &str) -> bool;
    fn is_inline_token(&self) -> bool;
    fn is_static_byte_slice(&self) -> bool;
    fn is_range_usize(&self) -> bool;
    fn has_local_frame_bytes(&self) -> bool;
    fn rewrite_local_to_ref(&mut self);
    fn value_kind(&self) -> Result<ValueKind>;
    fn raw_field_ty(&self) -> Result<Type>;
    fn type_ident(&self) -> Result<Ident>;
    fn unsupported_field_error(&self) -> syn::Error;
}

impl TypeExt for Type {
    fn option_inner(&self) -> Option<&Type> {
        let Type::Path(path) = self else {
            return None;
        };
        let seg = path.path.segments.last()?;
        if seg.ident != "Option" {
            return None;
        }
        let PathArguments::AngleBracketed(args) = &seg.arguments else {
            return None;
        };
        match args.args.first()? {
            GenericArgument::Type(inner) => Some(inner),
            _ => None,
        }
    }

    fn vec_inner(&self) -> Option<&Type> {
        let Type::Path(path) = self else {
            return None;
        };
        let seg = path.path.segments.last()?;
        if seg.ident != "Vec" {
            return None;
        }
        let PathArguments::AngleBracketed(args) = &seg.arguments else {
            return None;
        };
        match args.args.first()? {
            GenericArgument::Type(inner) => Some(inner),
            _ => None,
        }
    }

    fn value_inner(&self) -> &Type {
        self.option_inner().unwrap_or(self)
    }

    fn value_optional(&self) -> bool {
        self.option_inner().is_some()
    }

    fn is_plain_ident(&self, want: &str) -> bool {
        let Type::Path(path) = self else {
            return false;
        };
        path.path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == want)
    }

    fn is_inline_token(&self) -> bool {
        self.is_plain_ident("InlineToken")
    }

    fn is_static_byte_slice(&self) -> bool {
        let Type::Reference(r) = self else {
            return false;
        };
        let Some(lt) = &r.lifetime else {
            return false;
        };
        if lt.ident != "static" {
            return false;
        }
        let Type::Slice(s) = r.elem.as_ref() else {
            return false;
        };
        let Type::Path(p) = s.elem.as_ref() else {
            return false;
        };
        p.path.is_ident("u8")
    }

    fn is_range_usize(&self) -> bool {
        let Type::Path(path) = self else {
            return false;
        };
        let Some(seg) = path.path.segments.last() else {
            return false;
        };
        if seg.ident != "Range" {
            return false;
        }
        let PathArguments::AngleBracketed(args) = &seg.arguments else {
            return false;
        };
        match args.args.first() {
            Some(GenericArgument::Type(inner)) => inner.is_plain_ident("usize"),
            _ => false,
        }
    }

    fn unsupported_field_error(&self) -> syn::Error {
        syn::Error::new_spanned(
            self,
            "unsupported field type; use Option<LocalFrameBytes>, Option<Range<usize>>, or Option<T> with a supported typed parser",
        )
    }

    fn has_local_frame_bytes(&self) -> bool {
        if self.is_plain_ident("LocalFrameBytes") {
            return true;
        }
        if let Some(inner) = self.option_inner() {
            return inner.is_plain_ident("LocalFrameBytes");
        }
        false
    }

    fn rewrite_local_to_ref(&mut self) {
        if self.is_plain_ident("LocalFrameBytes") {
            *self = syn::parse_quote!(::sark::sark_core::http::LocalFrameBytesRef<'req>);
            return;
        }
        if let Type::Path(path) = self
            && let Some(seg) = path.path.segments.last_mut()
            && seg.ident == "Option"
            && let PathArguments::AngleBracketed(args) = &mut seg.arguments
        {
            for arg in &mut args.args {
                if let GenericArgument::Type(inner) = arg
                    && inner.is_plain_ident("LocalFrameBytes")
                {
                    *inner = syn::parse_quote!(::sark::sark_core::http::LocalFrameBytesRef<'req>);
                }
            }
        }
    }

    fn raw_field_ty(&self) -> Result<Type> {
        match self.value_kind()? {
            ValueKind::Local => Ok(syn::parse_quote! { Option<std::ops::Range<usize>> }),
            _ if self.value_optional() => Ok(self.clone()),
            _ => {
                let ty = self;
                Ok(syn::parse_quote! { Option<#ty> })
            }
        }
    }

    fn value_kind(&self) -> Result<ValueKind> {
        let inner = self.value_inner();
        if inner.is_range_usize() {
            return Ok(ValueKind::Range);
        }
        if inner.is_plain_ident("LocalFrameBytes") {
            return Ok(ValueKind::Local);
        }
        if inner.is_plain_ident("usize") {
            return Ok(ValueKind::Usize);
        }
        if inner.is_plain_ident("u64") {
            return Ok(ValueKind::U64);
        }
        if inner.is_plain_ident("bool") {
            return Ok(ValueKind::Bool);
        }
        let Type::Path(path) = inner else {
            return Err(self.unsupported_field_error());
        };
        if path.qself.is_some() {
            return Err(self.unsupported_field_error());
        }
        Ok(ValueKind::Custom)
    }

    fn type_ident(&self) -> Result<Ident> {
        let Type::Path(path) = self else {
            return Err(syn::Error::new_spanned(
                self,
                "#[request(...)] requires a plain request type",
            ));
        };
        path.path
            .segments
            .last()
            .map(|seg| seg.ident.clone())
            .ok_or_else(|| {
                syn::Error::new_spanned(self, "#[request(...)] requires a plain request type")
            })
    }
}

pub(super) trait AttributeSliceExt {
    fn field_attr(&self, name: &str) -> Option<FieldAttr>;
    fn header_name(&self) -> Result<Option<LitStr>>;
    fn static_headers(&self) -> Result<Vec<(LitStr, LitStr)>>;
}

impl AttributeSliceExt for [Attribute] {
    fn field_attr(&self, name: &str) -> Option<FieldAttr> {
        for attr in self {
            if !attr.path().is_ident(name) {
                continue;
            }
            let args = attr
                .parse_args_with(Punctuated::<Expr, Token![,]>::parse_terminated)
                .ok()?;
            let Expr::Lit(first) = args.first()? else {
                return None;
            };
            let syn::Lit::Str(base) = &first.lit else {
                return None;
            };
            let mut default = None;
            for expr in args.iter().skip(1) {
                let Expr::Assign(ExprAssign { left, right, .. }) = expr else {
                    continue;
                };
                let Expr::Path(path) = &**left else {
                    continue;
                };
                if !path.path.is_ident("default") {
                    continue;
                }
                let Expr::Lit(expr) = &**right else {
                    return None;
                };
                let syn::Lit::Str(lit) = &expr.lit else {
                    return None;
                };
                default = Some(lit.clone());
            }
            return Some(FieldAttr {
                name: base.clone(),
                default,
            });
        }
        None
    }

    fn header_name(&self) -> Result<Option<LitStr>> {
        let mut found = None::<LitStr>;
        for attr in self {
            if !attr.path().is_ident("header") {
                continue;
            }
            if found.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate #[header(...)] attribute",
                ));
            }
            found = Some(attr.parse_args::<LitStr>()?);
        }
        Ok(found)
    }

    fn static_headers(&self) -> Result<Vec<(LitStr, LitStr)>> {
        let mut out = Vec::new();
        for attr in self {
            if attr.path().is_ident("header_static") || attr.path().is_ident("header") {
                let values =
                    attr.parse_args_with(Punctuated::<LitStr, Token![,]>::parse_terminated)?;
                if values.len() != 2 {
                    let msg = if attr.path().is_ident("header_static") {
                        "header_static requires #[header_static(\"name\", \"value\")]"
                    } else {
                        "#[header(\"name\", \"value\")] is only valid on #[sark_gen::response] structs"
                    };
                    return Err(syn::Error::new_spanned(attr, msg));
                }
                let mut it = values.into_iter();
                let name = it.next().unwrap();
                let value = it.next().unwrap();
                out.push((name, value));
            }
        }
        Ok(out)
    }
}
