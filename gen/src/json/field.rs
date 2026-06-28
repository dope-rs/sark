use syn::punctuated::Punctuated;
use syn::{Expr, ExprLit, Lit, Meta, Result};

#[derive(Clone, Copy, Default)]
pub(super) struct FieldMode {
    pub(super) raw: bool,
    pub(super) unused: bool,
    pub(super) plain: bool,
    pub(super) nested: bool,
    pub(super) seq: bool,
}

impl FieldMode {
    pub(super) fn from_field(
        field: &syn::Field,
        default_plain: bool,
    ) -> Result<(Self, Option<String>)> {
        let mut mode = Self {
            plain: default_plain,
            ..Self::default()
        };
        let mut name = None;
        for attr in &field.attrs {
            if attr.path().is_ident("raw") {
                mode.raw = true;
                continue;
            }
            if attr.path().is_ident("unused") {
                mode.unused = true;
                continue;
            }
            if attr.path().is_ident("plain") {
                mode.plain = true;
                continue;
            }
            if attr.path().is_ident("field") {
                let items =
                    attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)?;
                for item in items {
                    match item {
                        Meta::Path(path) if path.is_ident("raw") => mode.raw = true,
                        Meta::Path(path) if path.is_ident("unused") => mode.unused = true,
                        Meta::Path(path) if path.is_ident("plain") => mode.plain = true,
                        Meta::Path(path) if path.is_ident("nested") => mode.nested = true,
                        Meta::Path(path) if path.is_ident("seq") => mode.seq = true,
                        Meta::NameValue(nv) if nv.path.is_ident("name") => {
                            let Expr::Lit(ExprLit {
                                lit: Lit::Str(s), ..
                            }) = nv.value
                            else {
                                return Err(syn::Error::new_spanned(
                                    nv.value,
                                    "#[field(name = ...)] expects a string literal",
                                ));
                            };
                            name = Some(s.value());
                        }
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "#[field(...)] supports only `raw`, `unused`, `plain`, `nested`, `seq`, or `name = \"...\"`",
                            ));
                        }
                    }
                }
            }
        }
        Ok((mode, name))
    }
}
