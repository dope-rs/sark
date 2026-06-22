use syn::punctuated::Punctuated;
use syn::{Ident, Result};

#[derive(Clone, Copy, Default)]
pub(super) struct FieldMode {
    pub(super) raw: bool,
    pub(super) unused: bool,
    pub(super) plain: bool,
    pub(super) nested: bool,
    pub(super) seq: bool,
}

impl FieldMode {
    pub(super) fn from_field(field: &syn::Field, default_plain: bool) -> Result<Self> {
        let mut mode = Self {
            plain: default_plain,
            ..Self::default()
        };
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
                    attr.parse_args_with(Punctuated::<Ident, syn::Token![,]>::parse_terminated)?;
                for item in items {
                    if item == "raw" {
                        mode.raw = true;
                    } else if item == "unused" {
                        mode.unused = true;
                    } else if item == "plain" {
                        mode.plain = true;
                    } else if item == "nested" {
                        mode.nested = true;
                    } else if item == "seq" {
                        mode.seq = true;
                    } else {
                        return Err(syn::Error::new_spanned(
                            item,
                            "#[field(...)] supports only `raw`, `unused`, `plain`, `nested`, or `seq`",
                        ));
                    }
                }
            }
        }
        Ok(mode)
    }
}
