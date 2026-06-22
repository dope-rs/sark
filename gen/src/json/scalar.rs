use syn::{Result, Type};

use crate::util::TypeExt;

#[derive(Clone, Copy)]
pub(super) enum Scalar {
    U64,
    Bool,
    LocalFrameBytes,
    InlineToken,
}

#[derive(Clone, Copy)]
pub(super) struct Classified {
    pub(super) scalar: Scalar,
    pub(super) optional: bool,
}

impl Classified {
    pub(super) fn of(ty: &Type) -> Result<Self> {
        let (inner, optional) = match ty.option_inner() {
            Some(inner) => (inner, true),
            None => (ty, false),
        };
        let scalar = if inner.is_plain_ident("u64") {
            Scalar::U64
        } else if inner.is_plain_ident("bool") {
            Scalar::Bool
        } else if inner.is_plain_ident("LocalFrameBytes") {
            Scalar::LocalFrameBytes
        } else if inner.is_inline_token() {
            Scalar::InlineToken
        } else {
            return Err(syn::Error::new_spanned(
                ty,
                "unsupported #[sark_gen::json] field type; use u64, bool, LocalFrameBytes, or Option<T>",
            ));
        };
        Ok(Self { scalar, optional })
    }
}
