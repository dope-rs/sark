use syn::{Result, Type};

use crate::util::TypeExt;

#[derive(Clone, Copy)]
pub(super) enum Scalar {
    U64,
    I64,
    F64,
    Bool,
    String,
    Shared,
    Retained,
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
        } else if inner.is_plain_ident("i64") {
            Scalar::I64
        } else if inner.is_plain_ident("f64") {
            Scalar::F64
        } else if inner.is_plain_ident("bool") {
            Scalar::Bool
        } else if inner.is_plain_ident("String") {
            Scalar::String
        } else if inner.is_plain_ident("Shared") {
            Scalar::Shared
        } else if inner.is_bytes_with_storage("Retained") {
            Scalar::Retained
        } else if inner.is_inline_token() {
            Scalar::InlineToken
        } else {
            return Err(syn::Error::new_spanned(
                ty,
                "unsupported #[sark_gen::json] field type",
            ));
        };
        Ok(Self { scalar, optional })
    }
}
