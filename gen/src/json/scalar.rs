use crate::types;

#[derive(Clone, Copy)]
pub(super) enum Scalar {
    U64,
    I64,
    F64,
    Bool,
    String,
    Shared,
    Borrowed,
    Bytes,
    Token,
}

#[derive(Clone, Copy)]
pub(super) struct Classified {
    pub(super) scalar: Scalar,
    pub(super) optional: bool,
}

impl Classified {
    pub(super) fn of(ty: &syn::Type) -> syn::Result<Self> {
        let view = types::View::new(ty);
        let optional = view.is_optional();
        let inner = types::View::new(view.value());
        let scalar = if inner.is_named("u64") {
            Scalar::U64
        } else if inner.is_named("i64") {
            Scalar::I64
        } else if inner.is_named("f64") {
            Scalar::F64
        } else if inner.is_named("bool") {
            Scalar::Bool
        } else if inner.is_named("String") {
            Scalar::String
        } else if inner.is_named("Shared") {
            Scalar::Shared
        } else if inner.is_slice() {
            Scalar::Borrowed
        } else if inner.is_named("Bytes") {
            Scalar::Bytes
        } else if inner.is_named("Token") {
            Scalar::Token
        } else {
            use syn::Error;
            return Err(Error::new_spanned(
                ty,
                "unsupported #[sark_gen::json] field type",
            ));
        };
        Ok(Self { scalar, optional })
    }
}
