use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Result, Type};

pub(super) struct RawBody;

impl RawBody {
    pub(super) fn field_expr(ty: &Type) -> Result<TokenStream> {
        let Type::Path(path) = ty else {
            return Err(Self::ty_error(ty));
        };
        let Some(seg) = path.path.segments.last() else {
            return Err(Self::ty_error(ty));
        };
        Ok(match seg.ident.to_string().as_str() {
            "Body" => quote!(raw_body),
            "LocalFrameBytes" => quote!(raw_body.into_local()),
            "Shared" => quote!(raw_body.into_bytes()),
            _ => return Err(Self::ty_error(ty)),
        })
    }

    fn ty_error(ty: &Type) -> Error {
        Error::new_spanned(
            ty,
            "#[raw_body] field type must be request::Body, LocalFrameBytes, or Shared",
        )
    }
}
