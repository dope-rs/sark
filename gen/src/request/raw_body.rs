use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Result, Type};

use crate::util::TypeExt;

pub(super) struct RawBody;

impl RawBody {
    pub(super) fn borrowed_type(ty: &Type) -> Result<Type> {
        if ty.is_bytes_with_storage("Retained") {
            return Ok(syn::parse_quote!(
                ::o3::buffer::Bytes<::o3::buffer::Borrowed<'req>>
            ));
        }
        let Type::Path(path) = ty else {
            return Err(Self::ty_error(ty));
        };
        let Some(seg) = path.path.segments.last() else {
            return Err(Self::ty_error(ty));
        };
        Ok(match seg.ident.to_string().as_str() {
            "Shared" => syn::parse_quote!(::o3::buffer::Bytes<::o3::buffer::Borrowed<'req>>),
            _ => return Err(Self::ty_error(ty)),
        })
    }

    pub(super) fn borrowed_field_expr(ty: &Type) -> Result<TokenStream> {
        if ty.is_bytes_with_storage("Retained") {
            return Ok(quote!(req.body_frame()));
        }
        let Type::Path(path) = ty else {
            return Err(Self::ty_error(ty));
        };
        let Some(seg) = path.path.segments.last() else {
            return Err(Self::ty_error(ty));
        };
        Ok(match seg.ident.to_string().as_str() {
            "Shared" => quote!(req.body_frame()),
            _ => return Err(Self::ty_error(ty)),
        })
    }

    fn ty_error(ty: &Type) -> Error {
        Error::new_spanned(
            ty,
            "#[raw_body] field type must be Bytes<Retained> or Shared",
        )
    }
}
