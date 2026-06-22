use proc_macro2::TokenStream;
use quote::quote;
use syn::{Result, Type};

use super::scalar::{Classified, Scalar};

pub(super) struct Encode;

impl Encode {
    pub(super) fn len_expr(ty: &Type, raw: bool, access: TokenStream) -> Result<TokenStream> {
        let class = Classified::of(ty)?;
        let len = match class.scalar {
            Scalar::U64 => quote!(sark::json::Encode::u64_len(#access)),
            Scalar::Bool => quote!(if #access { 4usize } else { 5usize }),
            Scalar::LocalFrameBytes | Scalar::InlineToken => {
                if raw {
                    quote!(#access.len())
                } else {
                    quote!(sark::json::Encode::str_len(#access.as_bytes()))
                }
            }
        };
        Ok(if class.optional {
            quote! {
                match #access {
                    Some(value) => #len,
                    None => 4usize,
                }
            }
        } else {
            len
        })
    }

    pub(super) fn write_expr(
        ty: &Type,
        raw: bool,
        plain: bool,
        access: TokenStream,
    ) -> Result<TokenStream> {
        let class = Classified::of(ty)?;
        let write = match class.scalar {
            Scalar::U64 => quote!(sark::json::Encode::extend_u64(__out, #access);),
            Scalar::Bool => quote! {
                if #access {
                    __out.extend_from_slice(b"true");
                } else {
                    __out.extend_from_slice(b"false");
                }
            },
            Scalar::LocalFrameBytes | Scalar::InlineToken => {
                if raw {
                    quote!(__out.extend_from_slice(#access.as_bytes());)
                } else if plain {
                    quote! {
                        __out.extend_from_slice(b"\"");
                        __out.extend_from_slice(#access.as_bytes());
                        __out.extend_from_slice(b"\"");
                    }
                } else {
                    quote!(sark::json::Encode::extend_str(#access.as_bytes(), __out);)
                }
            }
        };
        Ok(if class.optional {
            quote! {
                match #access {
                    Some(value) => { #write }
                    None => { __out.extend_from_slice(b"null"); }
                }
            }
        } else {
            write
        })
    }
}
