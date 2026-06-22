use proc_macro2::TokenStream;
use quote::quote;
use syn::{Result, Type};

use super::field::FieldMode;
use super::scalar::{Classified, Scalar};

pub(super) struct Encode;

impl Encode {
    pub(super) fn len_expr(
        ty: &Type,
        fmode: FieldMode,
        access: TokenStream,
    ) -> Result<TokenStream> {
        if fmode.seq {
            let elem = if fmode.nested {
                quote!(sark::json::JsonEncode::json_len(__e))
            } else {
                quote!(sark::json::Encode::str_len(__e.as_bytes()))
            };
            return Ok(quote! {{
                let mut __n = 2usize;
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __n += 1;
                    }
                    __first = false;
                    __n += #elem;
                }
                __n
            }});
        }
        if fmode.nested {
            return Ok(quote!(sark::json::JsonEncode::json_len(&#access)));
        }
        let class = Classified::of(ty)?;
        let len = match class.scalar {
            Scalar::U64 => quote!(sark::json::Encode::u64_len(#access)),
            Scalar::Bool => quote!(if #access { 4usize } else { 5usize }),
            Scalar::LocalFrameBytes | Scalar::InlineToken => {
                if fmode.raw {
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
        fmode: FieldMode,
        access: TokenStream,
    ) -> Result<TokenStream> {
        if fmode.seq {
            let elem = if fmode.nested {
                quote!(sark::json::JsonEncode::write_json(__e, __out);)
            } else {
                quote!(sark::json::Encode::extend_str(__e.as_bytes(), __out);)
            };
            return Ok(quote! {{
                __out.extend_from_slice(b"[");
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __out.extend_from_slice(b",");
                    }
                    __first = false;
                    #elem
                }
                __out.extend_from_slice(b"]");
            }});
        }
        if fmode.nested {
            return Ok(quote!(sark::json::JsonEncode::write_json(&#access, __out);));
        }
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
                if fmode.raw {
                    quote!(__out.extend_from_slice(#access.as_bytes());)
                } else if fmode.plain {
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
