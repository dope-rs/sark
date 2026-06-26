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
                quote!(sark::json::JsonEncode::write_into(__e, __w);)
            } else {
                quote!(__w.put_str(__e.as_bytes());)
            };
            return Ok(quote! {{
                __w.put(b"[");
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __w.put(b",");
                    }
                    __first = false;
                    #elem
                }
                __w.put(b"]");
            }});
        }
        if fmode.nested {
            return Ok(quote!(sark::json::JsonEncode::write_into(&#access, __w);));
        }
        let class = Classified::of(ty)?;
        let write = match class.scalar {
            Scalar::U64 => quote!(__w.put_u64(#access);),
            Scalar::Bool => quote! {
                if #access {
                    __w.put(b"true");
                } else {
                    __w.put(b"false");
                }
            },
            Scalar::LocalFrameBytes | Scalar::InlineToken => {
                if fmode.raw {
                    quote!(__w.put(#access.as_bytes());)
                } else if fmode.plain {
                    quote!(__w.put_str_plain(#access.as_bytes());)
                } else {
                    quote!(__w.put_str(#access.as_bytes());)
                }
            }
        };
        Ok(if class.optional {
            quote! {
                match #access {
                    Some(value) => { #write }
                    None => { __w.put(b"null"); }
                }
            }
        } else {
            write
        })
    }
}
