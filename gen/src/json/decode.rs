use proc_macro2::TokenStream;
use quote::quote;
use syn::{Result, Type};

use super::field::FieldMode;
use super::scalar::{Classified, Scalar};
use crate::util::TypeExt;

pub(super) struct Decode;

impl Decode {
    pub(super) fn expr(ty: &Type, field_mode: FieldMode) -> Result<TokenStream> {
        if field_mode.seq {
            let push = if field_mode.nested {
                let elem = ty.vec_inner().ok_or_else(|| {
                    syn::Error::new_spanned(ty, "#[field(seq, nested)] requires a Vec<T> field")
                })?;
                quote!(__v.push(
                    <#elem as sark::json::JsonDecode>::decode_json_borrowed(&__raw[__vs..__idx])?
                );)
            } else {
                quote!(__v.push(sark::json::Parse::local(__bytes.clone(), __raw, &mut __idx)?);)
            };
            let capture = if field_mode.nested {
                quote! {
                    let __vs = __idx;
                    sark::json::Scan::skip_value(__raw, &mut __idx)?;
                }
            } else {
                quote!()
            };
            return Ok(quote! {{
                sark::json::Scan::ws(__raw, &mut __idx);
                sark::json::Scan::expect_byte(__raw, &mut __idx, b'[')?;
                let mut __v = Vec::new();
                sark::json::Scan::ws(__raw, &mut __idx);
                if !sark::json::Scan::eat_byte(__raw, &mut __idx, b']') {
                    loop {
                        sark::json::Scan::ws(__raw, &mut __idx);
                        #capture
                        #push
                        sark::json::Scan::ws(__raw, &mut __idx);
                        if sark::json::Scan::eat_byte(__raw, &mut __idx, b',') {
                            continue;
                        }
                        sark::json::Scan::expect_byte(__raw, &mut __idx, b']')?;
                        break;
                    }
                }
                __v
            }});
        }
        if field_mode.nested {
            return Ok(quote! {{
                sark::json::Scan::ws(__raw, &mut __idx);
                let __vs = __idx;
                sark::json::Scan::skip_value(__raw, &mut __idx)?;
                <#ty as sark::json::JsonDecode>::decode_json_borrowed(&__raw[__vs..__idx])?
            }});
        }
        let class = Classified::of(ty)?;
        let decode = match class.scalar {
            Scalar::U64 => quote!(sark::json::Parse::u64(__raw, &mut __idx)?),
            Scalar::Bool => quote!(sark::json::Parse::bool(__raw, &mut __idx)?),
            Scalar::LocalFrameBytes => {
                if field_mode.raw {
                    quote!(sark::json::Parse::local_raw(
                        __bytes.clone(),
                        __raw,
                        &mut __idx
                    )?)
                } else if field_mode.plain {
                    quote!(sark::json::Parse::local_plain(
                        __bytes.clone(),
                        __raw,
                        &mut __idx
                    )?)
                } else {
                    quote!(sark::json::Parse::local(
                        __bytes.clone(),
                        __raw,
                        &mut __idx
                    )?)
                }
            }
            Scalar::InlineToken => {
                if field_mode.plain && !field_mode.raw {
                    quote!(sark::json::Parse::inline_plain(__raw, &mut __idx)?)
                } else {
                    quote!(sark::json::Parse::inline_raw(__raw, &mut __idx)?)
                }
            }
        };
        Ok(if class.optional {
            quote!({
                if sark::json::Scan::eat_null(__raw, &mut __idx) {
                    None
                } else {
                    Some(#decode)
                }
            })
        } else {
            decode
        })
    }

    pub(super) fn empty(ty: &Type) -> Result<TokenStream> {
        let class = Classified::of(ty)?;
        if class.optional {
            return Ok(quote!(None));
        }
        Ok(match class.scalar {
            Scalar::U64 => quote!(0u64),
            Scalar::Bool => quote!(false),
            Scalar::LocalFrameBytes => quote!(sark::json::Parse::empty_local()),
            Scalar::InlineToken => quote!(sark::json::InlineToken::new()),
        })
    }
}
