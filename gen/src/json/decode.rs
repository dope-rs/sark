use proc_macro2::TokenStream;
use quote::quote;
use syn::{Result, Type};

use super::field::FieldMode;
use super::scalar::{Classified, Scalar};
use crate::util::TypeExt;

pub(super) struct Decoder<'a> {
    ty: &'a Type,
    mode: FieldMode,
    request_view: bool,
}

impl<'a> Decoder<'a> {
    pub(super) fn new(ty: &'a Type, mode: FieldMode) -> Self {
        Self {
            ty,
            mode,
            request_view: false,
        }
    }

    pub(super) fn request_view(ty: &'a Type, mode: FieldMode) -> Self {
        Self {
            ty,
            mode,
            request_view: true,
        }
    }

    pub(super) fn expr(&self) -> Result<TokenStream> {
        if self.mode.seq {
            if self.request_view {
                return Err(syn::Error::new_spanned(
                    self.ty,
                    "request JSON views support only flat fields",
                ));
            }
            let elem = self.ty.vec_inner().ok_or_else(|| {
                syn::Error::new_spanned(self.ty, "#[field(seq)] requires a Vec<T> field")
            })?;
            let push = if self.mode.nested {
                quote!(__v.push(
                    <#elem as sark::json::JsonDecode>::decode_json_borrowed(
                        &__raw[__vs..__idx]
                    )?
                );)
            } else {
                match Classified::of(elem)?.scalar {
                    Scalar::Retained => {
                        quote!(__v.push(sark::json::Parse::frame(
                            &__bytes,
                            &mut __idx,
                        )?);)
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            elem,
                            "unsupported JSON sequence field type",
                        ));
                    }
                }
            };
            let capture = if self.mode.nested {
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
        if self.mode.nested {
            if self.request_view {
                return Err(syn::Error::new_spanned(
                    self.ty,
                    "request JSON views support only flat fields",
                ));
            }
            let ty = self.ty;
            return Ok(quote! {{
                sark::json::Scan::ws(__raw, &mut __idx);
                let __vs = __idx;
                sark::json::Scan::skip_value(__raw, &mut __idx)?;
                <#ty as sark::json::JsonDecode>::decode_json_borrowed(
                    &__raw[__vs..__idx]
                )?
            }});
        }
        let class = Classified::of(self.ty)?;
        let decode = match class.scalar {
            Scalar::U64 => quote!(sark::json::Parse::u64(__raw, &mut __idx)?),
            Scalar::I64 | Scalar::F64 | Scalar::String | Scalar::Shared => {
                return Err(syn::Error::new_spanned(
                    self.ty,
                    "this JSON field type requires `#[sark_gen::json(encode)]`",
                ));
            }
            Scalar::Bool => quote!(sark::json::Parse::bool(__raw, &mut __idx)?),
            Scalar::Borrowed => {
                if !self.request_view {
                    return Err(syn::Error::new_spanned(
                        self.ty,
                        "borrowed JSON bytes are available only in request views",
                    ));
                }
                if self.mode.raw {
                    quote!(sark::json::Parse::frame_raw(__raw, &mut __idx)?)
                } else {
                    quote!(sark::json::Parse::frame_plain(__raw, &mut __idx)?)
                }
            }
            Scalar::Retained => {
                if self.request_view {
                    return Err(syn::Error::new_spanned(
                        self.ty,
                        "retained JSON bytes must be rewritten for request views",
                    ));
                }
                if self.mode.raw {
                    quote!(sark::json::Parse::frame_raw(&__bytes, &mut __idx)?)
                } else if self.mode.plain {
                    quote!(sark::json::Parse::frame_plain(&__bytes, &mut __idx)?)
                } else {
                    quote!(sark::json::Parse::frame(&__bytes, &mut __idx)?)
                }
            }
            Scalar::JsonBytes => {
                if !self.request_view {
                    return Err(syn::Error::new_spanned(
                        self.ty,
                        "JsonBytes is available only in request views",
                    ));
                }
                quote!(sark::json::Parse::frame(__raw, &mut __idx)?)
            }
            Scalar::InlineToken => {
                if self.mode.plain && !self.mode.raw {
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

    pub(super) fn empty(&self) -> Result<TokenStream> {
        let class = Classified::of(self.ty)?;
        if class.optional {
            return Ok(quote!(None));
        }
        Ok(match class.scalar {
            Scalar::U64 => quote!(0u64),
            Scalar::I64 | Scalar::F64 | Scalar::String | Scalar::Shared => {
                return Err(syn::Error::new_spanned(
                    self.ty,
                    "this JSON field type requires `#[sark_gen::json(encode)]`",
                ));
            }
            Scalar::Bool => quote!(false),
            Scalar::Borrowed => {
                quote!(sark::sark_core::http::Bytes::<
                    sark::sark_core::http::Borrowed<'req>,
                >::from(&[]))
            }
            Scalar::Retained => quote!(sark::json::Parse::empty_frame()),
            Scalar::JsonBytes => quote!(sark::json::Parse::empty_view()),
            Scalar::InlineToken => quote!(sark::json::InlineToken::new()),
        })
    }
}
