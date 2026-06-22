use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Result, Type};

use super::scalar::{Classified, Scalar};

pub(super) struct Scan;

impl Scan {
    pub(super) fn field(name: &[u8], ty: &Type, iter_expr: TokenStream) -> Result<TokenStream> {
        let tag = {
            let mut out = Vec::with_capacity(name.len() + 3);
            out.push(b'"');
            out.extend_from_slice(name);
            out.extend_from_slice(b"\":");
            out
        };
        let tag = syn::LitByteStr::new(&tag, Span::call_site());
        let class = Classified::of(ty)?;
        if class.optional {
            return Err(syn::Error::new_spanned(
                ty,
                "`exact` scan does not support Option fields",
            ));
        }
        let (init, push, finish) = match class.scalar {
            Scalar::LocalFrameBytes => (
                quote!(let mut __value = o3::buffer::Owned::with_capacity(24);),
                quote!(__value.extend_from_slice(&[__b]);),
                quote!(sark::sark_core::http::LocalFrameBytes::from_shared(
                    __value.freeze()
                )),
            ),
            Scalar::InlineToken => (
                quote!(let mut __value = sark::json::InlineToken::new();),
                quote!(__value.push(__b)?;),
                quote!(__value),
            ),
            _ => {
                return Err(syn::Error::new_spanned(
                    ty,
                    "`exact` scan currently supports only LocalFrameBytes or InlineToken fields",
                ));
            }
        };
        Ok(quote! {
            let mut __tag_idx = 0usize;
            #init
            let mut __capture = false;
            let mut __seen = false;
            'scan: for __chunk in #iter_expr {
                for &__b in __chunk {
                    if !__capture {
                        if __b == #tag[__tag_idx] {
                            __tag_idx += 1;
                            if __tag_idx == #tag.len() {
                                __capture = true;
                            }
                        } else if __b == #tag[0] {
                            __tag_idx = 1;
                        } else {
                            __tag_idx = 0;
                        }
                        continue;
                    }
                    if !__seen {
                        if __b.is_ascii_whitespace() {
                            continue;
                        }
                        __seen = true;
                    }
                    match __b {
                        b',' | b'}' | b']' => break 'scan,
                        _ => { #push }
                    }
                }
            }
            if __value.is_empty() {
                return Err(sark::json::Json::bad_request("Invalid JSON body"));
            }
            #finish
        })
    }
}
