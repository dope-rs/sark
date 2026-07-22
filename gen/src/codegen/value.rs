#![allow(clippy::too_many_arguments)]

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::{LitByteStr, LitStr, Result, Type};

use super::header::BytesMatch;
use crate::util::{TypeExt, ValueKind};

pub(crate) struct Value;

impl Value {
    pub(crate) fn group_arms_by_length(
        items: impl IntoIterator<Item = (usize, TokenStream)>,
    ) -> Vec<TokenStream> {
        let mut groups: std::collections::BTreeMap<usize, Vec<TokenStream>> =
            std::collections::BTreeMap::new();
        for (len, check) in items {
            groups.entry(len).or_default().push(check);
        }
        groups
            .into_iter()
            .map(|(len, checks)| quote! { #len => { #( #checks )* } })
            .collect()
    }

    pub(crate) fn build_default_retained_expr(default: &LitStr) -> TokenStream {
        let bytes = LitByteStr::new(default.value().as_bytes(), default.span());
        quote! {
            sark_core::http::Bytes::<sark_core::http::Retained>::from(
                ::o3::buffer::Shared::from_static(#bytes)
            )
        }
    }

    pub(crate) fn build_default_borrowed_expr(default: &LitStr) -> TokenStream {
        let bytes = LitByteStr::new(default.value().as_bytes(), default.span());
        quote! {
            sark_core::http::Bytes::<sark_core::http::Borrowed<'static>>::from(#bytes)
        }
    }

    pub(crate) fn build_default_typed_expr(ty: &Type, default: &LitStr) -> Result<TokenStream> {
        let kind = ty.value_kind()?;
        let raw = default.value();
        match kind {
            ValueKind::Bytes => Ok(Self::build_default_retained_expr(default)),
            ValueKind::U64 => {
                let n: u64 = raw
                    .parse()
                    .map_err(|_| syn::Error::new_spanned(default, "invalid u64 default literal"))?;
                let lit = syn::LitInt::new(&format!("{n}u64"), default.span());
                Ok(quote! { #lit })
            }
            ValueKind::Usize => {
                let n: usize = raw.parse().map_err(|_| {
                    syn::Error::new_spanned(default, "invalid usize default literal")
                })?;
                let lit = syn::LitInt::new(&format!("{n}usize"), default.span());
                Ok(quote! { #lit })
            }
            ValueKind::Bool => {
                let b: bool = raw.parse().map_err(|_| {
                    syn::Error::new_spanned(default, "invalid bool default literal")
                })?;
                let lit = syn::LitBool::new(b, default.span());
                Ok(quote! { #lit })
            }
            ValueKind::Range | ValueKind::Custom => Err(syn::Error::new_spanned(
                default,
                "default = \"...\" not supported for Range or custom field types",
            )),
        }
    }

    pub(crate) fn build_required_or_default_expr(
        ty: &Type,
        default: Option<&LitStr>,
        value: TokenStream,
    ) -> Result<TokenStream> {
        if let Some(default) = default {
            let fallback = Self::build_default_typed_expr(ty, default)?;
            return Ok(quote! { #value.unwrap_or_else(|| #fallback) });
        }
        Ok(quote! { #value? })
    }

    pub(crate) fn build_parse_expr(kind: ValueKind, range_expr: TokenStream) -> TokenStream {
        match kind {
            ValueKind::Range | ValueKind::Bytes => quote! { Some(#range_expr) },
            _ => quote! {{
                let value = sark::service::SliceValue::new(input, #range_expr);
                Some(sark::service::FieldValue::parse_value(&value)?)
            }},
        }
    }

    pub(crate) fn build_header_query_field(
        ty: &Type,
        default: Option<&LitStr>,
        ident: &Ident,
        frame_read: TokenStream,
        prefix: &str,
        borrowed: bool,
    ) -> Result<TokenStream> {
        let invariant = format!("{prefix} frame range invariant: stored range must be readable");
        let require_default =
            format!("non-Option {prefix} Bytes<Retained> fields require default = \"...\"");
        let require_typed_default =
            format!("non-Option {prefix} typed fields require default = \"...\"");
        Ok(match ty.value_kind()? {
            ValueKind::Bytes if ty.value_optional() => quote! {
                match #ident {
                    Some(range) => Some(
                        #frame_read.ok_or_else(|| sark::error::Error::BadRequest(#invariant.into()))?
                    ),
                    None => None,
                }
            },
            ValueKind::Bytes => {
                let default =
                    default.ok_or_else(|| syn::Error::new_spanned(ty, require_default.as_str()))?;
                let fallback = if borrowed {
                    Self::build_default_borrowed_expr(default)
                } else {
                    Self::build_default_retained_expr(default)
                };
                quote! {
                    match #ident {
                        Some(range) => {
                            #frame_read.ok_or_else(|| sark::error::Error::BadRequest(#invariant.into()))?
                        }
                        None => #fallback,
                    }
                }
            }
            _ if ty.value_optional() => quote! { #ident },
            _ => {
                let default = default
                    .ok_or_else(|| syn::Error::new_spanned(ty, require_typed_default.as_str()))?;
                let fallback = Self::build_default_typed_expr(ty, default)?;
                quote! { #ident.unwrap_or_else(|| #fallback) }
            }
        })
    }

    pub(crate) fn build_slots<'a>(
        slot_ident: &Ident,
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
        canonical_name: bool,
    ) -> Result<(
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
    )> {
        let entries: Vec<_> = entries
            .into_iter()
            .enumerate()
            .map(|(idx, (ident, bytes, ty))| {
                Ok((format_ident!("S{}", idx), ident, bytes, ty.value_kind()?))
            })
            .collect::<Result<Vec<_>>>()?;
        let variants: Vec<_> = entries.iter().map(|(slot, _, _, _)| slot).collect();
        let enum_tokens = quote! {
            #[derive(Clone, Copy)]
            enum #slot_ident { #( #variants, )* }
        };
        let slot_probe_match =
            Self::group_arms_by_length(entries.iter().map(|(slot, _, bytes, _)| {
                let cond = if canonical_name {
                    let lit = LitByteStr::new(bytes.as_slice(), Span::call_site());
                    quote! { name.eq_ignore_ascii_case(#lit) }
                } else {
                    BytesMatch::exact(&format_ident!("name"), bytes)
                };
                (
                    bytes.len(),
                    quote! { if #cond { return Some(#slot_ident::#slot); } },
                )
            }));
        let slot_probe_fn = quote! {
            match name.len() {
                #( #slot_probe_match )*
                _ => {}
            }
            None
        };
        let slot_probe_u8_arms = Self::group_arms_by_length(entries.iter().enumerate().map(
            |(idx, (_slot, _, bytes, _))| {
                let lower = bytes.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>();
                let cond = BytesMatch::exact(&format_ident!("name"), lower.as_slice());
                let idx = idx as u8;
                (bytes.len(), quote! { if #cond { return Some(#idx); } })
            },
        ));
        let slot_probe_u8_fn = quote! {
            match name.len() {
                #( #slot_probe_u8_arms )*
                _ => {}
            }
            None
        };
        let set_arms: Vec<_> = entries
            .iter()
            .map(|(slot, ident, _, _)| {
                quote! {
                    #slot_ident::#slot => {
                        if headers.#ident.is_none() {
                            headers.#ident = Some(sark::service::FieldValue::parse_value(value)?);
                        }
                    }
                }
            })
            .collect();
        let set_fn = quote! {
            match slot {
                #( #set_arms, )*
            }
            Ok(())
        };
        let set_u8_arms: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(idx, (_slot, ident, _, _))| {
                let assign = quote! { Some(sark::service::FieldValue::parse_value(value)?) };
                let idx = idx as u8;
                quote! {
                    #idx => {
                        if headers.#ident.is_none() {
                            headers.#ident = #assign;
                        }
                    }
                }
            })
            .collect();
        let set_u8_fn = quote! {
            match slot {
                #( #set_u8_arms, )*
                _ => {}
            }
            Ok(())
        };
        let set_name_fn = Self::set_name(
            entries
                .iter()
                .map(|(_slot, ident, bytes, _)| (*ident, bytes.as_slice())),
            canonical_name,
        );
        Ok((
            enum_tokens,
            slot_probe_fn,
            set_fn,
            set_name_fn,
            slot_probe_u8_fn,
            set_u8_fn,
        ))
    }

    pub(crate) fn build_set_name<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
        canonical_name: bool,
    ) -> Result<TokenStream> {
        let entries: Vec<_> = entries
            .into_iter()
            .map(|(ident, bytes, ty)| Ok((ident, bytes, ty.value_kind()?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::set_name(
            entries
                .iter()
                .map(|(ident, bytes, _)| (*ident, bytes.as_slice())),
            canonical_name,
        ))
    }

    pub(crate) fn build_parse_query<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
    ) -> Result<TokenStream> {
        let entries: Vec<_> = entries
            .into_iter()
            .map(|(ident, bytes, ty)| Ok((ident, bytes, ty.value_kind()?)))
            .collect::<Result<Vec<_>>>()?;
        let match_arms = Self::name_match_arms(
            &entries,
            |kind| Self::build_parse_expr(kind, quote!(value_start_abs..value_end_abs)),
            false,
        );
        let per_segment = quote! {
            match name.len() {
                #( #match_arms )*
                _ => {}
            }
        };
        Ok(QueryScanLoop::build(&per_segment))
    }

    pub(crate) fn build_set_slice<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
    ) -> Result<TokenStream> {
        let entries: Vec<_> = entries
            .into_iter()
            .map(|(ident, bytes, ty)| Ok((ident, bytes, ty.value_kind()?)))
            .collect::<Result<Vec<_>>>()?;
        let match_arms = Self::name_match_arms(
            &entries,
            |kind| Self::build_parse_expr(kind, quote!(range)),
            true,
        );
        Ok(quote! {
            match name.len() {
                #( #match_arms )*
                _ => {}
            }
            Ok(())
        })
    }

    fn name_match_arms<F>(
        entries: &[(&Ident, Vec<u8>, ValueKind)],
        parsed_for: F,
        with_return: bool,
    ) -> Vec<TokenStream>
    where
        F: Fn(ValueKind) -> TokenStream,
    {
        let ret = if with_return {
            quote! { return Ok(()); }
        } else {
            TokenStream::new()
        };
        Self::group_arms_by_length(entries.iter().map(|(ident, bytes, kind)| {
            let lit = LitByteStr::new(bytes.as_slice(), Span::call_site());
            let parsed = parsed_for(*kind);
            (
                bytes.len(),
                quote! {
                    if name == #lit {
                        if headers.#ident.is_none() {
                            headers.#ident = #parsed;
                        }
                        #ret
                    }
                },
            )
        }))
    }

    fn set_name<'a>(
        items: impl IntoIterator<Item = (&'a Ident, &'a [u8])>,
        canonical_name: bool,
    ) -> TokenStream {
        let arms = Value::group_arms_by_length(items.into_iter().map(|(ident, bytes)| {
            let lit = LitByteStr::new(bytes, Span::call_site());
            let cond = if canonical_name {
                quote! { name.eq_ignore_ascii_case(#lit) }
            } else {
                quote! { name == #lit }
            };
            let assign = quote! { Some(sark::service::FieldValue::parse_value(value)?) };
            (
                bytes.len(),
                quote! {
                    if #cond {
                        if headers.#ident.is_none() {
                            headers.#ident = #assign;
                        }
                        return Ok(());
                    }
                },
            )
        }));
        quote! {
            match name.len() {
                #( #arms )*
                _ => {}
            }
            Ok(())
        }
    }
}

pub(crate) struct QueryScanLoop;

impl QueryScanLoop {
    pub(crate) fn build(per_segment: &TokenStream) -> TokenStream {
        quote! {
            if range.start >= range.end {
                return Ok(());
            }
            let bytes = &input[range.clone()];
            let mut seg_start = 0usize;
            let mut eq_idx = usize::MAX;
            let mut idx = 0usize;
            while idx < bytes.len() {
                let b = bytes[idx];
                if b == b'=' {
                    if eq_idx == usize::MAX {
                        eq_idx = idx;
                    }
                    idx += 1;
                    continue;
                }
                if b == b'&' {
                    if seg_start < idx {
                        let key_end = if eq_idx == usize::MAX { idx } else { eq_idx };
                        let value_start = if eq_idx == usize::MAX { idx } else { eq_idx + 1 };
                        let name = &bytes[seg_start..key_end];
                        let value_start_abs = range.start + value_start;
                        let value_end_abs = range.start + idx;
                        #per_segment
                    }
                    seg_start = idx + 1;
                    eq_idx = usize::MAX;
                }
                idx += 1;
            }
            if seg_start < bytes.len() {
                let key_end = if eq_idx == usize::MAX { bytes.len() } else { eq_idx };
                let value_start = if eq_idx == usize::MAX { bytes.len() } else { eq_idx + 1 };
                let name = &bytes[seg_start..key_end];
                let value_start_abs = range.start + value_start;
                let value_end_abs = range.end;
                #per_segment
            }
            Ok(())
        }
    }
}
