#![allow(clippy::too_many_arguments)]

use std::collections::BTreeMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::{LitByteStr, LitStr, Result, Type};

use super::header::BytesMatch;
use crate::util::{TypeExt, ValueKind};

pub(crate) struct LengthArms {
    groups: BTreeMap<usize, Vec<TokenStream>>,
}

impl LengthArms {
    pub(crate) fn collect(items: impl IntoIterator<Item = (usize, TokenStream)>) -> Self {
        let mut groups: BTreeMap<usize, Vec<TokenStream>> = BTreeMap::new();
        for (len, check) in items {
            groups.entry(len).or_default().push(check);
        }
        Self { groups }
    }

    pub(crate) fn emit(self) -> Vec<TokenStream> {
        self.groups
            .into_iter()
            .map(|(len, checks)| quote! { #len => { #( #checks )* } })
            .collect()
    }
}

pub(crate) struct ValueBinding<'a> {
    ty: &'a Type,
    default: Option<&'a LitStr>,
}

impl<'a> ValueBinding<'a> {
    pub(crate) fn new(ty: &'a Type, default: Option<&'a LitStr>) -> Self {
        Self { ty, default }
    }

    fn default_retained_expr(default: &LitStr) -> TokenStream {
        let bytes = LitByteStr::new(default.value().as_bytes(), default.span());
        quote! {
            sark_core::http::Bytes::<sark_core::http::Retained>::from(
                ::o3::buffer::Shared::from_static(#bytes)
            )
        }
    }

    fn default_borrowed_expr(default: &LitStr) -> TokenStream {
        let bytes = LitByteStr::new(default.value().as_bytes(), default.span());
        quote! {
            sark_core::http::Bytes::<sark_core::http::Borrowed<'static>>::from(#bytes)
        }
    }

    fn default_typed_expr(&self, default: &LitStr) -> Result<TokenStream> {
        let kind = self.ty.value_kind()?;
        let raw = default.value();
        match kind {
            ValueKind::Bytes => Ok(Self::default_retained_expr(default)),
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

    pub(crate) fn default_bytes_expr(
        &self,
        borrowed: bool,
        missing_message: &str,
    ) -> Result<TokenStream> {
        let default = self
            .default
            .ok_or_else(|| syn::Error::new_spanned(self.ty, missing_message))?;
        Ok(if borrowed {
            Self::default_borrowed_expr(default)
        } else {
            Self::default_retained_expr(default)
        })
    }

    pub(crate) fn required_or_default_expr(&self, value: TokenStream) -> Result<TokenStream> {
        if let Some(default) = self.default {
            let fallback = self.default_typed_expr(default)?;
            return Ok(quote! { #value.unwrap_or_else(|| #fallback) });
        }
        Ok(quote! { #value? })
    }

    pub(crate) fn header_query_field(
        &self,
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
        Ok(match self.ty.value_kind()? {
            ValueKind::Bytes if self.ty.value_optional() => quote! {
                match #ident {
                    Some(range) => Some(
                        #frame_read.ok_or_else(|| sark::error::Error::BadRequest(#invariant.into()))?
                    ),
                    None => None,
                }
            },
            ValueKind::Bytes => {
                let fallback = self.default_bytes_expr(borrowed, &require_default)?;
                quote! {
                    match #ident {
                        Some(range) => {
                            #frame_read.ok_or_else(|| sark::error::Error::BadRequest(#invariant.into()))?
                        }
                        None => #fallback,
                    }
                }
            }
            _ if self.ty.value_optional() => quote! { #ident },
            _ => {
                let default = self.default.ok_or_else(|| {
                    syn::Error::new_spanned(self.ty, require_typed_default.as_str())
                })?;
                let fallback = self.default_typed_expr(default)?;
                quote! { #ident.unwrap_or_else(|| #fallback) }
            }
        })
    }
}

pub(crate) struct ParsedValue {
    kind: ValueKind,
    range: TokenStream,
}

impl ParsedValue {
    pub(crate) fn new(kind: ValueKind, range: TokenStream) -> Self {
        Self { kind, range }
    }

    pub(crate) fn emit(self) -> TokenStream {
        let Self { kind, range } = self;
        match kind {
            ValueKind::Range | ValueKind::Bytes => quote! { Some(#range) },
            _ => quote! {{
                let value = sark::service::SliceValue::new(input, #range);
                Some(sark::service::FieldValue::parse_value(&value)?)
            }},
        }
    }
}

#[derive(Clone)]
pub(super) struct FieldSpec {
    pub(super) slot: u8,
    pub(super) variant: Ident,
    pub(super) ident: Ident,
    pub(super) bytes: Vec<u8>,
    pub(super) kind: ValueKind,
}

pub(crate) struct FieldPlan {
    entries: Vec<FieldSpec>,
}

impl FieldPlan {
    pub(crate) fn collect<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
    ) -> Result<Self> {
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(idx, (ident, bytes, ty))| {
                let slot = u8::try_from(idx).map_err(|_| {
                    syn::Error::new_spanned(
                        ident,
                        "too many generated request fields; maximum is 256",
                    )
                })?;
                Ok(FieldSpec {
                    slot,
                    variant: format_ident!("S{}", idx),
                    ident: ident.clone(),
                    bytes,
                    kind: ty.value_kind()?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self { entries })
    }

    pub(super) fn entries(&self) -> &[FieldSpec] {
        &self.entries
    }

    pub(crate) fn slots(
        &self,
        slot_ident: &Ident,
        canonical_name: bool,
    ) -> (
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
    ) {
        let variants: Vec<_> = self.entries.iter().map(|field| &field.variant).collect();
        let enum_tokens = quote! {
            #[derive(Clone, Copy)]
            enum #slot_ident { #( #variants, )* }
        };
        let slot_probe_match = LengthArms::collect(self.entries.iter().map(|field| {
            let slot = &field.variant;
            let bytes = &field.bytes;
            let cond = if canonical_name {
                let lit = LitByteStr::new(bytes.as_slice(), Span::call_site());
                quote! { name.eq_ignore_ascii_case(#lit) }
            } else {
                BytesMatch::Exact.emit(&format_ident!("name"), bytes)
            };
            (
                bytes.len(),
                quote! { if #cond { return Some(#slot_ident::#slot); } },
            )
        }))
        .emit();
        let slot_probe_fn = quote! {
            match name.len() {
                #( #slot_probe_match )*
                _ => {}
            }
            None
        };
        let slot_probe_u8_arms = LengthArms::collect(self.entries.iter().map(|field| {
            let bytes = &field.bytes;
            let lower = bytes.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>();
            let cond = BytesMatch::Exact.emit(&format_ident!("name"), lower.as_slice());
            let idx = field.slot;
            (bytes.len(), quote! { if #cond { return Some(#idx); } })
        }))
        .emit();
        let slot_probe_u8_fn = quote! {
            match name.len() {
                #( #slot_probe_u8_arms )*
                _ => {}
            }
            None
        };
        let set_arms: Vec<_> = self
            .entries
            .iter()
            .map(|field| {
                let slot = &field.variant;
                let ident = &field.ident;
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
        let set_u8_arms: Vec<_> = self
            .entries
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let assign = quote! { Some(sark::service::FieldValue::parse_value(value)?) };
                let idx = field.slot;
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
        let set_name_fn = Self::emit_set_name(
            self.entries
                .iter()
                .map(|field| (&field.ident, field.bytes.as_slice())),
            canonical_name,
        );
        (
            enum_tokens,
            slot_probe_fn,
            set_fn,
            set_name_fn,
            slot_probe_u8_fn,
            set_u8_fn,
        )
    }

    pub(crate) fn set_name(&self, canonical_name: bool) -> TokenStream {
        Self::emit_set_name(
            self.entries
                .iter()
                .map(|field| (&field.ident, field.bytes.as_slice())),
            canonical_name,
        )
    }

    pub(crate) fn parse_query(&self) -> TokenStream {
        let match_arms = Self::name_match_arms(
            &self.entries,
            |kind| ParsedValue::new(kind, quote!(value_start_abs..value_end_abs)).emit(),
            false,
        );
        let per_segment = quote! {
            match name.len() {
                #( #match_arms )*
                _ => {}
            }
        };
        QueryScan::new(per_segment).emit()
    }

    pub(crate) fn set_slice(&self) -> TokenStream {
        let match_arms = Self::name_match_arms(
            &self.entries,
            |kind| ParsedValue::new(kind, quote!(range)).emit(),
            true,
        );
        quote! {
            match name.len() {
                #( #match_arms )*
                _ => {}
            }
            Ok(())
        }
    }

    fn name_match_arms<F>(
        entries: &[FieldSpec],
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
        LengthArms::collect(entries.iter().map(|field| {
            let ident = &field.ident;
            let bytes = &field.bytes;
            let lit = LitByteStr::new(bytes.as_slice(), Span::call_site());
            let parsed = parsed_for(field.kind);
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
        .emit()
    }

    fn emit_set_name<'a>(
        items: impl IntoIterator<Item = (&'a Ident, &'a [u8])>,
        canonical_name: bool,
    ) -> TokenStream {
        let arms = LengthArms::collect(items.into_iter().map(|(ident, bytes)| {
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
        }))
        .emit();
        quote! {
            match name.len() {
                #( #arms )*
                _ => {}
            }
            Ok(())
        }
    }
}

pub(crate) struct QueryScan {
    per_segment: TokenStream,
}

impl QueryScan {
    pub(crate) fn new(per_segment: TokenStream) -> Self {
        Self { per_segment }
    }

    pub(crate) fn emit(self) -> TokenStream {
        let per_segment = self.per_segment;
        quote! {
            if range.start >= range.end {
                return Ok(());
            }
            let bytes = input.get(range.clone()).ok_or_else(|| {
                sark::error::Error::BadRequest("Invalid query range".into())
            })?;
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
