#![allow(clippy::too_many_arguments)]

use std::collections::BTreeMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::{LitByteStr, Result, Type};

use super::value::Value;
use crate::util::{TypeExt, ValueKind};

pub(crate) struct Emit;

#[derive(Clone, Copy, Default)]
pub(crate) enum HeaderValueMode {
    #[default]
    Full,
    Skip,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum HeaderApplyMode {
    #[default]
    Full,
    Skip,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct HeaderParserCfg {
    pub(crate) value: HeaderValueMode,
    pub(crate) apply: HeaderApplyMode,
}

#[derive(Clone, Copy)]
struct KnownHeaderSpec {
    suffix: &'static str,
    bytes: &'static [u8],
}

const KNOWN_HEADERS: [KnownHeaderSpec; 5] = [
    KnownHeaderSpec {
        suffix: "HOST",
        bytes: b"host",
    },
    KnownHeaderSpec {
        suffix: "CONNECTION",
        bytes: b"connection",
    },
    KnownHeaderSpec {
        suffix: "CONTENT_LENGTH",
        bytes: b"content-length",
    },
    KnownHeaderSpec {
        suffix: "TRANSFER_ENCODING",
        bytes: b"transfer-encoding",
    },
    KnownHeaderSpec {
        suffix: "EXPECT",
        bytes: b"expect",
    },
];

impl KnownHeaderSpec {
    fn helper_ident(&self) -> Ident {
        format_ident!("apply_{}", self.suffix.to_ascii_lowercase())
    }

    fn apply_call(&self) -> TokenStream {
        let helper = self.helper_ident();
        if self.suffix == "HOST" {
            quote! { sark::sark_core::http::head::#helper(scan, flags)?; }
        } else {
            quote! { sark::sark_core::http::head::#helper(scan, flags, raw)?; }
        }
    }
}

#[derive(Clone)]
struct HeaderField {
    slot: u8,
    ident: Ident,
    bytes: Vec<u8>,
    kind: ValueKind,
}

struct HeaderPlan {
    known: Vec<Option<HeaderField>>,
    custom: Vec<HeaderField>,
}

impl HeaderPlan {
    fn collect<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
    ) -> Result<Self> {
        let mut known = vec![None; KNOWN_HEADERS.len()];
        let mut custom = Vec::new();
        for (idx, (ident, bytes, ty)) in entries.into_iter().enumerate() {
            let field = HeaderField {
                slot: idx as u8,
                ident: ident.clone(),
                bytes: bytes.iter().map(u8::to_ascii_lowercase).collect(),
                kind: ty.value_kind()?,
            };
            if let Some(known_idx) = KNOWN_HEADERS
                .iter()
                .position(|known| known.bytes == field.bytes)
            {
                known[known_idx] = Some(field);
            } else {
                custom.push(field);
            }
        }
        Ok(Self { known, custom })
    }

    fn is_empty(&self) -> bool {
        self.custom.is_empty() && self.known.iter().all(Option::is_none)
    }
}

struct ActionSpec {
    variant: Ident,
    bytes: Vec<u8>,
    body: TokenStream,
}

#[derive(Clone, Copy)]
enum KnownKind {
    Host,
    Connection,
    ContentLength,
    TransferEncoding,
    Expect,
}

impl KnownKind {
    fn from_index(idx: usize) -> Self {
        match idx {
            0 => Self::Host,
            1 => Self::Connection,
            2 => Self::ContentLength,
            3 => Self::TransferEncoding,
            4 => Self::Expect,
            _ => unreachable!(),
        }
    }

    fn line_fn(self) -> TokenStream {
        match self {
            Self::Host => quote!(sark::sark_core::http::head::host_line),
            Self::Expect => quote!(sark::sark_core::http::head::expect_line),
            Self::Connection => quote!(sark::sark_core::http::head::conn_line),
            Self::ContentLength => quote!(sark::sark_core::http::head::clen_line),
            Self::TransferEncoding => quote!(sark::sark_core::http::head::te_line),
        }
    }

    fn build_contig_arm(
        self,
        capture: Option<&HeaderField>,
        count_tail: &TokenStream,
        skip_apply: bool,
    ) -> TokenStream {
        let capture_body = capture.map(|field| {
            let raw_expr = quote! { &rest[colon_idx + 1 + value_start..colon_idx + 1 + value_end] };
            let abs_start = quote! { line_start + colon_idx + 1 + value_start };
            let abs_end = quote! { line_start + colon_idx + 1 + value_end };
            Assign::emit(&field.ident, field.kind, raw_expr, abs_start, abs_end)
        });
        let maybe_assign = if skip_apply {
            TokenStream::new()
        } else {
            capture_body
                .map(|assign| {
                    quote! {
                        let raw = &rest[colon_idx + 1 + value_start..colon_idx + 1 + value_end];
                        #assign
                    }
                })
                .unwrap_or_default()
        };
        let line_fn = self.line_fn();
        quote! {{
            let Some((tail_end, value_start, value_end)) =
                #line_fn(scan, flags, &rest[colon_idx + 1..])?
            else {
                return Ok(None);
            };
            let _ = (value_start, value_end);
            #count_tail
            #maybe_assign
            return Ok(Some(colon_idx + 1 + tail_end));
        }}
    }
}

struct Assign;

impl Assign {
    fn emit(
        ident: &Ident,
        kind: ValueKind,
        raw_expr: TokenStream,
        abs_start: TokenStream,
        abs_end: TokenStream,
    ) -> TokenStream {
        match kind {
            ValueKind::Range | ValueKind::Local => quote! {
                if headers.#ident.is_none() {
                    headers.#ident = Some((#abs_start)..(#abs_end));
                }
            },
            ValueKind::Usize => Self::integer(ident, quote!(usize), &raw_expr),
            ValueKind::U64 => Self::integer(ident, quote!(u64), &raw_expr),
            ValueKind::Bool => quote! {
                if headers.#ident.is_none() {
                    let raw = #raw_expr;
                    let parsed = if raw.eq_ignore_ascii_case(b"true") || raw == b"1" {
                        true
                    } else if raw.eq_ignore_ascii_case(b"false") || raw == b"0" {
                        false
                    } else {
                        return Err(sark_core::error::Error::BadRequest(
                            "Invalid boolean field".into(),
                        ));
                    };
                    headers.#ident = Some(parsed);
                }
            },
            ValueKind::Custom => quote! {
                if headers.#ident.is_none() {
                    let value = sark::service::SliceValue::new(input, (#abs_start)..(#abs_end));
                    headers.#ident = Some(sark::service::FieldValue::parse_value(&value)?);
                }
            },
        }
    }

    fn integer(ident: &Ident, ty: TokenStream, raw_expr: &TokenStream) -> TokenStream {
        quote! {
            if headers.#ident.is_none() {
                let raw = #raw_expr;
                let mut value: #ty = 0;
                let mut seen = false;
                for &b in raw {
                    if !b.is_ascii_digit() {
                        return Err(sark_core::error::Error::BadRequest(
                            "Invalid integer header".into(),
                        ));
                    }
                    value = value
                        .checked_mul(10)
                        .and_then(|v| v.checked_add((b - b'0') as #ty))
                        .ok_or_else(|| {
                            sark_core::error::Error::BadRequest(
                                "Invalid integer header".into(),
                            )
                        })?;
                    seen = true;
                }
                if !seen {
                    return Err(sark_core::error::Error::BadRequest(
                        "Invalid integer header".into(),
                    ));
                }
                headers.#ident = Some(value);
            }
        }
    }
}

pub(crate) struct BytesMatch;

impl BytesMatch {
    pub(crate) fn exact(name_ident: &Ident, bytes: &[u8]) -> TokenStream {
        Self::build(name_ident, bytes, false)
    }

    fn fold(name_ident: &Ident, bytes: &[u8]) -> TokenStream {
        Self::build(name_ident, bytes, true)
    }

    fn build(name_ident: &Ident, bytes: &[u8], folded: bool) -> TokenStream {
        let chunk = format_ident!("__c");
        let mut checks = Vec::new();
        if folded && bytes.len() > 8 {
            let mut offsets = Vec::new();
            let mut offset = 0usize;
            while offset + 8 < bytes.len() {
                offsets.push(offset);
                offset += 8;
            }
            let tail_offset = bytes.len() - 8;
            if offsets.last().copied() != Some(tail_offset) {
                offsets.push(tail_offset);
            }
            for offset in offsets {
                checks.push(Self::chunk_check(&chunk, bytes, offset, 8, folded));
            }
            return Self::wrap(&chunk, name_ident, bytes.len(), checks);
        }
        let mut offset = 0usize;
        while offset + 8 <= bytes.len() {
            checks.push(Self::chunk_check(&chunk, bytes, offset, 8, folded));
            offset += 8;
        }
        if offset + 4 <= bytes.len() {
            checks.push(Self::chunk_check(&chunk, bytes, offset, 4, folded));
            offset += 4;
        }
        if offset + 2 <= bytes.len() {
            checks.push(Self::chunk_check(&chunk, bytes, offset, 2, folded));
            offset += 2;
        }
        if offset < bytes.len() {
            let byte = bytes[offset];
            if folded && Self::can_fold_or(byte) {
                checks.push(quote! { ((#chunk[#offset] | 0x20) == #byte) });
            } else {
                checks.push(quote! { #chunk[#offset] == #byte });
            }
        }
        Self::wrap(&chunk, name_ident, bytes.len(), checks)
    }

    fn chunk_check(
        chunk: &Ident,
        bytes: &[u8],
        offset: usize,
        width: usize,
        folded: bool,
    ) -> TokenStream {
        let c = &bytes[offset..offset + width];
        let end = offset + width;
        if folded {
            let mut mask_bytes = vec![0u8; width];
            for (idx, &b) in c.iter().enumerate() {
                if Self::can_fold_or(b) {
                    mask_bytes[idx] = 0x20;
                }
            }
            match width {
                8 => {
                    let word = u64::from_le_bytes(c.try_into().unwrap());
                    let mask = u64::from_le_bytes(mask_bytes.as_slice().try_into().unwrap());
                    quote! { (u64::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) | #mask) == #word }
                }
                4 => {
                    let word = u32::from_le_bytes(c.try_into().unwrap());
                    let mask = u32::from_le_bytes(mask_bytes.as_slice().try_into().unwrap());
                    quote! { (u32::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) | #mask) == #word }
                }
                2 => {
                    let word = u16::from_le_bytes(c.try_into().unwrap());
                    let mask = u16::from_le_bytes(mask_bytes.as_slice().try_into().unwrap());
                    quote! { (u16::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) | #mask) == #word }
                }
                _ => unreachable!(),
            }
        } else {
            match width {
                8 => {
                    let word = u64::from_le_bytes(c.try_into().unwrap());
                    quote! { u64::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) == #word }
                }
                4 => {
                    let word = u32::from_le_bytes(c.try_into().unwrap());
                    quote! { u32::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) == #word }
                }
                2 => {
                    let word = u16::from_le_bytes(c.try_into().unwrap());
                    quote! { u16::from_le_bytes(#chunk[#offset..#end].try_into().unwrap()) == #word }
                }
                _ => unreachable!(),
            }
        }
    }

    fn wrap(
        chunk: &Ident,
        name_ident: &Ident,
        len: usize,
        checks: Vec<TokenStream>,
    ) -> TokenStream {
        let len = proc_macro2::Literal::usize_unsuffixed(len);
        quote! {
            match #name_ident.first_chunk::<#len>() {
                Some(#chunk) => true #( && #checks )*,
                None => false,
            }
        }
    }

    fn can_fold_or(byte: u8) -> bool {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b':')
    }
}

type ProbeKey = (usize, u64, u64, u64);
type ActionRow = (Vec<u8>, Vec<u8>, TokenStream);

impl Emit {
    fn prefix_cases(action_specs: &[ActionSpec], unknown_miss: &TokenStream) -> TokenStream {
        let mut prefix_groups: BTreeMap<ProbeKey, Vec<ActionRow>> = BTreeMap::new();
        for spec in action_specs {
            let (probe_len, probe_word, probe_mask, probe_active, tail) =
                Self::probe_meta(&spec.bytes);
            prefix_groups
                .entry((probe_len, probe_word, probe_mask, probe_active))
                .or_default()
                .push((spec.bytes.clone(), tail, spec.body.clone()));
        }
        let match_mask = u64::from_le_bytes([0x20, 0x20, 0x20, 0x20, 0x20, 0xff, 0xff, 0xff]);
        let fold_mask = u64::from_le_bytes([0x20, 0x20, 0x20, 0x20, 0x20, 0x00, 0x00, 0x00]);
        let active_mask = u64::from_le_bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00]);
        let can_match = prefix_groups
            .keys()
            .all(|(probe_len, _, probe_mask, probe_active)| {
                *probe_len == 5 && *probe_mask == fold_mask && *probe_active == active_mask
            });
        let groups: Vec<_> = prefix_groups.into_iter().collect();

        if can_match {
            let mut match_arms = Vec::new();
            for ((probe_len, probe_word, _, _), group) in groups {
                let checks = Self::group_checks(probe_len, &group);
                let fallback_idx = probe_len.min(5);
                let probe_key = probe_word | match_mask;
                match_arms.push(quote! {
                    #probe_key => {
                        #( #checks )*
                        let idx = #fallback_idx;
                        #unknown_miss
                    }
                });
            }
            return quote! {
                let __probe_key = __probe_word | #match_mask;
                match __probe_key {
                    #( #match_arms, )*
                    _ => {
                        let idx = 0usize;
                        #unknown_miss
                    }
                }
            };
        }

        let mut cases: Vec<(u8, TokenStream, TokenStream)> = Vec::new();
        for ((probe_len, probe_word, probe_mask, probe_active), group) in groups {
            let priority = Self::prefix_priority(&group[0].0);
            let checks = Self::group_checks(probe_len, &group);
            let fallback_idx = probe_len.min(5);
            let cond = quote! { ((__probe_word | #probe_mask) & #probe_active) == #probe_word };
            let body = quote! {
                #( #checks )*
                let idx = #fallback_idx;
                #unknown_miss
            };
            cases.push((priority, cond, body));
        }
        cases.sort_by_key(|case| case.0);
        let mut iter = cases.into_iter();
        let Some((_, first_cond, first_body)) = iter.next() else {
            return quote! {
                let idx = 0usize;
                #unknown_miss
            };
        };
        let rest: Vec<_> = iter.collect();
        let rest_conds: Vec<_> = rest.iter().map(|case| case.1.clone()).collect();
        let rest_bodies: Vec<_> = rest.iter().map(|case| case.2.clone()).collect();
        quote! {
            if #first_cond {
                #first_body
            }
            #( else if #rest_conds {
                #rest_bodies
            } )*
            else {
                let idx = 0usize;
                #unknown_miss
            }
        }
    }

    fn group_checks(probe_len: usize, group: &[ActionRow]) -> Vec<TokenStream> {
        group
            .iter()
            .map(|(bytes, tail, body)| {
                let colon_idx = bytes.len();
                let total_len = colon_idx + 1;
                if tail.is_empty() {
                    quote! {
                        let colon_idx = #colon_idx;
                        #body
                    }
                } else {
                    let tail_ident = format_ident!("tail");
                    let cond = BytesMatch::fold(&tail_ident, tail);
                    quote! {
                        if rest.len() >= #total_len {
                            let #tail_ident = &rest[#probe_len..#total_len];
                            if #cond {
                                let colon_idx = #colon_idx;
                                #body
                            }
                        }
                    }
                }
            })
            .collect()
    }

    fn probe_meta(bytes: &[u8]) -> (usize, u64, u64, u64, Vec<u8>) {
        let mut full = Vec::with_capacity(bytes.len() + 1);
        full.extend_from_slice(bytes);
        full.push(b':');
        let probe_len = full.len().min(5);
        let mut probe = [0u8; 8];
        let mut fold = [0u8; 8];
        let mut active = [0u8; 8];
        for idx in 0..probe_len {
            let b = full[idx];
            probe[idx] = b;
            active[idx] = 0xff;
            if BytesMatch::can_fold_or(b) {
                fold[idx] = 0x20;
            }
        }
        let tail = if probe_len < full.len() {
            full[probe_len..].to_vec()
        } else {
            Vec::new()
        };
        (
            probe_len,
            u64::from_le_bytes(probe),
            u64::from_le_bytes(fold),
            u64::from_le_bytes(active),
            tail,
        )
    }

    fn prefix_priority(bytes: &[u8]) -> u8 {
        match bytes {
            b if b.starts_with(b"host") => 0,
            b if b.starts_with(b"conne") => 1,
            b if b.starts_with(b"x-ben") => 2,
            b if b.starts_with(b"conte") => 3,
            b if b.starts_with(b"trans") => 4,
            b if b.starts_with(b"expec") => 5,
            _ => 6,
        }
    }
}

impl Emit {
    pub(crate) fn scan() -> Result<TokenStream> {
        Ok(quote! {
            Ok(sark::sark_core::http::head::BytesScan::find_header_line_from(rest, 0))
        })
    }

    pub(crate) fn apply<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
        cfg: HeaderParserCfg,
    ) -> Result<TokenStream> {
        let plan = HeaderPlan::collect(entries)?;
        let skip_value = matches!(cfg.value, HeaderValueMode::Skip);
        let skip_apply = matches!(cfg.apply, HeaderApplyMode::Skip);
        if plan.is_empty() && !skip_value {
            return Ok(quote! {
                return sark::sark_core::http::head::apply_well_known_header(
                    input,
                    line,
                    line_start,
                    colon_idx,
                    pretrim_start,
                    pretrim_end,
                    scan,
                    flags,
                    scan_info,
                );
            });
        }
        let raw = format_ident!("__raw");
        let name_valid = Self::header_name_valid(&raw);
        let action_enum = format_ident!("__HeaderAction");
        let mut action_specs = Vec::new();
        for (idx, known) in KNOWN_HEADERS.iter().enumerate() {
            let capture = plan.known[idx].clone();
            let action = format_ident!("Known{}", known.suffix);
            let apply = known.apply_call();
            let arm = if let Some(field) = capture {
                let raw_expr = quote! { raw };
                let abs_start = quote! { line_start + value_start };
                let abs_end = quote! { line_start + value_end };
                let assign = Assign::emit(&field.ident, field.kind, raw_expr, abs_start, abs_end);
                let maybe_assign = if skip_apply {
                    TokenStream::new()
                } else {
                    quote! { #assign }
                };
                quote! {{
                    #apply
                    #maybe_assign
                    return Ok(());
                }}
            } else {
                quote! {{
                    #apply
                    return Ok(());
                }}
            };
            action_specs.push(ActionSpec {
                variant: action,
                bytes: known.bytes.to_vec(),
                body: arm,
            });
        }
        for field in &plan.custom {
            let action = format_ident!("Custom{}", field.slot);
            let raw_expr = quote! { raw };
            let abs_start = quote! { line_start + value_start };
            let abs_end = quote! { line_start + value_end };
            let assign = Assign::emit(&field.ident, field.kind, raw_expr, abs_start, abs_end);
            let maybe_assign = if skip_apply {
                TokenStream::new()
            } else {
                quote! { #assign }
            };
            action_specs.push(ActionSpec {
                variant: action.clone(),
                bytes: field.bytes.clone(),
                body: quote! {{
                    #maybe_assign
                    return Ok(());
                }},
            });
        }
        let action_select_arms = Self::action_select(&action_enum, &action_specs, true);
        let action_variants: Vec<_> = action_specs.iter().map(|spec| &spec.variant).collect();
        let action_arms: Vec<_> = action_specs
            .iter()
            .map(|spec| {
                let variant = &spec.variant;
                let body = &spec.body;
                quote! { #action_enum::#variant => #body }
            })
            .collect();
        Ok(quote! {
            if colon_idx == 0 {
                return Err(sark_core::error::Error::BadRequest("Invalid header name".into()));
            }
            let _ = scan_info;
            let name = &line[..colon_idx];
            for &#raw in name {
                if !(#name_valid) {
                    return Err(sark_core::error::Error::BadRequest("Invalid header name".into()));
                }
            }
            enum #action_enum {
                Unknown,
                #( #action_variants, )*
            }
            let mut action = #action_enum::Unknown;
            match name.len() {
                #( #action_select_arms )*
                _ => {}
            }
            if matches!(action, #action_enum::Unknown) {
                return Ok(());
            }
            if #skip_value {
                return Ok(());
            }

            let mut value_start = colon_idx + 1;
            let mut value_end = line.len();
            if let Some(start) = pretrim_start {
                value_start = start.min(line.len());
                value_end = pretrim_end.unwrap_or(line.len()).min(line.len());
            } else {
                while value_start < line.len() && (line[value_start] == b' ' || line[value_start] == b'\t') {
                    value_start += 1;
                }
                while value_end > value_start && (line[value_end - 1] == b' ' || line[value_end - 1] == b'\t') {
                    value_end -= 1;
                }
            }
            let raw = &line[value_start..value_end];
            match action {
                #( #action_arms )*
                #action_enum::Unknown => Ok(()),
            }
        })
    }

    pub(crate) fn contig<'a>(
        entries: impl IntoIterator<Item = (&'a Ident, Vec<u8>, &'a Type)>,
        cfg: HeaderParserCfg,
    ) -> Result<TokenStream> {
        let plan = HeaderPlan::collect(entries)?;
        let skip_value = matches!(cfg.value, HeaderValueMode::Skip);
        let skip_apply = matches!(cfg.apply, HeaderApplyMode::Skip);
        if plan.is_empty() && !skip_value {
            return Ok(quote! {
                return sark::sark_core::http::head::apply_well_known_header_contig(
                    rest,
                    scan,
                    flags,
                    &mut (),
                    header_count,
                    max_header_count,
                );
            });
        }
        let raw = format_ident!("__raw");
        let name_valid = Self::header_name_valid(&raw);
        if plan.custom.iter().any(|field| field.bytes.len() < 4) {
            return Err(syn::Error::new(
                Span::call_site(),
                "custom headers used in generated DFA must be at least 4 bytes long",
            ));
        }
        if plan.custom.len() > 59 {
            return Err(syn::Error::new(
                Span::call_site(),
                "too many headers for generated DFA; maximum is 59 custom headers",
            ));
        }

        let count_tail = quote! {
            if *header_count == max_header_count {
                return Err(sark::error::Error::BadRequest("Too many headers".into()));
            }
            *header_count += 1;
        };
        let trim_contig = quote! {
            let mut __value_idx = colon_idx + 1;
            while __value_idx < rest.len() && (rest[__value_idx] == b' ' || rest[__value_idx] == b'\t') {
                __value_idx += 1;
            }
            let value_start = __value_idx - (colon_idx + 1);
            let mut value_end = value_start;
            loop {
                if __value_idx >= rest.len() {
                    return Ok(None);
                }
                let __b = rest[__value_idx];
                if __b == b'\r' {
                    if __value_idx + 1 >= rest.len() {
                        return Ok(None);
                    }
                    if rest[__value_idx + 1] != b'\n' {
                        return Err(sark::error::Error::BadRequest("Invalid header value".into()));
                    }
                    break;
                }
                if __b == b'\n' {
                    return Err(sark::error::Error::BadRequest("Invalid header value".into()));
                }
                if __b != b' ' && __b != b'\t' {
                    value_end = __value_idx + 1 - (colon_idx + 1);
                }
                __value_idx += 1;
            }
            let tail_end = __value_idx - (colon_idx + 1);
        };
        let unknown_dispatch = quote! {
            let Some(__line_end) = sark::sark_core::http::head::BytesScan::find_crlf_from(rest, colon_idx + 1) else {
                return Ok(None);
            };
            #count_tail
            return Ok(Some(__line_end));
        };
        let unknown_miss = quote! {
            return sark::sark_core::http::head::unknown_line(rest, idx, &mut (), header_count, max_header_count);
        };
        let mut action_specs = Vec::new();
        for field in &plan.custom {
            let action = format_ident!("Custom{}", field.slot);
            let raw_expr = quote! { &rest[colon_idx + 1 + value_start..colon_idx + 1 + value_end] };
            let abs_start = quote! { line_start + colon_idx + 1 + value_start };
            let abs_end = quote! { line_start + colon_idx + 1 + value_end };
            let assign = Assign::emit(&field.ident, field.kind, raw_expr, abs_start, abs_end);
            let maybe_assign = if skip_apply {
                TokenStream::new()
            } else {
                quote! { #assign }
            };
            let body = if skip_value {
                unknown_dispatch.clone()
            } else {
                quote! {{
                    #trim_contig
                    #count_tail
                    let _ = (value_start, value_end);
                    #maybe_assign
                    return Ok(Some(colon_idx + 1 + tail_end));
                }}
            };
            action_specs.push(ActionSpec {
                variant: action.clone(),
                bytes: field.bytes.clone(),
                body,
            });
        }

        for (idx, known) in KNOWN_HEADERS.iter().enumerate() {
            let capture = plan.known[idx].clone();
            let action = format_ident!("Known{}", known.suffix);
            let body = if skip_value {
                unknown_dispatch.clone()
            } else {
                KnownKind::from_index(idx).build_contig_arm(
                    capture.as_ref(),
                    &count_tail,
                    skip_apply,
                )
            };
            action_specs.push(ActionSpec {
                variant: action,
                bytes: known.bytes.to_vec(),
                body,
            });
        }

        let prefix_detect = Self::prefix_cases(&action_specs, &unknown_miss);

        Ok(quote! {
            let colon_idx = 'name: {
                if rest.is_empty() {
                    return Ok(None);
                }
                if rest.len() < 8 {
                    let mut idx = 0usize;
                    loop {
                        if idx >= rest.len() {
                            return Ok(None);
                        }
                        let #raw = rest[idx];
                        if #raw == b':' {
                            if idx == 0 {
                                return Err(sark::error::Error::BadRequest("Invalid header name".into()));
                            }
                            break 'name idx;
                        }
                        if #raw == b'\r' {
                            if idx + 1 >= rest.len() {
                                return Ok(None);
                            }
                            if rest[idx + 1] == b'\n' {
                                if idx == 0 {
                                    return Ok(Some(0));
                                }
                                return Err(sark::error::Error::BadRequest("Invalid header name".into()));
                            }
                            return Err(sark::error::Error::BadRequest("Invalid header name".into()));
                        }
                        if !(#name_valid) {
                            return Err(sark::error::Error::BadRequest("Invalid header name".into()));
                        }
                        idx += 1;
                    }
                }

                let __probe_word = u64::from_le_bytes(rest[..8].try_into().unwrap());
                #prefix_detect
            };
            #unknown_dispatch
        })
    }

    fn header_name_valid(raw: &Ident) -> TokenStream {
        quote! {
            #raw.is_ascii_alphanumeric()
                || matches!(
                    #raw,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        }
    }

    fn action_select(
        action_enum: &Ident,
        action_specs: &[ActionSpec],
        canonical_name: bool,
    ) -> Vec<TokenStream> {
        Value::group_arms_by_length(action_specs.iter().map(|spec| {
            let lit = LitByteStr::new(spec.bytes.as_slice(), Span::call_site());
            let variant = &spec.variant;
            let cond = if canonical_name {
                quote! { name.eq_ignore_ascii_case(#lit) }
            } else {
                quote! { name == #lit }
            };
            (
                spec.bytes.len(),
                quote! {
                    if #cond {
                        action = #action_enum::#variant;
                    }
                },
            )
        }))
    }
}
