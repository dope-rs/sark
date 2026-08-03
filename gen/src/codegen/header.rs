#![allow(clippy::too_many_arguments)]

use std::collections::BTreeMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use sark_protocol::{KnownRequestHeadName, RequestHeadSemantic};
use syn::Result;

use super::value::{FieldPlan, FieldSpec};
use crate::util::ValueKind;

trait HeaderAssignment {
    fn assignment(
        &self,
        raw_expr: TokenStream,
        abs_start: TokenStream,
        abs_end: TokenStream,
    ) -> TokenStream;

    fn integer_assignment(&self, ty: TokenStream, raw_expr: &TokenStream) -> TokenStream;
}

impl HeaderAssignment for FieldSpec {
    fn assignment(
        &self,
        raw_expr: TokenStream,
        abs_start: TokenStream,
        abs_end: TokenStream,
    ) -> TokenStream {
        let ident = &self.ident;
        match self.kind {
            ValueKind::Range | ValueKind::Bytes => quote! {
                if headers.#ident.is_none() {
                    headers.#ident = Some((#abs_start)..(#abs_end));
                }
            },
            ValueKind::Usize => self.integer_assignment(quote!(usize), &raw_expr),
            ValueKind::U64 => self.integer_assignment(quote!(u64), &raw_expr),
            ValueKind::Bool => quote! {
                if headers.#ident.is_none() {
                    let raw = #raw_expr;
                    let parsed = if raw.eq_ignore_ascii_case(b"true") || raw == b"1" {
                        true
                    } else if raw.eq_ignore_ascii_case(b"false") || raw == b"0" {
                        false
                    } else {
                        return sark::service::HeaderLineOutcome::Bad;
                    };
                    headers.#ident = Some(parsed);
                }
            },
            ValueKind::Custom => quote! {
                if headers.#ident.is_none() {
                    let value = sark::service::SliceValue::new(input, (#abs_start)..(#abs_end));
                    let Ok(value) = sark::service::FieldValue::parse_value(&value) else {
                        return sark::service::HeaderLineOutcome::Bad;
                    };
                    headers.#ident = Some(value);
                }
            },
        }
    }

    fn integer_assignment(&self, ty: TokenStream, raw_expr: &TokenStream) -> TokenStream {
        let ident = &self.ident;
        quote! {
            if headers.#ident.is_none() {
                let raw = #raw_expr;
                let mut value: #ty = 0;
                let mut seen = false;
                for &b in raw {
                    if !b.is_ascii_digit() {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    let Some(next) = value
                        .checked_mul(10)
                        .and_then(|v| v.checked_add((b - b'0') as #ty))
                    else {
                        return sark::service::HeaderLineOutcome::Bad;
                    };
                    value = next;
                    seen = true;
                }
                if !seen {
                    return sark::service::HeaderLineOutcome::Bad;
                }
                headers.#ident = Some(value);
            }
        }
    }
}

struct HeaderPlan {
    known: Vec<Option<FieldSpec>>,
    custom: Vec<FieldSpec>,
}

impl HeaderPlan {
    fn collect(fields: &FieldPlan) -> Self {
        let mut known = vec![None; RequestHeadSemantic::HTTP1.len()];
        let mut custom = Vec::new();
        for mut field in fields.entries().iter().cloned() {
            field.bytes.make_ascii_lowercase();
            if let Some(known_idx) = RequestHeadSemantic::HTTP1
                .iter()
                .position(|known| known.wire_name() == field.bytes)
            {
                known[known_idx] = Some(field);
            } else {
                custom.push(field);
            }
        }
        Self { known, custom }
    }
}

pub(crate) struct HeaderEmitter {
    plan: HeaderPlan,
    full: bool,
}

impl HeaderEmitter {
    pub(crate) fn new(fields: &FieldPlan, full: bool) -> Self {
        Self {
            plan: HeaderPlan::collect(fields),
            full,
        }
    }
}

struct ActionSpec {
    bytes: Vec<u8>,
    prefix_body: TokenStream,
    short_body: Option<TokenStream>,
}

pub(crate) struct Contiguous {
    pub(crate) fast: TokenStream,
    pub(crate) ignored: TokenStream,
    pub(crate) unknown: TokenStream,
    pub(crate) short: TokenStream,
}

trait Http1KnownHead {
    fn header(self) -> TokenStream;

    fn build_contig_arm(
        self,
        capture: Option<&FieldSpec>,
        validate_tail: &TokenStream,
    ) -> TokenStream;
}

impl Http1KnownHead for RequestHeadSemantic {
    fn header(self) -> TokenStream {
        match self.known() {
            KnownRequestHeadName::Host => {
                quote!(sark::sark_core::http::head::KnownHeader::Host)
            }
            KnownRequestHeadName::Expect => {
                quote!(sark::sark_core::http::head::KnownHeader::Expect)
            }
            KnownRequestHeadName::Connection => {
                quote!(sark::sark_core::http::head::KnownHeader::Connection)
            }
            KnownRequestHeadName::ContentLength => {
                quote!(sark::sark_core::http::head::KnownHeader::ContentLength)
            }
            KnownRequestHeadName::TransferEncoding => {
                quote!(sark::sark_core::http::head::KnownHeader::TransferEncoding)
            }
            KnownRequestHeadName::AcceptEncoding => {
                quote!(sark::sark_core::http::head::KnownHeader::AcceptEncoding)
            }
            _ => unreachable!("non-HTTP/1 head semantic in HTTP/1 lowering"),
        }
    }

    fn build_contig_arm(
        self,
        capture: Option<&FieldSpec>,
        validate_tail: &TokenStream,
    ) -> TokenStream {
        let colon_idx = self.wire_name().len();
        let assignment = capture.map(|field| {
            let raw_expr = quote! {
                match rest.get(colon_idx + 1 + value_start..colon_idx + 1 + value_end) {
                    Some(raw) => raw,
                    None => return sark::service::HeaderLineOutcome::Bad,
                }
            };
            let abs_start = quote! { line_start + colon_idx + 1 + value_start };
            let abs_end = quote! { line_start + colon_idx + 1 + value_end };
            field.assignment(raw_expr, abs_start, abs_end)
        });
        let header = self.header();
        quote! {{
            let colon_idx = #colon_idx;
            let (tail_end, value_start, value_end) = match #header.scan_line(
                scan,
                flags,
                &rest[colon_idx + 1..],
            ) {
                Ok(Some(value)) => value,
                Ok(None) => return sark::service::HeaderLineOutcome::NeedMore,
                Err(_) => return sark::service::HeaderLineOutcome::Bad,
            };
            let _ = (value_start, value_end);
            #validate_tail
            #assignment
            return sark::service::HeaderLineOutcome::Complete(colon_idx + 1 + tail_end);
        }}
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BytesMatch {
    Exact,
    Folded,
}

impl BytesMatch {
    pub(crate) fn emit(self, name_ident: &Ident, bytes: &[u8]) -> TokenStream {
        self.build(name_ident, bytes)
    }

    fn build(self, name_ident: &Ident, bytes: &[u8]) -> TokenStream {
        let folded = matches!(self, Self::Folded);
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
                checks.push(self.chunk_check(&chunk, bytes, offset, 8));
            }
            return Self::wrap(&chunk, name_ident, bytes.len(), checks);
        }
        let mut offset = 0usize;
        while offset + 8 <= bytes.len() {
            checks.push(self.chunk_check(&chunk, bytes, offset, 8));
            offset += 8;
        }
        if offset + 4 <= bytes.len() {
            checks.push(self.chunk_check(&chunk, bytes, offset, 4));
            offset += 4;
        }
        if offset + 2 <= bytes.len() {
            checks.push(self.chunk_check(&chunk, bytes, offset, 2));
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

    fn chunk_check(self, chunk: &Ident, bytes: &[u8], offset: usize, width: usize) -> TokenStream {
        let c = &bytes[offset..offset + width];
        let end = offset + width;
        let indices: Vec<_> = (offset..end).collect();
        let read = match width {
            8 => quote! { u64::from_le_bytes([#(#chunk[#indices]),*]) },
            4 => quote! { u32::from_le_bytes([#(#chunk[#indices]),*]) },
            2 => quote! { u16::from_le_bytes([#(#chunk[#indices]),*]) },
            _ => return quote!(false),
        };
        let word = Self::little_endian_word(c);
        if matches!(self, Self::Folded) {
            let mut mask_bytes = vec![0u8; width];
            for (idx, &b) in c.iter().enumerate() {
                if Self::can_fold_or(b) {
                    mask_bytes[idx] = 0x20;
                }
            }
            let mask = Self::little_endian_word(&mask_bytes);
            quote! { ((#read as u64) | #mask) == #word }
        } else {
            quote! { (#read as u64) == #word }
        }
    }

    fn little_endian_word(bytes: &[u8]) -> u64 {
        bytes.iter().enumerate().fold(0u64, |word, (idx, byte)| {
            word | (u64::from(*byte) << (idx * 8))
        })
    }

    fn wrap(
        chunk: &Ident,
        name_ident: &Ident,
        len: usize,
        checks: Vec<TokenStream>,
    ) -> TokenStream {
        let len = proc_macro2::Literal::usize_unsuffixed(len);
        quote! {
            #name_ident.len() == #len
                && match #name_ident.first_chunk::<#len>() {
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

impl HeaderEmitter {
    fn prefix_cases<F>(&self, action_specs: &[ActionSpec], unknown_miss: &F) -> TokenStream
    where
        F: Fn(usize) -> TokenStream,
    {
        let mut prefix_groups: BTreeMap<ProbeKey, Vec<ActionRow>> = BTreeMap::new();
        for spec in action_specs {
            let (probe_len, probe_word, probe_mask, probe_active, tail) =
                self.probe_meta(&spec.bytes);
            prefix_groups
                .entry((probe_len, probe_word, probe_mask, probe_active))
                .or_default()
                .push((spec.bytes.clone(), tail, spec.prefix_body.clone()));
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
                let fallback_idx = probe_len.min(5);
                let probe_key = probe_word | match_mask;
                let miss = unknown_miss(fallback_idx);
                let body = self.group_body(probe_len, &group, &miss);
                match_arms.push(quote! {
                    #probe_key => {
                        #body
                    }
                });
            }
            let miss = unknown_miss(0);
            return quote! {
                let __probe_key = __probe_word | #match_mask;
                match __probe_key {
                    #( #match_arms, )*
                    _ => {
                        #miss
                    }
                }
            };
        }

        let mut cases: Vec<(u8, TokenStream, TokenStream)> = Vec::new();
        for ((probe_len, probe_word, probe_mask, probe_active), group) in groups {
            let priority = self.prefix_priority(&group[0].0);
            let fallback_idx = probe_len.min(5);
            let cond = quote! { ((__probe_word | #probe_mask) & #probe_active) == #probe_word };
            let miss = unknown_miss(fallback_idx);
            let body = self.group_body(probe_len, &group, &miss);
            cases.push((priority, cond, body));
        }
        cases.sort_by_key(|case| case.0);
        let mut iter = cases.into_iter();
        let Some((_, first_cond, first_body)) = iter.next() else {
            let miss = unknown_miss(0);
            return quote! {
                #miss
            };
        };
        let rest: Vec<_> = iter.collect();
        let rest_conds: Vec<_> = rest.iter().map(|case| case.1.clone()).collect();
        let rest_bodies: Vec<_> = rest.iter().map(|case| case.2.clone()).collect();
        let miss = unknown_miss(0);
        quote! {
            if #first_cond {
                #first_body
            }
            #( else if #rest_conds {
                #rest_bodies
            } )*
            else {
                #miss
            }
        }
    }

    fn group_body(&self, probe_len: usize, group: &[ActionRow], miss: &TokenStream) -> TokenStream {
        if let [(_, tail, body)] = group
            && tail.is_empty()
        {
            return body.clone();
        }
        let checks = self.group_checks(probe_len, group);
        quote! {
            #( #checks )*
            #miss
        }
    }

    fn group_checks(&self, probe_len: usize, group: &[ActionRow]) -> Vec<TokenStream> {
        group
            .iter()
            .map(|(bytes, tail, body)| {
                let colon_idx = bytes.len();
                let total_len = colon_idx + 1;
                if tail.is_empty() {
                    body.clone()
                } else {
                    let tail_ident = format_ident!("tail");
                    let cond = BytesMatch::Folded.emit(&tail_ident, tail);
                    quote! {
                        if rest.len() >= #total_len {
                            let #tail_ident = &rest[#probe_len..#total_len];
                            if #cond {
                                #body
                            }
                        }
                    }
                }
            })
            .collect()
    }

    fn probe_meta(&self, bytes: &[u8]) -> (usize, u64, u64, u64, Vec<u8>) {
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

    fn prefix_priority(&self, bytes: &[u8]) -> u8 {
        match bytes {
            b if b.starts_with(RequestHeadSemantic::HOST.wire_name()) => 0,
            b if b.starts_with(b"conne") => 1,
            b if b.starts_with(b"x-ben") => 2,
            b if b.starts_with(b"conte") => 3,
            b if b.starts_with(b"trans") => 4,
            b if b.starts_with(b"expec") => 5,
            _ => 6,
        }
    }
}

impl HeaderEmitter {
    pub(crate) fn contiguous(&self) -> Result<Contiguous> {
        let plan = &self.plan;
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

        let max_header_line_bytes = quote!(sark::sark_core::http::head::MAX_HEADER_LINE_BYTES);
        let line_too_long = quote! {
            return sark::service::HeaderLineOutcome::Bad;
        };
        let capped_rest = quote! {
            let __capped = if rest.len() > #max_header_line_bytes {
                &rest[..#max_header_line_bytes]
            } else {
                rest
            };
        };
        let validate_tail = quote! {
            if colon_idx + 1 + tail_end
                > #max_header_line_bytes
            {
                #line_too_long
            }
        };
        let scan_value = quote! {
            #capped_rest
            let __line_end = match sark::sark_core::http::scan::scan_header_value(
                __capped,
                colon_idx + 1,
            ) {
                sark::sark_core::http::scan::HeaderValueOutcome::Found { pos } => pos,
                sark::sark_core::http::scan::HeaderValueOutcome::Invalid => {
                    return sark::service::HeaderLineOutcome::Bad;
                }
                sark::sark_core::http::scan::HeaderValueOutcome::None
                    if rest.len() > #max_header_line_bytes =>
                {
                    #line_too_long
                }
                sark::sark_core::http::scan::HeaderValueOutcome::None => {
                    return sark::service::HeaderLineOutcome::NeedMore;
                }
            };
        };
        let trim_contig = quote! {
            let __limit = rest.len().min(#max_header_line_bytes);
            let mut __value_idx = colon_idx + 1;
            while __value_idx < __limit
                && (rest[__value_idx] == b' ' || rest[__value_idx] == b'\t')
            {
                __value_idx += 1;
            }
            let value_start = __value_idx - (colon_idx + 1);
            let mut value_end = value_start;
            loop {
                if __value_idx >= __limit {
                    if rest.len() > #max_header_line_bytes {
                        #line_too_long
                    }
                    return sark::service::HeaderLineOutcome::NeedMore;
                }
                let __byte = rest[__value_idx];
                if __byte == b'\r' {
                    if __value_idx + 1 >= rest.len() {
                        return sark::service::HeaderLineOutcome::NeedMore;
                    }
                    if rest[__value_idx + 1] != b'\n' {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    break;
                }
                if (__byte < 0x20 && __byte != b'\t') || __byte == 0x7f {
                    return sark::service::HeaderLineOutcome::Bad;
                }
                if __byte != b' ' && __byte != b'\t' {
                    value_end = __value_idx + 1 - (colon_idx + 1);
                }
                __value_idx += 1;
            }
            let tail_end = __value_idx - (colon_idx + 1);
        };
        let unknown_dispatch = quote! {
            #scan_value
            return sark::service::HeaderLineOutcome::Complete(__line_end);
        };
        let short_unknown = quote! {
            #capped_rest
            if __capped.len() < 16 {
                let mut __cursor = idx;
                let colon_idx = loop {
                    if __cursor >= __capped.len() {
                        return sark::service::HeaderLineOutcome::NeedMore;
                    }
                    let byte = __capped[__cursor];
                    if byte == b':' {
                        if __cursor == 0 {
                            return sark::service::HeaderLineOutcome::Bad;
                        }
                        break __cursor;
                    }
                    if byte == b'\r' {
                        if __cursor + 1 >= __capped.len() {
                            return sark::service::HeaderLineOutcome::NeedMore;
                        }
                        if __capped[__cursor + 1] == b'\n' && __cursor == 0 {
                            return sark::service::HeaderLineOutcome::Complete(0);
                        }
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    if !sark::sark_core::http::head::is_header_name_byte(byte) {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    __cursor += 1;
                };
                __cursor = colon_idx + 1;
                loop {
                    if __cursor >= __capped.len() {
                        return sark::service::HeaderLineOutcome::NeedMore;
                    }
                    let byte = __capped[__cursor];
                    if byte == b'\r' {
                        if __cursor + 1 >= __capped.len() {
                            return sark::service::HeaderLineOutcome::NeedMore;
                        }
                        if __capped[__cursor + 1] != b'\n' {
                            return sark::service::HeaderLineOutcome::Bad;
                        }
                        return sark::service::HeaderLineOutcome::Complete(__cursor);
                    }
                    if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    __cursor += 1;
                }
            }
        };
        let unknown_name = quote! {
            #capped_rest
            let (name_end, name_term) =
                match sark::sark_core::http::scan::scan_header_name(__capped, idx) {
                    sark::sark_core::http::scan::HeaderNameOutcome::Found {
                        pos,
                        byte,
                    } => (pos, byte),
                    sark::sark_core::http::scan::HeaderNameOutcome::Invalid => {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    sark::sark_core::http::scan::HeaderNameOutcome::None
                        if rest.len() > #max_header_line_bytes =>
                    {
                        #line_too_long
                    }
                    sark::sark_core::http::scan::HeaderNameOutcome::None => {
                        return sark::service::HeaderLineOutcome::NeedMore;
                    }
            };
            if name_term != b':' {
                if name_end + 1 >= __capped.len() {
                    return if rest.len() > #max_header_line_bytes {
                        sark::service::HeaderLineOutcome::Bad
                    } else {
                        sark::service::HeaderLineOutcome::NeedMore
                    };
                }
                if __capped[name_end + 1] == b'\n' && name_end == 0 {
                    return sark::service::HeaderLineOutcome::Complete(0);
                }
                return sark::service::HeaderLineOutcome::Bad;
            }
            if name_end == 0 {
                return sark::service::HeaderLineOutcome::Bad;
            }
            let colon_idx = name_end;
        };
        let unknown_call = |idx: usize| {
            quote! {
                return Self::__sark_scan_header_line_unknown::<#idx>(rest);
            }
        };
        let ignored_call = |colon_idx: usize| {
            quote! {
                return Self::__sark_scan_header_value_ignored::<#colon_idx>(rest);
            }
        };
        let mut action_specs = Vec::new();
        for field in &plan.custom {
            let colon_idx = field.bytes.len();
            let raw_expr = quote! {
                match rest.get(colon_idx + 1 + value_start..colon_idx + 1 + value_end) {
                    Some(raw) => raw,
                    None => return sark::service::HeaderLineOutcome::Bad,
                }
            };
            let abs_start = quote! { line_start + colon_idx + 1 + value_start };
            let abs_end = quote! { line_start + colon_idx + 1 + value_end };
            let assign = field.assignment(raw_expr, abs_start, abs_end);
            let body = quote! {{
                let colon_idx = #colon_idx;
                #trim_contig
                #validate_tail
                let _ = (value_start, value_end);
                #assign
                return sark::service::HeaderLineOutcome::Complete(
                    colon_idx + 1 + tail_end,
                );
            }};
            action_specs.push(ActionSpec {
                bytes: field.bytes.clone(),
                prefix_body: body.clone(),
                short_body: Some(body),
            });
        }

        let specialize_ignored = plan.custom.len() < 8;
        for (idx, known) in RequestHeadSemantic::HTTP1.iter().copied().enumerate() {
            let capture = plan.known[idx].as_ref();
            let semantic = known.http1_mandatory() || self.full || capture.is_some();
            let (prefix_body, short_body) = if semantic {
                let body = known.build_contig_arm(capture, &validate_tail);
                (body.clone(), Some(body))
            } else if known.known() == KnownRequestHeadName::AcceptEncoding {
                let semantic_body = known.build_contig_arm(None, &validate_tail);
                let ignored = ignored_call(known.wire_name().len());
                (
                    quote! {{
                        if __PARSE_ACCEPT_ENCODING {
                            #semantic_body
                        }
                        #ignored
                    }},
                    Some(quote! {{
                        if __PARSE_ACCEPT_ENCODING {
                            #semantic_body
                        }
                    }}),
                )
            } else if specialize_ignored {
                (ignored_call(known.wire_name().len()), None)
            } else {
                continue;
            };
            action_specs.push(ActionSpec {
                bytes: known.wire_name().to_vec(),
                prefix_body,
                short_body,
            });
        }

        for bytes in [b"accept".as_slice(), b"user-agent".as_slice()] {
            if plan.custom.iter().any(|field| field.bytes == bytes) {
                continue;
            }
            action_specs.push(ActionSpec {
                bytes: bytes.to_vec(),
                prefix_body: ignored_call(bytes.len()),
                short_body: None,
            });
        }

        let prefix_detect = self.prefix_cases(&action_specs, &unknown_call);
        let short_dispatch: Vec<TokenStream> = action_specs
            .iter()
            .filter_map(|spec| {
                let body = spec.short_body.as_ref()?;
                let matches = BytesMatch::Folded.emit(&format_ident!("__name"), &spec.bytes);
                Some(quote! {
                    if #matches {
                        #body
                    }
                })
            })
            .collect();
        let short_miss = quote! {
            return Self::__sark_scan_header_line_short::<__PARSE_ACCEPT_ENCODING>(
                headers,
                input,
                rest,
                line_start,
                scan,
                flags,
            );
        };

        Ok(Contiguous {
            fast: quote! {
                let Some(__probe) = rest.first_chunk::<8>() else {
                    if rest.first() == Some(&b'\r') {
                        if rest.len() == 1 {
                            return sark::service::HeaderLineOutcome::NeedMore;
                        }
                        return if rest[1] == b'\n' {
                            sark::service::HeaderLineOutcome::Complete(0)
                        } else {
                            sark::service::HeaderLineOutcome::Bad
                        };
                    }
                    #short_miss
                };
                let __probe_word = u64::from_le_bytes(*__probe);
                #prefix_detect
            },
            ignored: quote! {
                let value_offset = __COLON_IDX + 1;
                let value_rest = &rest[value_offset..];
                let __value_limit = #max_header_line_bytes - value_offset;
                let __capped = if value_rest.len() > __value_limit {
                    &value_rest[..__value_limit]
                } else {
                    value_rest
                };
                let scan_start = if let Some(__lane) = __capped.first_chunk::<16>() {
                    match sark::sark_core::http::scan::scan_header_value(__lane, 0) {
                        sark::sark_core::http::scan::HeaderValueOutcome::Found { pos } => {
                            return sark::service::HeaderLineOutcome::Complete(value_offset + pos);
                        }
                        sark::sark_core::http::scan::HeaderValueOutcome::Invalid => {
                            return sark::service::HeaderLineOutcome::Bad;
                        }
                        sark::sark_core::http::scan::HeaderValueOutcome::None
                            if __lane[15] == b'\r' => 15,
                        sark::sark_core::http::scan::HeaderValueOutcome::None => 16,
                    }
                } else {
                    let mut pos = 0usize;
                    loop {
                        if pos >= __capped.len() {
                            if value_rest.len() > __value_limit {
                                #line_too_long
                            }
                            return sark::service::HeaderLineOutcome::NeedMore;
                        }
                        let byte = __capped[pos];
                        if byte == b'\r' {
                            if pos + 1 >= __capped.len() {
                                return sark::service::HeaderLineOutcome::NeedMore;
                            }
                            if __capped[pos + 1] != b'\n' {
                                return sark::service::HeaderLineOutcome::Bad;
                            }
                            return sark::service::HeaderLineOutcome::Complete(value_offset + pos);
                        }
                        if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
                            return sark::service::HeaderLineOutcome::Bad;
                        }
                        pos += 1;
                    }
                };
                let __value_end = match sark::sark_core::http::scan::scan_header_value(
                    __capped,
                    scan_start,
                ) {
                    sark::sark_core::http::scan::HeaderValueOutcome::Found { pos } => pos,
                    sark::sark_core::http::scan::HeaderValueOutcome::Invalid => {
                        return sark::service::HeaderLineOutcome::Bad;
                    }
                    sark::sark_core::http::scan::HeaderValueOutcome::None
                        if value_rest.len() > __value_limit =>
                    {
                        #line_too_long
                    }
                    sark::sark_core::http::scan::HeaderValueOutcome::None => {
                        return sark::service::HeaderLineOutcome::NeedMore;
                    }
                };
                sark::service::HeaderLineOutcome::Complete(value_offset + __value_end)
            },
            unknown: quote! {
                let idx = __START;
                #short_unknown
                #unknown_name
                #unknown_dispatch
            },
            short: quote! {
                let idx = 0usize;
                #unknown_name
                let __name = &rest[..colon_idx];
                #( #short_dispatch )*
                #unknown_dispatch
            },
        })
    }
}
