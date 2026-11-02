use std::collections::BTreeMap;

use crate::{
    macros::tokens::local,
    request::{header::bytes, values},
    runtime::paths::Paths,
};

pub(in crate::request) fn build(
    paths: &Paths,
    fields: &values::FieldPlan,
    request: &proc_macro2::Ident,
    slot: &proc_macro2::Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_core = &paths.core;
    use quote::quote;
    let [prefix, rest, probe, word] = ["prefix", "rest", "__probe", "__probe_word"].map(local);

    let plan = Plan::new(fields, slot)?;
    let dispatch = plan.cases();
    Ok(quote! {
        impl #ns_core::route::field::header::Policy<#request<'static>> for #slot {
            fn prefix<__P>(#prefix: __P) -> __P::Output
            where
                __P: #ns_core::request::field::Prefix<Self>,
            {
                let #rest = #prefix.rest();
                let ::core::option::Option::Some(#probe) = #rest.first_chunk::<8>() else {
                    return #prefix.short();
                };
                let #word = ::core::primitive::u64::from_le_bytes(*#probe);
                #dispatch
            }
        }
    })
}

struct Plan {
    buckets: BTreeMap<u8, BTreeMap<Probe, Vec<Case>>>,
}

impl Plan {
    fn new(fields: &values::FieldPlan, kind: &proc_macro2::Ident) -> syn::Result<Self> {
        let mut buckets: BTreeMap<u8, BTreeMap<Probe, Vec<Case>>> = BTreeMap::new();
        for entry in fields.entries() {
            let (first, probe, case) = Case::new(entry, kind)?;
            buckets
                .entry(first)
                .or_default()
                .entry(probe)
                .or_default()
                .push(case);
        }
        Ok(Self { buckets })
    }

    fn cases(self) -> proc_macro2::TokenStream {
        let word = local("__probe_word");
        let mut arms = Vec::new();
        for (first, groups) in self.buckets {
            let groups = groups
                .into_iter()
                .map(|(probe, cases)| Group { probe, cases })
                .collect();
            let body = Self::bucket(groups);
            arms.push(quote::quote! {
                #first => {
                    #body
                }
            });
        }
        let fallback = Self::miss(0);
        quote::quote! {
            match (#word as ::core::primitive::u8) | 0x20 {
                #( #arms, )*
                _ => {
                    #fallback
                }
            }
        }
    }

    fn bucket(groups: Vec<Group>) -> proc_macro2::TokenStream {
        let mut layouts: BTreeMap<Layout, Vec<Group>> = BTreeMap::new();
        for group in groups {
            layouts
                .entry(Layout::from_probe(group.probe))
                .or_default()
                .push(group);
        }
        let mut layers: Vec<Layer> = layouts
            .into_iter()
            .map(|(layout, groups)| Layer { layout, groups })
            .collect();
        layers.sort_by_key(|layer| layer.layout.active.count_ones());
        Self::layers(&layers)
    }

    fn layers(layers: &[Layer]) -> proc_macro2::TokenStream {
        let [key, word] = ["__probe_key", "__probe_word"].map(local);
        let Some((layer, rest)) = layers.split_first() else {
            return Self::miss(0);
        };
        let mask = layer.layout.mask;
        let active = layer.layout.active;
        let mut arms = Vec::new();
        for group in &layer.groups {
            let word = group.probe.word;
            let fallback = Self::miss(group.probe.len);
            let body = group.emit(&fallback);
            arms.push(quote::quote! {
                #word => {
                    #body
                }
            });
        }
        let fallback = Self::layers(rest);
        quote::quote! {
            let #key = (#word | #mask) & #active;
            match #key {
                #( #arms, )*
                _ => {
                    #fallback
                }
            }
        }
    }

    fn miss(start: usize) -> proc_macro2::TokenStream {
        let prefix = local("prefix");
        quote::quote! { return #prefix.miss(#start); }
    }
}

struct Case {
    len: usize,
    tail: Vec<u8>,
    body: proc_macro2::TokenStream,
}

struct Group {
    probe: Probe,
    cases: Vec<Case>,
}

impl Case {
    fn new(entry: &values::FieldSpec, kind: &proc_macro2::Ident) -> syn::Result<(u8, Probe, Self)> {
        let Some(first) = entry.bytes.first().copied() else {
            return Err(syn::Error::new_spanned(
                &entry.ident,
                "request header name must not be empty",
            ));
        };
        let variant = &entry.variant;
        let len = entry.bytes.len();
        let (probe, tail) = Probe::new(&entry.bytes);
        let prefix = local("prefix");
        let body = quote::quote! { return #prefix.user(#kind::#variant, #len); };
        Ok((
            first.to_ascii_lowercase() | 0x20,
            probe,
            Self { len, tail, body },
        ))
    }
}

impl Group {
    fn emit(&self, miss: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        if let [case] = self.cases.as_slice()
            && case.tail.is_empty()
        {
            return case.body.clone();
        }
        let checks = self.checks();
        quote::quote! {
            #( #checks )*
            #miss
        }
    }

    fn checks(&self) -> Vec<proc_macro2::TokenStream> {
        let rest = local("rest");
        self.cases
            .iter()
            .map(|case| {
                let total = case.len + 1;
                if case.tail.is_empty() {
                    case.body.clone()
                } else {
                    let ident = local("tail");
                    let cond = bytes::Case::Folded.emit(&ident, &case.tail);
                    let len = self.probe.len;
                    let body = &case.body;
                    quote::quote! {
                        if let ::core::option::Option::Some(#ident) = #rest.get(#len..#total) {
                            if #cond {
                                #body
                            }
                        }
                    }
                }
            })
            .collect()
    }
}

struct Layer {
    layout: Layout,
    groups: Vec<Group>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Probe {
    len: usize,
    word: u64,
    mask: u64,
    active: u64,
}

impl Probe {
    fn new(bytes: &[u8]) -> (Self, Vec<u8>) {
        let total = bytes.len() + 1;
        let len = total.min(8);
        let mut probe = [0u8; 8];
        let mut fold = [0u8; 8];
        let mut active = [0u8; 8];
        for index in 0..len {
            let byte = match bytes.get(index) {
                Some(byte) => *byte,
                None => b':',
            }
            .to_ascii_lowercase();
            probe[index] = byte;
            active[index] = 0xff;
            if byte.is_ascii_lowercase() {
                fold[index] = 0x20;
            }
        }
        let tail = if len < total {
            let mut tail = Vec::with_capacity(total - len);
            tail.extend(bytes[len..].iter().map(|byte| byte.to_ascii_lowercase()));
            tail.push(b':');
            tail
        } else {
            Vec::new()
        };
        (
            Self {
                len,
                word: u64::from_le_bytes(probe),
                mask: u64::from_le_bytes(fold),
                active: u64::from_le_bytes(active),
            },
            tail,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Layout {
    mask: u64,
    active: u64,
}

impl Layout {
    fn from_probe(probe: Probe) -> Self {
        Self {
            mask: probe.mask,
            active: probe.active,
        }
    }
}
