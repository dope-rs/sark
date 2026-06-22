use std::collections::BTreeMap;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitByteStr;

use super::Method;

pub(crate) struct StaticRoute {
    pub(crate) method: Method,
    pub(crate) path: Vec<u8>,
    pub(crate) body: TokenStream,
}

impl StaticRoute {
    pub(crate) fn compile(routes: Vec<Self>) -> TokenStream {
        if routes.is_empty() {
            return TokenStream::new();
        }
        let mut by_method: BTreeMap<u8, Vec<Self>> = BTreeMap::new();
        for r in routes {
            by_method.entry(r.method.ord()).or_default().push(r);
        }
        let arms: Vec<TokenStream> = by_method
            .into_values()
            .map(|group| {
                let key = group[0].method.key_token();
                let len_tree = Self::build_len_tree(group);
                quote! { #key => { #len_tree } }
            })
            .collect();
        quote! {
            match __method {
                #( #arms )*
                _ => {}
            }
        }
    }

    fn build_len_tree(group: Vec<Self>) -> TokenStream {
        let mut by_len: BTreeMap<usize, Vec<Self>> = BTreeMap::new();
        for r in group {
            by_len.entry(r.path.len()).or_default().push(r);
        }
        let arms: Vec<TokenStream> = by_len
            .into_iter()
            .map(|(len, sub)| {
                let byte_tree = Self::build_byte_tree(sub);
                quote! { #len => { #byte_tree } }
            })
            .collect();
        quote! {
            match __path.len() {
                #( #arms )*
                _ => {}
            }
        }
    }

    fn build_byte_tree(routes: Vec<Self>) -> TokenStream {
        let n = routes.len();
        if n <= 1 {
            return Self::confirm_chain(routes.iter());
        }
        if n > 64 {
            return Self::build_byte_tree_greedy(routes);
        }
        let len = routes[0].path.len();
        let full: u64 = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let mut memo: BTreeMap<u64, (u64, Plan)> = BTreeMap::new();
        Plan::optimal(full, &routes, len, &mut memo);
        Plan::emit(full, &routes, &memo)
    }

    fn build_byte_tree_greedy(routes: Vec<Self>) -> TokenStream {
        if routes.len() <= 1 {
            return Self::confirm_chain(routes.iter());
        }
        match Self::pick_byte(&routes) {
            Some(k) => {
                let mut by_byte: BTreeMap<u8, Vec<Self>> = BTreeMap::new();
                for r in routes {
                    by_byte.entry(r.path[k]).or_default().push(r);
                }
                let arms: Vec<TokenStream> = by_byte
                    .into_iter()
                    .map(|(b, sub)| {
                        let sub_tree = Self::build_byte_tree_greedy(sub);
                        quote! { #b => { #sub_tree } }
                    })
                    .collect();
                quote! {
                    match __path[#k] {
                        #( #arms )*
                        _ => {}
                    }
                }
            }
            None => Self::confirm_chain(routes.iter()),
        }
    }

    fn confirm_chain<'r>(routes: impl Iterator<Item = &'r Self>) -> TokenStream {
        let ifs: Vec<TokenStream> = routes
            .map(|r| {
                let lit = LitByteStr::new(&r.path, Span::call_site());
                let body = &r.body;
                quote! { if __path == #lit { #body } }
            })
            .collect();
        quote! { #( #ifs )* }
    }

    fn pick_byte(routes: &[Self]) -> Option<usize> {
        let len = routes[0].path.len();
        let mut best: Option<(usize, usize)> = None;
        for k in 1..len {
            let mut counts: BTreeMap<u8, usize> = BTreeMap::new();
            for r in routes {
                *counts.entry(r.path[k]).or_default() += 1;
            }
            if counts.len() < 2 {
                continue;
            }
            let max = *counts.values().max().unwrap();
            if best.is_none_or(|(_, best_max)| max < best_max) {
                best = Some((k, max));
            }
        }
        best.map(|(k, _)| k)
    }
}

#[derive(Clone, Copy)]
enum Plan {
    Leaf,
    Split(usize),
}

impl Plan {
    fn optimal(
        mask: u64,
        routes: &[StaticRoute],
        len: usize,
        memo: &mut BTreeMap<u64, (u64, Self)>,
    ) -> u64 {
        if let Some(&(cost, _)) = memo.get(&mask) {
            return cost;
        }
        let mut best = (mask.count_ones() as u64, Self::Leaf);
        for k in 1..len {
            let mut groups: BTreeMap<u8, u64> = BTreeMap::new();
            for (i, route) in routes.iter().enumerate() {
                if mask & (1u64 << i) != 0 {
                    *groups.entry(route.path[k]).or_default() |= 1u64 << i;
                }
            }
            if groups.len() < 2 {
                continue;
            }
            let mut worst_child = 0;
            for &sub in groups.values() {
                worst_child = worst_child.max(Self::optimal(sub, routes, len, memo));
            }
            let cost = 1 + worst_child;
            if cost < best.0 {
                best = (cost, Self::Split(k));
            }
        }
        memo.insert(mask, best);
        best.0
    }

    fn emit(mask: u64, routes: &[StaticRoute], memo: &BTreeMap<u64, (u64, Self)>) -> TokenStream {
        if let Some((_, Self::Split(k))) = memo.get(&mask).copied() {
            let mut groups: BTreeMap<u8, u64> = BTreeMap::new();
            for (i, route) in routes.iter().enumerate() {
                if mask & (1u64 << i) != 0 {
                    *groups.entry(route.path[k]).or_default() |= 1u64 << i;
                }
            }
            let arms: Vec<TokenStream> = groups
                .into_iter()
                .map(|(b, sub)| {
                    let sub_tree = Self::emit(sub, routes, memo);
                    quote! { #b => { #sub_tree } }
                })
                .collect();
            return quote! {
                match __path[#k] {
                    #( #arms )*
                    _ => {}
                }
            };
        }
        StaticRoute::confirm_chain(
            (0..routes.len())
                .filter(|&i| mask & (1u64 << i) != 0)
                .map(|i| &routes[i]),
        )
    }
}
