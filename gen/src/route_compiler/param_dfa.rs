use std::collections::BTreeMap;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::LitByteStr;

use super::{Method, Seg};

pub(crate) struct ParamRoute {
    pub(crate) method: Method,
    pub(crate) segs: Vec<Seg>,
    pub(crate) body: TokenStream,
}

impl ParamRoute {
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
                let mut root = Node::new();
                for r in group {
                    root.insert(&r.segs, r.body);
                }
                let trie = root.emit(quote!(0usize), 0, 0);
                quote! { #key => { #trie } }
            })
            .collect();
        quote! {
            match ctx.method_key {
                #( #arms )*
                _ => {}
            }
        }
    }
}

struct Node {
    literals: BTreeMap<Vec<u8>, Node>,
    param: Option<Box<Node>>,
    accept: Option<TokenStream>,
}

impl Node {
    fn new() -> Self {
        Self {
            literals: BTreeMap::new(),
            param: None,
            accept: None,
        }
    }

    fn insert(&mut self, segs: &[Seg], body: TokenStream) {
        match segs.split_first() {
            None => {
                if self.accept.is_none() {
                    self.accept = Some(body);
                }
            }
            Some((Seg::Literal(bytes), rest)) => {
                self.literals
                    .entry(bytes.clone())
                    .or_insert_with(Node::new)
                    .insert(rest, body);
            }
            Some((Seg::Param, rest)) => {
                self.param
                    .get_or_insert_with(|| Box::new(Node::new()))
                    .insert(rest, body);
            }
        }
    }

    fn emit(&self, cur: TokenStream, depth: usize, params: usize) -> TokenStream {
        let nx = format_ident!("__nx{}", depth);

        let none_arm = match &self.accept {
            Some(body) => quote! { { #body } },
            None => quote! {},
        };

        if self.param.is_none() {
            let lit_arms: Vec<TokenStream> = self
                .literals
                .iter()
                .map(|(bytes, child)| {
                    let lit = LitByteStr::new(bytes, proc_macro2::Span::call_site());
                    let sub = child.emit(quote!(#nx), depth + 1, params);
                    quote! {
                        if let ::std::option::Option::Some(#nx) =
                            sark::service::PathProbe::probe_literal(
                                &ctx.slice_path, #cur, #lit,
                            )
                        {
                            #sub
                        }
                    }
                })
                .collect();
            return quote! {
                if #cur >= sark::service::PathProbe::len(&ctx.slice_path) {
                    #none_arm
                } else {
                    #( #lit_arms )*
                }
            };
        }

        let s = format_ident!("__s{}", depth);
        let e = format_ident!("__e{}", depth);

        let lit_arms: Vec<TokenStream> = self
            .literals
            .iter()
            .map(|(bytes, child)| {
                let lit = LitByteStr::new(bytes, proc_macro2::Span::call_site());
                let sub = child.emit(quote!(#nx), depth + 1, params);
                quote! {
                    if sark::service::PathProbe::eq_range(
                        &ctx.slice_path, #s, #e, #lit,
                    ) {
                        #sub
                    }
                }
            })
            .collect();

        let param_arm = match &self.param {
            Some(child) => {
                let cap = format_ident!("__cap{}", params);
                let sub = child.emit(quote!(#nx), depth + 1, params + 1);
                quote! {
                    {
                        let #cap = sark::service::PathCapture::new(#s, #e);
                        #sub
                    }
                }
            }
            None => quote! {},
        };

        quote! {
            match sark::service::PathProbe::next_seg(&ctx.slice_path, #cur) {
                ::std::option::Option::None => {
                    #none_arm
                }
                ::std::option::Option::Some((#s, #e, #nx)) => {
                    #( #lit_arms )*
                    #param_arm
                }
            }
        }
    }
}
