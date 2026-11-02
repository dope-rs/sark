use std::collections;

use crate::runtime::paths::Paths;

#[derive(Default)]
pub(super) struct Radix {
    terminal: Option<usize>,
    children: collections::BTreeMap<u8, Radix>,
}

struct Node {
    prefix: Vec<u8>,
    terminal: Option<usize>,
    children: Vec<(u8, usize)>,
}

impl Radix {
    pub(super) fn insert(&mut self, literal: &[u8], state: usize) {
        let mut node = self;
        for &byte in literal {
            node = node.children.entry(byte).or_default();
        }
        node.terminal = Some(state);
    }

    pub(super) fn emit_scan(
        &self,
        paths: &Paths,
        generics: &crate::types::Generics,
        fallback: Option<usize>,
    ) -> proc_macro2::TokenStream {
        use quote::quote;
        let [v_prefix, v_tail] = generics.locals(["__prefix", "__tail"]);
        let [v_rtail] = generics.locals(["__route_tail"]);
        let [v_node] = generics.locals(["__literal_node"]);
        let [v_rbyte, v_found] = generics.locals(["__route_byte", "__route_match"]);

        let (literal, node) = self.chain();
        if let Some(state) = node.terminal
            && node.children.is_empty()
        {
            let len = literal.len();
            let literal = proc_macro2::Literal::byte_string(&literal);
            let mismatch = match fallback {
                Some(next) => {
                    let scan = Self::scan_tail(paths, &v_rtail, &v_found);
                    quote! {
                        #scan
                        ::core::option::Option::Some(#next)
                    }
                }
                None => quote!(::core::option::Option::None),
            };
            return quote! {
                if let ::core::option::Option::Some((#v_prefix, #v_tail)) =
                    #v_rtail.split_first_chunk::<#len>()
                    && #v_prefix == #literal
                    && ::core::matches!(
                        #v_tail.first(),
                        ::core::option::Option::None
                            | ::core::option::Option::Some(b'/' | b'?')
                    )
                {
                    #v_rtail = #v_tail;
                    ::core::option::Option::Some(#state)
                } else {
                    #mismatch
                }
            };
        }

        if self.children.is_empty() && self.terminal.is_none() {
            return match fallback {
                Some(next) => {
                    let scan = Self::scan_tail(paths, &v_rtail, &v_found);
                    quote! {
                        #scan
                        ::core::option::Option::Some(#next)
                    }
                }
                None => quote!(::core::option::Option::None),
            };
        }
        let (nodes, root) = self.flatten();

        let edges = nodes.iter().enumerate().flat_map(|(node, value)| {
            value
                .children
                .iter()
                .map(move |(byte, next)| quote!((#node, #byte) => #next))
        });
        let ends = nodes.iter().enumerate().filter_map(|(node, value)| {
            value
                .terminal
                .map(|state| quote!(#node => ::core::option::Option::Some(#state)))
        });
        let miss = match fallback {
            Some(next) => quote!(::core::option::Option::Some(#next)),
            None => quote!(::core::option::Option::None),
        };
        let label = fallback.map(|_| quote!('__sark_radix:));
        let mismatch = match fallback {
            Some(next) => {
                let scan = Self::scan_tail(paths, &v_rtail, &v_found);
                quote! {
                    #scan
                    break '__sark_radix (::core::option::Option::Some(#next));
                }
            }
            None => quote!(break '__sark_route_match),
        };
        let prefixes = nodes
            .iter()
            .enumerate()
            .filter(|(_, value)| !value.prefix.is_empty())
            .map(|(node, value)| {
                let len = value.prefix.len();
                let literal = proc_macro2::Literal::byte_string(&value.prefix);
                quote! {
                    #node => {
                        if let ::core::option::Option::Some((#v_prefix, #v_tail)) =
                            #v_rtail.split_first_chunk::<#len>()
                            && #v_prefix == #literal
                        {
                            #v_rtail = #v_tail;
                        } else {
                            #mismatch
                        }
                    }
                }
            });
        quote! {
            #label {
                let mut #v_node = #root;
                loop {
                    match #v_node {
                        #( #prefixes, )*
                        _ => {}
                    }
                    let ::core::option::Option::Some((&#v_rbyte, #v_tail)) =
                        #v_rtail.split_first()
                    else {
                        break;
                    };
                    if #v_rbyte == b'/' || #v_rbyte == b'?' {
                        break;
                    }
                    #v_rtail = #v_tail;
                    #v_node = match (#v_node, #v_rbyte) {
                        #( #edges, )*
                        _ => { #mismatch }
                    };
                }
                match #v_node {
                    #( #ends, )*
                    _ => #miss,
                }
            }
        }
    }

    fn flatten(&self) -> (Vec<Node>, usize) {
        let mut nodes = Vec::new();
        let root = self.append(&mut nodes);
        (nodes, root)
    }

    fn append(&self, nodes: &mut Vec<Node>) -> usize {
        let (prefix, node) = self.chain();
        let children = node
            .children
            .iter()
            .map(|(&byte, child)| (byte, child.append(nodes)))
            .collect();
        let id = nodes.len();
        nodes.push(Node {
            prefix,
            terminal: node.terminal,
            children,
        });
        id
    }

    fn chain(&self) -> (Vec<u8>, &Self) {
        let mut literal = Vec::new();
        let mut node = self;
        while node.terminal.is_none() {
            let mut children = node.children.iter();
            let Some((byte, child)) = children.next() else {
                break;
            };
            if children.next().is_some() {
                break;
            }
            literal.push(*byte);
            node = child;
        }
        (literal, node)
    }

    fn scan_tail(paths: &Paths, tail: &syn::Ident, found: &syn::Ident) -> proc_macro2::TokenStream {
        let ns_http = &paths.http;
        use quote::quote;

        quote! {
            #tail = match #ns_http::field::scan::Match::find2(
                b'/',
                b'?',
                #tail,
            ) {
                ::core::option::Option::Some(#found) => #found.tail(),
                ::core::option::Option::None => &[],
            };
        }
    }
}
