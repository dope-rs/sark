use proc_macro2::TokenStream;
use quote::quote;

use crate::{runtime::paths::Paths, types::Generics};

pub(super) fn emit_scan(
    literals: &[(Vec<u8>, usize)],
    fallback: Option<usize>,
    paths: &Paths,
    generics: &Generics,
) -> TokenStream {
    let names = generics.locals(["__route_tail", "__tail", "__prefix"]);
    let [tail, ..] = &names;
    let miss = match fallback {
        Some(next) => {
            let http = &paths.http;
            let found = generics.local("__route_match");
            quote! {
                #tail = match #http::field::scan::Match::find2(b'/', b'?', #tail) {
                    ::core::option::Option::Some(#found) => #found.tail(),
                    ::core::option::Option::None => &[],
                };
                ::core::option::Option::Some(#next)
            }
        }
        None => quote!(::core::option::Option::None),
    };
    if literals.is_empty() {
        return miss;
    }
    let tree = branches(literals, 0, &names);
    quote! {
        '__sark_literal: {
            #tree
            #miss
        }
    }
}

fn branches(literals: &[(Vec<u8>, usize)], depth: usize, names: &[syn::Ident; 3]) -> TokenStream {
    let Some(((first, next), rest)) = literals.split_first() else {
        return TokenStream::new();
    };
    let last = rest.last().map_or(first, |(literal, _)| literal);
    let length = first[depth..]
        .iter()
        .zip(&last[depth..])
        .take_while(|(a, b)| a == b)
        .count();
    let end = depth + length;
    let [tail, suffix, prefix] = names;
    let (terminal, children) = if first.len() == end {
        let terminal = quote! {
            ::core::option::Option::None | ::core::option::Option::Some((b'/' | b'?', _)) =>
                break '__sark_literal ::core::option::Option::Some(#next),
        };
        (terminal, rest)
    } else {
        (TokenStream::new(), literals)
    };
    let edges = children
        .chunk_by(|(a, _), (b, _)| a[end] == b[end])
        .filter_map(|group| {
            let (literal, _) = group.first()?;
            let byte = literal[end];
            let child = branches(group, end + 1, names);
            Some(quote! {
                ::core::option::Option::Some((&#byte, #suffix)) => {
                    #tail = #suffix;
                    #child
                }
            })
        });
    let branch = quote! {
        match #tail.split_first() {
            #terminal
            #( #edges, )*
            _ => {}
        }
    };
    if length == 0 {
        return branch;
    }
    let literal = proc_macro2::Literal::byte_string(&first[depth..end]);
    quote! {
        if let ::core::option::Option::Some((#prefix, #suffix)) =
            #tail.split_first_chunk::<#length>()
            && #prefix == #literal
        {
            #tail = #suffix;
            #branch
        }
    }
}
