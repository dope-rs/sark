use std::collections::BTreeMap;

use crate::method;

#[derive(Default)]
pub(crate) struct Table<'a> {
    route: Option<(usize, &'a method::Method)>,
    children: BTreeMap<u8, Table<'a>>,
}

impl<'a> Table<'a> {
    pub(crate) fn new(methods: &'a [method::Method]) -> syn::Result<Self> {
        let mut root = Self::default();
        for (index, method) in methods.iter().enumerate() {
            root.insert(method, index)?;
        }
        Ok(root)
    }

    pub(crate) fn emit(
        &self,
        miss: proc_macro2::TokenStream,
        mut leaf: impl FnMut(&'a method::Method) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let (guard, branch) = self.emit_at(0, &miss, &mut leaf);
        match guard {
            Some(guard) => quote::quote!(if #guard { #branch } else { #miss }),
            None => branch,
        }
    }

    fn insert(&mut self, method: &'a method::Method, index: usize) -> syn::Result<()> {
        let mut node = self;
        for &byte in method.path.iter() {
            node = node.children.entry(byte).or_default();
        }
        if let Some((first, _)) = node.route {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("duplicate gRPC route at method indices {first} and {index}"),
            ));
        }
        node.route = Some((index, method));
        Ok(())
    }

    fn emit_at(
        &self,
        offset: usize,
        miss: &proc_macro2::TokenStream,
        leaf: &mut impl FnMut(&'a method::Method) -> proc_macro2::TokenStream,
    ) -> (Option<proc_macro2::TokenStream>, proc_macro2::TokenStream) {
        let (node, literal) = self.chain();
        let end = offset + literal.len();
        let guard = if literal.is_empty() {
            None
        } else {
            let literal = proc_macro2::Literal::byte_string(&literal);
            Some(quote::quote! {
                __grpc_path.get(#offset..#end) == ::core::option::Option::Some(#literal)
            })
        };
        let next = if node.children.is_empty() {
            miss.clone()
        } else {
            let edges = node.children.iter().map(|(&byte, child)| {
                let (guard, branch) = child.emit_at(end + 1, miss, leaf);
                let guard = guard.map(|guard| quote::quote!(if #guard));
                quote::quote!(::core::option::Option::Some(#byte) #guard => { #branch })
            });
            quote::quote! {
                match __grpc_path.get(#end).copied() {
                    #( #edges, )*
                    _ => { #miss }
                }
            }
        };
        let branch = match node.route {
            Some((_, method)) => {
                let accept = leaf(method);
                if node.children.is_empty() {
                    let exact = quote::quote!(__grpc_path.len() == #end);
                    let guard = match guard {
                        Some(guard) => quote::quote!(#guard && #exact),
                        None => exact,
                    };
                    return (Some(guard), accept);
                }
                quote::quote! {
                    if __grpc_path.len() == #end {
                        #accept
                    } else {
                        #next
                    }
                }
            }
            None => next,
        };
        (guard, branch)
    }

    fn chain(&self) -> (&Self, Vec<u8>) {
        let mut node = self;
        let mut literal = Vec::new();
        while node.route.is_none() && node.children.len() == 1 {
            let Some((&byte, child)) = node.children.first_key_value() else {
                break;
            };
            literal.push(byte);
            node = child;
        }
        (node, literal)
    }
}
