use std::collections::BTreeSet;

use proc_macro2::{Ident, TokenStream, TokenTree};
use syn::ext::IdentExt;

use crate::method::Method;

pub(super) fn stem(name: &Ident) -> String {
    let name = name.unraw().to_string();
    match name.strip_prefix('_') {
        Some(stem) if stem.starts_with(|c: char| c.is_ascii_digit()) => stem.to_owned(),
        _ => name,
    }
}

pub(super) fn suffixed(proto: &str, mut name: String, suffix: &str) -> syn::Result<Ident> {
    if proto.starts_with('_') && name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert(0, '_');
    }
    name.push_str(suffix);
    syn::parse_str(&name)
}

pub(super) struct Names(BTreeSet<String>);

impl Names {
    pub(super) fn new(source: &str) -> syn::Result<Self> {
        let mut names = Self(BTreeSet::new());
        for item in syn::parse_file(source)?.items {
            let ident = match item {
                syn::Item::Const(item) => Some(item.ident),
                syn::Item::Enum(item) => Some(item.ident),
                syn::Item::ExternCrate(item) => {
                    Some(item.rename.map_or(item.ident, |(_, name)| name))
                }
                syn::Item::Fn(item) => Some(item.sig.ident),
                syn::Item::Macro(item) => item.ident,
                syn::Item::Mod(item) => Some(item.ident),
                syn::Item::Static(item) => Some(item.ident),
                syn::Item::Struct(item) => Some(item.ident),
                syn::Item::Trait(item) => Some(item.ident),
                syn::Item::TraitAlias(item) => Some(item.ident),
                syn::Item::Type(item) => Some(item.ident),
                syn::Item::Union(item) => Some(item.ident),
                syn::Item::Use(item) => {
                    names.imports(item.tree, None);
                    None
                }
                _ => None,
            };
            if let Some(ident) = ident {
                names.insert(ident);
            }
        }
        Ok(names)
    }

    pub(super) fn types(&mut self, methods: &[Method]) {
        let types = methods.iter().map(|method| {
            let request = &method.request;
            let response = &method.response;
            quote::quote!(#request #response)
        });
        self.tokens(quote::quote!(#(#types)*));
    }

    pub(super) fn ident(&mut self, base: &Ident) -> Ident {
        let mut name = base.unraw().to_string();
        while !self.0.insert(name.clone()) {
            name.push('_');
        }
        Ident::new(&name, base.span())
    }

    fn insert(&mut self, name: Ident) {
        self.0.insert(name.unraw().to_string());
    }

    fn tokens(&mut self, tokens: TokenStream) {
        for token in tokens {
            match token {
                TokenTree::Ident(ident) => self.insert(ident),
                TokenTree::Group(group) => self.tokens(group.stream()),
                TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }

    fn imports(&mut self, tree: syn::UseTree, parent: Option<Ident>) {
        match tree {
            syn::UseTree::Path(path) => self.imports(*path.tree, Some(path.ident)),
            syn::UseTree::Name(name) => {
                let ident = if name.ident == "self" {
                    parent.unwrap_or(name.ident)
                } else {
                    name.ident
                };
                self.insert(ident);
            }
            syn::UseTree::Rename(rename) => self.insert(rename.rename),
            syn::UseTree::Group(group) => {
                for item in group.items {
                    self.imports(item, parent.clone());
                }
            }
            syn::UseTree::Glob(_) => {}
        }
    }
}
