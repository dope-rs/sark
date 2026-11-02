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

pub(super) struct Names(TokenStream);

impl Names {
    pub(super) fn new(methods: &[Method], service: &Ident) -> Self {
        let types = methods.iter().map(|method| {
            let request = &method.request;
            let response = &method.response;
            quote::quote!(#request #response)
        });
        Self(quote::quote!(#service #(#types)*))
    }

    pub(super) fn ident(&self, base: &str) -> Ident {
        let mut name = base.to_owned();
        while Self::contains(self.0.clone(), &name) {
            name.push('_');
        }
        Ident::new(&name, proc_macro2::Span::call_site())
    }

    fn contains(tokens: TokenStream, name: &str) -> bool {
        tokens.into_iter().any(|token| match token {
            TokenTree::Ident(ident) => ident.unraw() == name,
            TokenTree::Group(group) => Self::contains(group.stream(), name),
            TokenTree::Punct(_) | TokenTree::Literal(_) => false,
        })
    }
}
