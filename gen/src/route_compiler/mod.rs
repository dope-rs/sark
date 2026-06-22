pub(super) mod param_dfa;
pub(super) mod static_tree;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
}

pub(super) enum Seg {
    Literal(Vec<u8>),
    Param,
}

impl Seg {
    pub(super) fn segment(path: &str) -> Vec<Self> {
        let rest = path.strip_prefix('/').unwrap_or(path);
        if rest.is_empty() {
            return Vec::new();
        }
        rest.split('/')
            .map(|s| {
                if s.starts_with(':') {
                    Self::Param
                } else {
                    Self::Literal(s.as_bytes().to_vec())
                }
            })
            .collect()
    }
}

impl Method {
    pub(super) fn ord(self) -> u8 {
        match self {
            Self::Get => 0,
            Self::Post => 1,
            Self::Put => 2,
            Self::Patch => 3,
            Self::Delete => 4,
            Self::Head => 5,
            Self::Options => 6,
        }
    }

    pub(super) fn key_token(self) -> TokenStream {
        let path: syn::Path = match self {
            Self::Get => parse_quote!(sark::service::Key::Get),
            Self::Post => parse_quote!(sark::service::Key::Post),
            Self::Put => parse_quote!(sark::service::Key::Put),
            Self::Patch => parse_quote!(sark::service::Key::Patch),
            Self::Delete => parse_quote!(sark::service::Key::Delete),
            Self::Head => parse_quote!(sark::service::Key::Head),
            Self::Options => parse_quote!(sark::service::Key::Options),
        };
        quote!(#path)
    }
}
