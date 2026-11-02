use crate::runtime::paths::Paths;

mod plan;

pub(super) use plan::Plan;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Connect,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Seg {
    Literal(Vec<u8>),
    Param,
}

type Parsed = (Vec<Seg>, Vec<(Vec<u8>, usize)>);

impl Seg {
    pub(super) fn parse(path: &syn::LitStr, rest: &str) -> syn::Result<Parsed> {
        if rest.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let count = rest.split('/').count();
        let mut segments = Vec::with_capacity(count);
        let mut params = Vec::new();
        for (depth, segment) in rest.split('/').enumerate() {
            match segment.strip_prefix(':') {
                Some(name) => {
                    Self::parameter(name, path.span())?;
                    if params.iter().any(|(prior, _)| prior == name.as_bytes()) {
                        return Err(syn::Error::new_spanned(
                            path,
                            format!("duplicate path parameter `{name}`"),
                        ));
                    }
                    segments.push(Self::Param);
                    params.push((name.as_bytes().to_vec(), depth));
                }
                None => segments.push(Self::Literal(segment.as_bytes().to_vec())),
            }
        }
        params.sort_by(|left, right| left.0.cmp(&right.0));
        Ok((segments, params))
    }

    fn parameter(name: &str, span: proc_macro2::Span) -> syn::Result<()> {
        let Some((first, tail)) = name.as_bytes().split_first() else {
            return Err(syn::Error::new(
                span,
                "path parameter names must match `[A-Za-z_][A-Za-z0-9_]*`",
            ));
        };
        if !(first.is_ascii_alphabetic() || *first == b'_')
            || !tail
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return Err(syn::Error::new(
                span,
                "path parameter names must match `[A-Za-z_][A-Za-z0-9_]*`",
            ));
        }
        if name == "_" || name == "crate" || name == "self" || name == "Self" || name == "super" {
            return Err(syn::Error::new(
                span,
                "a path parameter name must be representable as a Rust field identifier",
            ));
        }
        Ok(())
    }

    pub(super) fn literal(&self) -> Option<&[u8]> {
        match self {
            Self::Literal(bytes) => Some(bytes),
            Self::Param => None,
        }
    }

    pub(super) fn is_literal(&self) -> bool {
        self.literal().is_some()
    }

    pub(super) fn is_overlap(&self, other: &Self) -> bool {
        match (self.literal(), other.literal()) {
            (Some(left), Some(right)) => left == right,
            (left, right) => left.is_none() || right.is_none(),
        }
    }

    pub(super) fn is_within(&self, other: &Self) -> bool {
        match other.literal() {
            Some(right) => self.literal() == Some(right),
            None => true,
        }
    }
}

impl Method {
    pub(super) const ALL: [Self; 8] = [
        Self::Get,
        Self::Post,
        Self::Put,
        Self::Patch,
        Self::Delete,
        Self::Head,
        Self::Options,
        Self::Connect,
    ];

    pub(super) fn parse(value: &syn::Ident) -> syn::Result<Self> {
        if value == "GET" || value == "WS" {
            Ok(Self::Get)
        } else if value == "POST" {
            Ok(Self::Post)
        } else if value == "PUT" {
            Ok(Self::Put)
        } else if value == "PATCH" {
            Ok(Self::Patch)
        } else if value == "DELETE" {
            Ok(Self::Delete)
        } else if value == "HEAD" {
            Ok(Self::Head)
        } else if value == "OPTIONS" {
            Ok(Self::Options)
        } else if value == "CONNECT" {
            Ok(Self::Connect)
        } else {
            Err(syn::Error::new_spanned(
                value,
                "unsupported method; use one of GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS/CONNECT/WS",
            ))
        }
    }

    pub(super) const fn ord(self) -> u8 {
        self as u8
    }

    pub(super) fn bit_token(self, paths: &Paths) -> proc_macro2::TokenStream {
        let key = self.key_token(paths);
        quote::quote!(#key.bit())
    }

    pub(super) fn key_token(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_http = &paths.http;
        let path = quote::format_ident!("{}", self.known_name());
        quote::quote!(#ns_http::method::Known::#path)
    }

    pub(super) fn bytes_token(self) -> proc_macro2::TokenStream {
        let bytes = proc_macro2::Literal::byte_string(self.name().as_bytes());
        quote::quote!(#bytes)
    }

    pub(super) fn http_token(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_http = &paths.http;
        let path = quote::format_ident!("{}", self.name());
        quote::quote!(#ns_http::method::Method::#path)
    }

    fn name(&self) -> &str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Connect => "CONNECT",
        }
    }

    fn known_name(&self) -> &str {
        match self {
            Self::Get => "Get",
            Self::Post => "Post",
            Self::Put => "Put",
            Self::Patch => "Patch",
            Self::Delete => "Delete",
            Self::Head => "Head",
            Self::Options => "Options",
            Self::Connect => "Connect",
        }
    }
}
