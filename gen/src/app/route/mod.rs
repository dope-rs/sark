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

    pub(super) fn parse(value: &syn::Ident) -> syn::Result<(Self, bool)> {
        let name = value.to_string();
        if name == "WS" {
            return Ok((Self::Get, true));
        }
        for method in Self::ALL {
            if name == method.spellings().0 {
                return Ok((method, false));
            }
        }
        Err(syn::Error::new_spanned(
            value,
            "unsupported method; use one of GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS/CONNECT/WS",
        ))
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
        let path = syn::Ident::new(self.spellings().1, proc_macro2::Span::call_site());
        quote::quote!(#ns_http::method::Known::#path)
    }

    pub(super) fn bytes_token(self) -> proc_macro2::TokenStream {
        let bytes = proc_macro2::Literal::byte_string(self.spellings().0.as_bytes());
        quote::quote!(#bytes)
    }

    pub(super) fn http_token(self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_http = &paths.http;
        let path = syn::Ident::new(self.spellings().0, proc_macro2::Span::call_site());
        quote::quote!(#ns_http::method::Method::#path)
    }

    fn spellings(self) -> (&'static str, &'static str) {
        match self {
            Self::Get => ("GET", "Get"),
            Self::Post => ("POST", "Post"),
            Self::Put => ("PUT", "Put"),
            Self::Patch => ("PATCH", "Patch"),
            Self::Delete => ("DELETE", "Delete"),
            Self::Head => ("HEAD", "Head"),
            Self::Options => ("OPTIONS", "Options"),
            Self::Connect => ("CONNECT", "Connect"),
        }
    }
}
