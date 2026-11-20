use crate::{runtime::paths::Paths, types::Lifetimes};

pub(super) struct Attrs {
    pub(super) required_gzip: bool,
    max_body: Option<syn::Expr>,
    date: bool,
    server: bool,
}

impl Attrs {
    pub(super) fn take(fun: &mut syn::ItemFn) -> syn::Result<Self> {
        let mut attrs = Self { required_gzip: false, max_body: None, date: true, server: true };
        for attr in &fun.attrs {
            if attr.path().is_ident("gzip") {
                let option = attr.parse_args::<syn::Ident>()?;
                if option != "required" {
                    return Err(syn::Error::new_spanned(option, "unknown #[gzip(...)] option; expected `required`"));
                }
                if attrs.required_gzip {
                    return Err(syn::Error::new_spanned(attr, "duplicate #[gzip(required)]"));
                }
                attrs.required_gzip = true;
            } else if attr.path().is_ident("max_body") {
                if attrs.max_body.is_some() {
                    return Err(syn::Error::new_spanned(attr, "duplicate #[max_body(...)]"));
                }
                attrs.max_body = Some(attr.parse_args::<syn::Expr>()?);
            } else if attr.path().is_ident("skip") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("date") {
                        attrs.date = false;
                    } else if meta.path.is_ident("server") {
                        attrs.server = false;
                    } else {
                        return Err(
                            meta.error("unknown #[skip(...)] target; expected `date` | `server`")
                        );
                    }
                    Ok(())
                })?;
            }
        }
        fun.attrs.retain(|attr| {
            let name = attr.path();
            !name.is_ident("gzip") && !name.is_ident("max_body") && !name.is_ident("skip")
        });
        Ok(attrs)
    }
}

pub(super) enum Response<'a> {
    Native(&'a syn::Type),
    Plain(proc_macro2::TokenStream),
}

impl<'a> Response<'a> {
    pub(super) fn build(
        self,
        paths: &Paths,
        name: &'a proc_macro2::Ident,
        req: &'a syn::Type,
        attrs: Attrs,
        h1: crate::runtime::H1,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let ns_h1 = &paths.h1;
        use quote::quote;
        let (kind, stream, mode) = match self {
            Response::Native(output) => {
                let life = Lifetimes::new(output);
                let drive = life.response(&syn::parse_quote!('__d), None, &[]);
                let owned = life.response(&syn::parse_quote!('static), None, &[]);
                let kind = quote! {
                    <#owned as #ns_http::payload::response::dispatch::Emit<'static, 'static>>::Kind
                };
                let stream = quote! {
                    <#drive as #ns_http::payload::response::dispatch::Emit<'__d, '__d>>::Stream
                };
                let payload = quote! {
                    <#owned as #ns_http::payload::response::dispatch::Emit<'static, 'static>>::BODY
                };
                let mode = if attrs.required_gzip {
                    quote!(#ns_h1::route::Mode::RequiredGzip)
                } else {
                    quote! {
                        match #payload {
                            #ns_http::payload::Kind::Inline => #ns_h1::route::Mode::Gzip,
                            #ns_http::payload::Kind::Static | #ns_http::payload::Kind::Stream => #ns_h1::route::Mode::Plain,
                        }
                    }
                };
                (kind, stream, mode)
            }
            Response::Plain(kind) => (
                kind,
                quote!(#ns_http::payload::response::dispatch::Never),
                quote!(#ns_h1::route::Mode::Plain),
            ),
        };
        let max = match attrs.max_body {
            Some(expr) => quote! { const MAX_BODY: ::core::primitive::usize = #expr; },
            None => quote! {},
        };
        let inclusion = |enabled: bool| if enabled { quote!(Include) } else { quote!(Omit) };
        let date = inclusion(attrs.date);
        let server = inclusion(attrs.server);
        let policy = match h1 {
            crate::runtime::H1::Available(_) => quote! {
                impl #ns_h1::route::PolicyOf for #name {
                    const POLICY: #ns_h1::route::Policy = #ns_h1::route::Policy::new(
                        #mode,
                        #ns_http::h1::head::response::Date::#date,
                        #ns_http::h1::head::response::Server::#server,
                    );
                }
            },
            crate::runtime::H1::Missing => quote! {},
        };

        quote! {
            impl #ns_core::route::Spec for #name {
                type Kind = #kind;
                type Request = #req;
                type Stream<'__d> = #stream;
                #max
            }

            #policy
        }
    }
}
