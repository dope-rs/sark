use crate::{runtime::paths::Paths, types::Lifetimes};

#[derive(Clone, Copy)]
enum Inclusion {
    Include,
    Omit,
}

#[derive(Clone, Copy)]
pub(super) struct Skip {
    date: Inclusion,
    server: Inclusion,
}

pub(super) struct Attrs {
    pub(super) max_body: Option<syn::Expr>,
    pub(super) head_skip: Skip,
}

impl Attrs {
    pub(super) fn take(fun: &mut syn::ItemFn) -> syn::Result<Self> {
        let mut max: Option<syn::Expr> = None;
        let mut skips = Skip {
            date: Inclusion::Include,
            server: Inclusion::Include,
        };
        for attr in &fun.attrs {
            if attr.path().is_ident("max_body") {
                if max.is_some() {
                    return Err(syn::Error::new_spanned(attr, "duplicate #[max_body(...)]"));
                }
                max = Some(attr.parse_args::<syn::Expr>()?);
            } else if attr.path().is_ident("skip") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("date") {
                        skips.date = Inclusion::Omit;
                    } else if meta.path.is_ident("server") {
                        skips.server = Inclusion::Omit;
                    } else {
                        return Err(
                            meta.error("unknown #[skip(...)] target; expected `date` | `server`")
                        );
                    }
                    Ok(())
                })?;
            }
        }
        fun.attrs.retain(|attr| !attr.path().is_ident("max_body") && !attr.path().is_ident("skip"));
        Ok(Self {
            max_body: max,
            head_skip: skips,
        })
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
        max_body: Option<&'a syn::Expr>,
        skip: Skip,
        h1: crate::runtime::H1,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let ns_h1 = &paths.h1;
        use quote::quote;
        let (kind, stream, coding) = match self {
            Response::Native(output) => {
                let life = Lifetimes::new(output);
                let drive = life.response(&syn::parse_quote!('__d), None, &[]);
                let owned = life.response(&syn::parse_quote!('static), None, &[]);
                let kind = quote! {
                    <#owned as #ns_core::manifold::Response<'static, 'static>>::Kind
                };
                let stream = quote! {
                    <#drive as #ns_core::manifold::Response<'__d, '__d>>::Stream
                };
                let payload = quote! {
                    <#owned as #ns_core::manifold::Response<'static, 'static>>::BODY_KIND
                };
                let coding = quote! {
                    match #payload {
                        #ns_http::payload::Kind::Inline => #ns_http::h1::head::request::Coding::Gzip,
                        #ns_http::payload::Kind::Static | #ns_http::payload::Kind::Stream => {
                            #ns_http::h1::head::request::Coding::Plain
                        }
                    }
                };
                (kind, stream, coding)
            }
            Response::Plain(kind) => (
                kind,
                quote!(#ns_http::payload::response::dispatch::Never),
                quote!(#ns_http::h1::head::request::Coding::Plain),
            ),
        };
        let max = match max_body {
            Some(expr) => quote! { const MAX_BODY: ::core::primitive::usize = #expr; },
            None => quote! {},
        };
        let date = match skip.date {
            Inclusion::Include => quote!(#ns_http::h1::head::response::Date::Include),
            Inclusion::Omit => quote!(#ns_http::h1::head::response::Date::Omit),
        };
        let server = match skip.server {
            Inclusion::Include => quote!(#ns_http::h1::head::response::Server::Include),
            Inclusion::Omit => quote!(#ns_http::h1::head::response::Server::Omit),
        };
        let policy = match h1 {
            crate::runtime::H1::Available(_) => quote! {
                impl #ns_h1::route::PolicyOf for #name {
                    const POLICY: #ns_h1::route::Policy = #ns_h1::route::Policy::new(
                        #coding,
                        #date,
                        #server,
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
