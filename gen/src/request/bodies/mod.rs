use crate::{macros::tokens::local, runtime::paths::Paths, types};

pub(super) mod task;

pub(super) enum Source {
    Discarded,
    Raw(Raw),
    Json(Json),
}

pub(super) struct Json {
    ident: syn::Ident,
    ty: syn::Type,
}

pub(super) struct Raw {
    ident: syn::Ident,
    borrowed_ty: syn::Type,
    segmented: bool,
}

impl Raw {
    fn new(paths: &Paths, ident: syn::Ident, ty: &syn::Type) -> syn::Result<Self> {
        let ns_http = &paths.http;
        let view = types::View::new(ty);
        let segments = view.is_named("Segments");
        let valid = segments || view.is_named("Shared");
        if !valid {
            return Err(syn::Error::new_spanned(
                ty,
                "#[raw_body] field type must be owned::Segments or Shared",
            ));
        }
        let borrowed = if segments {
            syn::parse_quote!(#ns_http::request::body::Ref<'req, 'd>)
        } else {
            syn::parse_quote!(&'req [::core::primitive::u8])
        };
        Ok(Self {
            ident,
            borrowed_ty: borrowed,
            segmented: segments,
        })
    }
}

pub(super) struct Plan {
    source: Source,
    length: Option<syn::Ident>,
}

impl Plan {
    pub(super) fn scope(
        &mut self,
        request: &syn::Ident,
    ) -> Option<(syn::Ident, proc_macro2::TokenStream)> {
        let Source::Json(json) = &mut self.source else {
            return None;
        };
        let name = quote::format_ident!("__{}Json", request);
        let ty = &json.ty;
        let declaration = quote::quote!(type #name = #ty;);
        json.ty = syn::parse_quote!(#name);
        Some((name, declaration))
    }

    pub(super) fn mode_impls(
        &self,
        paths: &Paths,
        vis: &syn::Visibility,
        mode: &syn::Ident,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let ns_json = &paths.json;
        let (borrowed, owned) = match &self.source {
            Source::Discarded => (quote::quote!(()), quote::quote!(())),
            Source::Raw(raw) => {
                let borrowed = &raw.borrowed_ty;
                let owned = if raw.segmented {
                    quote::quote!(#ns_http::request::Segments<'d>)
                } else {
                    quote::quote!(#ns_http::request::Bytes<'d>)
                };
                (quote::quote!(#borrowed), owned)
            }
            Source::Json(json) => {
                let ty = &json.ty;
                (
                    quote::quote!(
                        <#ty as #ns_json::parsing::Request>::View<'req>
                    ),
                    quote::quote!(
                        <#ty as #ns_json::segment::IntoRequest>::Request<'d>
                    ),
                )
            }
        };
        quote::quote! {
            #[doc(hidden)]
            #vis trait #mode<'d>: #ns_core::request::mode::Mode<'d> {
                type Body;
            }

            impl<'req, 'd: 'req> #mode<'d> for #ns_core::request::mode::Ref<'req> {
                type Body = #borrowed;
            }

            impl<'d> #mode<'d> for #ns_core::request::mode::Own {
                type Body = #owned;
            }
        }
    }

    pub(super) fn rewrite(&self, field: &mut syn::Field, mode: &syn::Ident, param: &syn::Ident) {
        let body = match &self.source {
            Source::Raw(raw) => field.ident.as_ref() == Some(&raw.ident),
            Source::Json(json) => field.ident.as_ref() == Some(&json.ident),
            Source::Discarded => false,
        };
        if body {
            field.ty = syn::parse_quote!(<#param as #mode<'d>>::Body);
        }
    }

    pub(super) fn from_attrs(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut json = None;
        for attr in attrs {
            if attr.path().is_ident("json_body") {
                if let Some(first) = &json {
                    let mut error = syn::Error::new_spanned(attr, "duplicate #[json_body(...)]");
                    error.combine(syn::Error::new_spanned(
                        first,
                        "first #[json_body(...)] declared here",
                    ));
                    return Err(error);
                }
                json = Some(attr.parse_args::<syn::Type>()?);
            }
        }
        Ok(Self {
            source: match json {
                Some(ty) => Source::Json(Json {
                    ident: syn::Ident::new("body", proc_macro2::Span::call_site()),
                    ty,
                }),
                None => Source::Discarded,
            },
            length: None,
        })
    }

    pub(super) fn try_raw(
        self,
        paths: &Paths,
        ident: syn::Ident,
        ty: &syn::Type,
    ) -> syn::Result<Self> {
        let source = match self.source {
            Source::Discarded => Source::Raw(Raw::new(paths, ident, ty)?),
            Source::Raw(raw) => {
                let mut error = syn::Error::new_spanned(ty, "duplicate #[raw_body]");
                error.combine(syn::Error::new_spanned(
                    raw.ident,
                    "first #[raw_body] declared here",
                ));
                return Err(error);
            }
            Source::Json(json) => {
                let mut error = syn::Error::new_spanned(
                    ty,
                    "#[json_body(...)] and #[raw_body] are mutually exclusive",
                );
                error.combine(syn::Error::new_spanned(
                    json.ty,
                    "#[json_body(...)] declared here",
                ));
                return Err(error);
            }
        };
        Ok(Self {
            source,
            length: self.length,
        })
    }

    pub(super) fn try_len(self, ident: syn::Ident, ty: &syn::Type) -> syn::Result<Self> {
        if let Some(first) = self.length {
            let mut error = syn::Error::new_spanned(ty, "duplicate #[body_len]");
            error.combine(syn::Error::new_spanned(
                first,
                "first #[body_len] declared here",
            ));
            return Err(error);
        }
        if !types::View::new(ty).is_named("BodyLen") {
            return Err(syn::Error::new_spanned(
                ty,
                "#[body_len] field type must be request::BodyLen",
            ));
        }
        Ok(Self {
            source: self.source,
            length: Some(ident),
        })
    }

    pub(super) fn append_json(&self, named: &mut syn::FieldsNamed, vis: &syn::Visibility) {
        let Source::Json(json) = &self.source else {
            return;
        };
        let ident = &json.ident;
        let ty = &json.ty;
        named.named.push(syn::parse_quote!(#vis #ident: #ty));
    }

    pub(super) fn fields(
        &self,
        paths: &Paths,
        body: syn::Ident,
        len: proc_macro2::TokenStream,
    ) -> Vec<proc_macro2::TokenStream> {
        let ns_core = &paths.core;
        let mut fields = Vec::with_capacity(2);
        match &self.source {
            Source::Raw(raw) => {
                let ident = &raw.ident;
                fields.push(quote::quote!(#ident: #body));
            }
            Source::Json(json) => {
                let ident = &json.ident;
                fields.push(quote::quote!(#ident: #body));
            }
            Source::Discarded => {}
        }
        if let Some(ident) = &self.length {
            fields.push(quote::quote!(#ident: #ns_core::request::BodyLen::new(#len)));
        }
        fields
    }

    pub(super) fn task(&self, paths: &Paths) -> (proc_macro2::TokenStream, task::Plan<'_>) {
        let ns_core = &paths.core;
        match &self.source {
            Source::Json(json) => (
                quote::quote!(#ns_core::body::Contiguous),
                task::Plan::Json(&json.ty),
            ),
            Source::Raw(raw) => {
                let mode = if raw.segmented {
                    quote::quote!(#ns_core::body::Segmented)
                } else {
                    quote::quote!(#ns_core::body::Contiguous)
                };
                (
                    mode,
                    task::Plan::Raw {
                        segmented: raw.segmented,
                    },
                )
            }
            Source::Discarded => (
                quote::quote!(#ns_core::body::Discarded),
                task::Plan::Discarded,
            ),
        }
    }

    pub(super) fn parsing(
        &self,
        paths: &Paths,
        path: &proc_macro2::TokenStream,
    ) -> (
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    ) {
        let ns_core = &paths.core;
        let ns_json = &paths.json;
        let body = local("body");
        let (cause, param, value) = match &self.source {
            Source::Json(json) => {
                let ty = &json.ty;
                (
                    quote::quote!(#ns_json::parsing::Error<'req>),
                    quote::quote!(<#ty as #ns_json::parsing::Request>::View<'req>),
                    quote::quote! {
                        <#ty as #ns_json::parsing::Request>::decode(#body.as_slice())
                            .map_err(#ns_core::route::Input::Body)
                    },
                )
            }
            Source::Raw(raw) => {
                let ty = &raw.borrowed_ty;
                let value = if raw.segmented {
                    quote::quote!(::core::result::Result::Ok(#body))
                } else {
                    quote::quote!(::core::result::Result::Ok(#body.as_slice()))
                };
                (
                    quote::quote!(::core::convert::Infallible),
                    quote::quote!(#ty),
                    value,
                )
            }
            Source::Discarded => (
                quote::quote!(::core::convert::Infallible),
                quote::quote!(()),
                quote::quote!(::core::result::Result::Ok(())),
            ),
        };
        let input = match &self.source {
            Source::Discarded => quote::quote!(_),
            Source::Raw(_) | Source::Json(_) => quote::quote!(#body),
        };
        let error = quote::quote!(#ns_core::route::Input<#path, #cause,>);
        let parsed = quote::quote! {
            type ParsedBody<'req, 'd> = #param
            where
                'd: 'req;

            fn parse_body<'req, 'd>(
                #input: <Self::Body as #ns_core::body::Project>::View<'req, 'd>,
            ) -> ::core::result::Result<
                Self::ParsedBody<'req, 'd>,
                #error,
            >
            where
                'd: 'req,
            {
                #value
            }
        };
        (error, parsed, param)
    }

    pub(super) fn request(&self) -> proc_macro2::TokenStream {
        let req = local("req");
        match &self.length {
            Some(_) => quote::quote!(#req),
            None => quote::quote!(_),
        }
    }

    pub(super) fn input(&self) -> proc_macro2::TokenStream {
        let body = local("parsed_body");
        match &self.source {
            Source::Raw(_) | Source::Json(_) => quote::quote!(#body),
            Source::Discarded => quote::quote!(_),
        }
    }
}
