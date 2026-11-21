use crate::{macros::tokens::local, runtime::paths::Paths, types};

pub(super) mod task;

pub(super) enum Source {
    Discarded,
    Raw { ident: syn::Ident, segmented: bool },
    Json(syn::Type),
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
        let Source::Json(ty) = &mut self.source else {
            return None;
        };
        let name = quote::format_ident!("__{}Json", request);
        let declaration = quote::quote!(type #name = #ty;);
        *ty = syn::parse_quote!(#name);
        Some((name, declaration))
    }

    pub(super) fn storage(
        &self,
        paths: &Paths,
        named: &mut syn::FieldsNamed,
        vis: &syn::Visibility,
        param: &syn::Ident,
    ) -> Option<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
        match &self.source {
            Source::Raw {
                segmented: true, ..
            } => {
                let http = &paths.http;
                Some((
                    quote::quote!(#http::request::Segments<'d>),
                    quote::quote!(#http::request::body::Ref<'req, 'd>),
                ))
            }
            Source::Json(ty) => {
                let json = &paths.json;
                named.named.push(syn::parse_quote!(#vis body: #param));
                Some((
                    quote::quote!(<#ty as #json::segment::IntoRequest>::Request<'d>),
                    quote::quote!(<#ty as #json::parsing::Request>::View<'req>),
                ))
            }
            Source::Discarded
            | Source::Raw {
                segmented: false, ..
            } => None,
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
            source: json.map_or(Source::Discarded, Source::Json),
            length: None,
        })
    }

    pub(super) fn try_raw(
        self,
        param: &syn::Ident,
        ident: syn::Ident,
        ty: &mut syn::Type,
    ) -> syn::Result<Self> {
        let source = match self.source {
            Source::Discarded => {
                let view = types::View::new(ty);
                let segmented = view.is_named("Segments");
                if segmented {
                    *ty = syn::parse_quote!(#param);
                } else if !view.is_named("Shared") {
                    return Err(syn::Error::new_spanned(
                        ty,
                        "#[raw_body] field type must be owned::Segments or Shared",
                    ));
                }
                Source::Raw { ident, segmented }
            }
            Source::Raw { ident: first, .. } => {
                let mut error = syn::Error::new_spanned(ty, "duplicate #[raw_body]");
                error.combine(syn::Error::new_spanned(
                    first,
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
                    json,
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

    pub(super) fn fields(
        &self,
        paths: &Paths,
        body: syn::Ident,
        len: proc_macro2::TokenStream,
    ) -> Vec<proc_macro2::TokenStream> {
        let ns_core = &paths.core;
        let mut fields = Vec::with_capacity(2);
        match &self.source {
            Source::Raw { ident, .. } => fields.push(quote::quote!(#ident: #body)),
            Source::Json(_) => fields.push(quote::quote!(body: #body)),
            Source::Discarded => {}
        }
        if let Some(ident) = &self.length {
            fields.push(quote::quote!(#ident: #ns_core::request::BodyLen::new(#len)));
        }
        fields
    }

    pub(super) fn task(&self, paths: &Paths) -> (proc_macro2::TokenStream, &Source) {
        let ns_core = &paths.core;
        let body = match &self.source {
            Source::Raw {
                segmented: true, ..
            } => quote::quote!(#ns_core::body::Segmented),
            Source::Json(_)
            | Source::Raw {
                segmented: false, ..
            } => {
                quote::quote!(#ns_core::body::Contiguous)
            }
            Source::Discarded => quote::quote!(#ns_core::body::Discarded),
        };
        (body, &self.source)
    }

    pub(super) fn project(
        &self,
        paths: &Paths,
        path_error: &proc_macro2::TokenStream,
        input: &syn::Ident,
        fields: &mut Vec<proc_macro2::TokenStream>,
    ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let ns_core = &paths.core;
        let ns_json = &paths.json;
        let parsed = local("__sark_parsed_body");
        let len = local("__sark_body_len");
        let length = self
            .length
            .as_ref()
            .map(|_| quote::quote!(let #len = #input.body().len();));
        let (error, read) = match &self.source {
            Source::Json(ty) => (
                quote::quote!(#ns_json::parsing::Error<'req>),
                quote::quote! {
                    let #parsed = <#ty as #ns_json::parsing::Request>::decode(
                        #input.into_inner().as_slice(),
                    ).map_err(#ns_core::route::Input::Body)?;
                },
            ),
            Source::Raw { segmented, .. } => {
                let slice = (!*segmented).then(|| quote::quote!(.as_slice()));
                (
                    quote::quote!(::core::convert::Infallible),
                    quote::quote!(let #parsed = #input.into_inner()#slice;),
                )
            }
            Source::Discarded => (
                quote::quote!(::core::convert::Infallible),
                proc_macro2::TokenStream::new(),
            ),
        };
        fields.extend(self.fields(paths, parsed, quote::quote!(#len)));
        (
            quote::quote!(#ns_core::route::Input<#path_error, #error>),
            quote::quote!(#length #read),
        )
    }
}
