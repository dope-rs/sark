use crate::{macros::tokens::local, request::fields, runtime::paths::Paths, types};

pub(super) struct Expansion {
    pub(super) declaration: proc_macro2::TokenStream,
    pub(super) code: proc_macro2::TokenStream,
    pub(super) error: proc_macro2::TokenStream,
    pub(super) start: proc_macro2::TokenStream,
    pub(super) reject: proc_macro2::TokenStream,
    pub(super) view: proc_macro2::TokenStream,
}

impl Expansion {
    pub(super) fn try_new(
        paths: &Paths,
        fields: &[(&fields::Path, types::ValueKind)],
        vis: &syn::Visibility,
        schema: &syn::Ident,
        inner: &syn::Ident,
        raw: &syn::Ident,
        symbols: &types::Generics,
    ) -> syn::Result<Self> {
        let ns_core = &paths.core;
        let [v_params, v_err, v_rqst, v_req, v_caps] =
            ["params", "error", "request", "req", "captures"].map(local);
        if fields.is_empty() {
            return Ok(Self {
                declaration: proc_macro2::TokenStream::new(),
                code: proc_macro2::TokenStream::new(),
                error: quote::quote!(::core::convert::Infallible),
                start: quote::quote!(let #v_params = ();),
                reject: quote::quote!(match #v_err {}),
                view: proc_macro2::TokenStream::new(),
            });
        }
        let req = symbols.local("req");
        let marker = symbols.ident("__sark_m");
        let names: Vec<&syn::Ident> = fields.iter().map(|(field, _)| &field.ident).collect();
        let locals: Vec<_> = (0..names.len())
            .map(|index| symbols.local(&format!("__sark_path{index}")))
            .collect();
        let keys: Vec<syn::LitByteStr> = fields
            .iter()
            .map(|(field, _)| {
                syn::LitByteStr::new(field.path.value().as_bytes(), field.path.span())
            })
            .collect();
        let mode = quote::format_ident!("M");
        let modes: Vec<syn::Type> = fields
            .iter()
            .map(|(field, _)| {
                let mut ty = field.ty.clone();
                types::Edit::new(&mut ty).retained(paths, types::RetainedAs::Mode(&mode));
                ty
            })
            .collect();
        let fallible = fields
            .iter()
            .any(|(_, kind)| matches!(kind, types::ValueKind::Usize));
        let raws = vec![quote::quote!(#ns_core::route::raw::PathCapture); names.len()];
        let values: Vec<_> = fields
            .iter()
            .zip(&locals)
            .enumerate()
            .map(|(idx, (field, local))| Self::field(paths, idx, field, local, schema, false, &req))
            .collect::<syn::Result<_>>()?;
        let owned: Vec<_> = fields
            .iter()
            .zip(&locals)
            .enumerate()
            .map(|(idx, (field, local))| Self::field(paths, idx, field, local, schema, true, &req))
            .collect::<syn::Result<_>>()?;
        let captures: Vec<syn::Ident> = (0..fields.len())
            .map(|idx| symbols.local(&format!("cap{idx}")))
            .collect();
        let cap_ty = vec![quote::quote!(#ns_core::route::raw::PathCapture); captures.len()];
        let frame = quote::quote!(<#schema<'static> as #ns_core::route::Schema>::Frame);
        let body = quote::quote! {
            let #raw { #( #names: #locals, )* } = *#req.params();
            Self {
                #( #values, )*
                #marker: ::core::marker::PhantomData,
            }
        };
        let bind = if fallible {
            quote::quote! {
                fn try_from_head(
                    #req: &#ns_core::request::raw::Parsed<
                        #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, #schema<'static>,
                    >,
                ) -> ::core::result::Result<Self, <#schema<'static> as #ns_core::route::Schema>::Error> {
                    ::core::result::Result::Ok({ #body })
                }
            }
        } else {
            quote::quote! {
                fn from_head(
                    #req: &#ns_core::request::raw::Parsed<
                        #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, #schema<'static>,
                    >,
                ) -> Self {
                    #body
                }
            }
        };
        let start = quote::quote! {
            let #v_params = {
                let #req = &#v_rqst;
                let #raw { #( #names: #locals, )* } = *#req.params();
                #inner::<'d, #ns_core::request::mode::Own> {
                    #( #owned, )*
                    #marker: ::core::marker::PhantomData,
                }
            };
        };
        let (error, reject, view) = if fallible {
            (
                quote::quote!(<#schema<'static> as #ns_core::route::Schema>::Error),
                quote::quote!(#v_err.reject()),
                quote::quote! {
                    #inner::<'req, #ns_core::request::mode::Ref<'req>>
                        ::try_from_head(#v_req)
                        .map_err(#ns_core::route::Input::Params)
                },
            )
        } else {
            (
                quote::quote!(::core::convert::Infallible),
                quote::quote!(match #v_err {}),
                quote::quote! {
                    ::core::result::Result::Ok(
                        #inner::<'req, #ns_core::request::mode::Ref<'req>>
                            ::from_head(#v_req),
                    )
                },
            )
        };
        let declaration = quote::quote! {
            #vis struct #inner<
                'd,
                M: #ns_core::request::mode::Mode<'d> =
                    #ns_core::request::mode::Ref<'d>,
            > {
                #( pub #names: #modes, )*
                #[doc(hidden)]
                pub #marker: ::core::marker::PhantomData<fn(&'d (), M) -> (&'d (), M)>,
            }

            #[derive(::core::clone::Clone, ::core::marker::Copy)]
            #vis struct #raw { #( #names: #raws, )* }
        };
        let code = quote::quote! {
            impl #ns_core::route::RawParams for #raw {
                type Captures = ( #( #cap_ty, )* );
                const PATH_NAMES: &'static [&'static [::core::primitive::u8]] = &[ #( #keys, )* ];
            }

            impl #ns_core::route::Params for #raw {
                fn from_match(#v_caps: Self::Captures) -> Self {
                    let ( #( #captures, )* ) = #v_caps;
                    Self { #( #names: #captures, )* }
                }
            }

            impl<'req> #inner<
                'req,
                #ns_core::request::mode::Ref<'req>,
            > {
                #bind
            }

        };
        Ok(Self {
            declaration,
            code,
            error,
            start,
            reject,
            view,
        })
    }

    fn field(
        paths: &Paths,
        idx: usize,
        (field, kind): &(&fields::Path, types::ValueKind),
        value: &syn::Ident,
        schema: &syn::Ident,
        owned: bool,
        req: &syn::Ident,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let ns_core = &paths.core;
        let ident = &field.ident;
        let cause = local("cause");
        Ok(match kind {
            types::ValueKind::Bytes => {
                let read = if owned {
                    quote::quote!(try_get().map_err(#ns_core::route::Input::Retain)?)
                } else {
                    quote::quote!(get())
                };
                quote::quote! {
                    #ident: unsafe {
                        #ns_core::request::frame::raw::Path::new(#req, #value)
                    }.#read
                }
            }
            types::ValueKind::Usize => {
                let slot = quote::format_ident!("Path{idx}");
                let convert =
                    owned.then(|| quote::quote!(.map_err(#ns_core::route::Input::Params)));
                quote::quote! {
                    #ident: unsafe {
                        #ns_core::request::frame::raw::Path::new(#req, #value)
                    }
                    .try_parse()
                    .map_err(|#cause| #ns_core::route::Error::Path {
                        field: <<#schema<'static> as #ns_core::route::Fields>::Identity>::#slot,
                        cause: #cause,
                    })#convert?
                }
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &field.ty,
                    "path fields require Shared or usize",
                ));
            }
        })
    }
}
