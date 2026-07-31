use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Fields, Ident, ItemStruct, LitByteStr, LitStr, Result, Token, Type};

use crate::lifetimes::TypeLifetimes;
use crate::util::{AttributeSliceExt, TypeExt};

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Json,
    Raw,
    Encoded,
}

impl Parse for Mode {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.is_empty() {
            return Ok(Self::Json);
        }
        let ident = input.parse::<Ident>()?;
        if ident == "json" {
            if !input.is_empty() {
                return Err(Error::new_spanned(
                    input.parse::<TokenStream>()?,
                    "#[sark_gen::response] supports only `json`, `raw`, or `encoded`",
                ));
            }
            return Ok(Self::Json);
        }
        if ident == "raw" {
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
                if !input.is_empty() {
                    let extra = input.parse::<Ident>()?;
                    return Err(Error::new_spanned(
                        extra,
                        "#[sark_gen::response] supports only `json`, `raw`, or `encoded`",
                    ));
                }
            }
            return Ok(Self::Raw);
        }
        if ident == "encoded" {
            if !input.is_empty() {
                return Err(Error::new_spanned(
                    input.parse::<TokenStream>()?,
                    "#[sark_gen::response] supports only `json`, `raw`, or `encoded`",
                ));
            }
            return Ok(Self::Encoded);
        }
        Err(Error::new_spanned(
            ident,
            "#[sark_gen::response] supports only `json`, `raw`, or `encoded`",
        ))
    }
}

impl Mode {
    pub(super) fn expand(self, mut st: ItemStruct) -> Result<TokenStream> {
        let public_name = st.ident.clone();
        let vis = st.vis.clone();
        let static_headers = st.attrs.static_headers()?;
        st.attrs.retain(|attr| {
            !attr.path().is_ident("header") && !attr.path().is_ident("header_static")
        });
        // Static handlers consume their response value during macro expansion, so the
        // source-level response struct can legitimately have no runtime constructor.
        st.attrs.push(syn::parse_quote!(#[allow(dead_code)]));
        let has_borrowed = match &st.fields {
            Fields::Named(fields) => fields.named.iter().any(|field| {
                field.ty.has_borrowed_bytes() || TypeLifetimes::new(&field.ty).has_non_static()
            }),
            _ => false,
        };
        let inner_name = public_name.clone();
        let fields = match &mut st.fields {
            Fields::Named(fields) => &mut fields.named,
            _ => {
                return Err(Error::new_spanned(
                    st.struct_token,
                    "#[sark_gen::response] requires a struct with named fields",
                ));
            }
        };

        let mut status_ident = None::<syn::Ident>;
        let mut body_ident = None::<syn::Ident>;
        let mut body_ty = None::<Type>;
        let mut body_is_static_slice = false;
        let mut dynamic = Vec::new();
        let mut all_fields = Vec::new();
        for field in fields.iter_mut() {
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| Error::new(Span::call_site(), "named field required"))?;
            all_fields.push(ident.clone());
            if ident == "status" {
                status_ident = Some(ident.clone());
            }
            if ident == "body" {
                body_ident = Some(ident.clone());
                body_ty = Some(field.ty.clone());
                body_is_static_slice = field.ty.is_static_byte_slice();
            }
            if let Some(header) = field.attrs.header_name()? {
                dynamic.push((ident.clone(), header));
            }
            field.attrs.retain(|attr| !attr.path().is_ident("header"));
        }
        if has_borrowed
            && !st
                .generics
                .params
                .iter()
                .any(|p| matches!(p, syn::GenericParam::Lifetime(lt) if lt.lifetime.ident == "req"))
        {
            st.generics.params.insert(0, syn::parse_quote!('req));
        }
        let status_ident = status_ident.ok_or_else(|| {
            Error::new(
                Span::call_site(),
                "#[sark_gen::response] requires `status` field",
            )
        })?;
        let body_ident = body_ident.ok_or_else(|| {
            Error::new(
                Span::call_site(),
                "#[sark_gen::response] requires `body` field",
            )
        })?;
        let body_ty = body_ty.ok_or_else(|| {
            Error::new(
                Span::call_site(),
                "#[sark_gen::response] requires `body` field",
            )
        })?;

        let header_count = dynamic.len();
        let headers = HeaderEmit::new(has_borrowed, &dynamic, &static_headers)?;
        let static_header_fields = quote!(::sark::sark_core::http::StaticHeaderFields);

        let body_build = match self {
            Mode::Json => quote! {
                let __resp_body = ::sark::json::JsonBody::new(#body_ident);
            },
            Mode::Raw | Mode::Encoded => quote! {
                let __resp_body = #body_ident;
            },
        };

        let (impl_generics, ty_lifetime, serve_lt) = if has_borrowed {
            (quote!(<'req>), quote!(<'req>), quote!('req))
        } else {
            (quote!(), quote!(), quote!('static))
        };
        let fixed_ret = match self {
            Mode::Json => quote! {
                ::sark::sark_core::http::EncodedResponse<
                    #serve_lt,
                    ::sark::json::JsonBody<#body_ty>,
                    #header_count,
                    #static_header_fields,
                >
            },
            Mode::Raw if has_borrowed => {
                quote!(
                    ::sark::sark_core::http::FixedResponse<
                        'req,
                        #header_count,
                        #static_header_fields,
                    >
                )
            }
            Mode::Raw => {
                quote!(
                    ::sark::sark_core::http::FixedResponse<
                        'static,
                        #header_count,
                        #static_header_fields,
                    >
                )
            }
            Mode::Encoded => quote! {
                ::sark::sark_core::http::EncodedResponse<
                    #serve_lt,
                    #body_ty,
                    #header_count,
                    #static_header_fields,
                >
            },
        };
        let destructure = quote! { let Self { #( #all_fields, )* } = self; };
        let headers_build = headers.build_expr();
        let response_ctor = match self {
            Mode::Json => quote!(::sark::sark_core::http::EncodedResponse::structured),
            Mode::Raw => quote!(::sark::sark_core::http::FixedResponse::structured),
            Mode::Encoded => quote!(::sark::sark_core::http::EncodedResponse::structured),
        };
        let into_fixed_body = quote! {
            #destructure
            #body_build
            #headers_build
            #response_ctor(
                #status_ident,
                &__RESP_HEADER_TEMPLATE,
                __resp_headers,
                __resp_body,
            )
        };
        let fixed_api = if matches!(self, Mode::Raw) && body_is_static_slice {
            quote!()
        } else {
            quote! {
                impl #impl_generics #inner_name #ty_lifetime {
                    #vis fn into_fixed(self) -> #fixed_ret {
                        #into_fixed_body
                    }
                }

                impl #impl_generics From<#inner_name #ty_lifetime> for #fixed_ret {
                    fn from(value: #inner_name #ty_lifetime) -> #fixed_ret {
                        value.into_fixed()
                    }
                }
            }
        };
        let static_slice_emit = if matches!(self, Mode::Raw) && body_is_static_slice {
            quote! {
                impl #impl_generics #inner_name #ty_lifetime {
                    #vis fn into_static_response(
                        self,
                    ) -> ::sark::sark_core::http::StaticResponseInner<
                        #serve_lt,
                        #header_count,
                        #static_header_fields,
                    > {
                        #destructure
                        #headers_build
                        ::sark::sark_core::http::StaticResponseInner::structured(
                            #status_ident,
                            &__RESP_HEADER_TEMPLATE,
                            __resp_headers,
                            #body_ident,
                        )
                    }
                }

            }
        } else {
            quote!()
        };
        let owned_shape_impl = if !has_borrowed && st.generics.params.is_empty() {
            quote! {
                impl ::sark::sark_core::http::__private::OwnedResponse for #inner_name {}
            }
        } else {
            quote!()
        };
        let (native_impl_generics, native_serve_lt, native_target, native_shape, into_native) =
            if matches!(self, Mode::Json | Mode::Encoded) {
                (
                    impl_generics.clone(),
                    serve_lt.clone(),
                    quote!(#inner_name #ty_lifetime),
                    fixed_ret.clone(),
                    quote!(self.into_fixed()),
                )
            } else {
                let (native_shape, into_native) = if body_is_static_slice {
                    (
                        quote!(
                            ::sark::sark_core::http::StaticResponseInner<
                                'req,
                                #header_count,
                                #static_header_fields,
                            >
                        ),
                        quote!(self.into_static_response()),
                    )
                } else {
                    (
                        quote!(
                            ::sark::sark_core::http::FixedResponse<
                                'req,
                                #header_count,
                                #static_header_fields,
                            >
                        ),
                        quote!(self.into_fixed()),
                    )
                };
                let native_target = if has_borrowed {
                    quote!(#inner_name<'req>)
                } else {
                    quote!(#inner_name)
                };
                (
                    quote!(<'req>),
                    quote!('req),
                    native_target,
                    native_shape,
                    into_native,
                )
            };
        let native_response_impl = quote! {
            impl #native_impl_generics
                ::sark::sark_core::http::IntoResponseShape<#native_serve_lt> for #native_target
            {
                type Shape = #native_shape;

                fn into_response_shape(self) -> Self::Shape {
                    #into_native
                }
            }
        };

        Ok(quote! {
            #st

            #fixed_api
            #static_slice_emit

            #owned_shape_impl

            #native_response_impl
        })
    }
}

struct HeaderEmit {
    headers_path: TokenStream,
    dyn_items: Vec<TokenStream>,
    static_wire: LitByteStr,
    static_fields: Vec<(LitByteStr, LitByteStr)>,
}

impl HeaderEmit {
    fn new(
        has_borrowed: bool,
        dynamic: &[(syn::Ident, LitStr)],
        static_headers: &[(LitStr, LitStr)],
    ) -> Result<Self> {
        let header_count = dynamic.len();
        if header_count > usize::from(u8::MAX) {
            return Err(Error::new(
                dynamic[usize::from(u8::MAX)].1.span(),
                "response supports at most 255 dynamic headers",
            ));
        }
        let (item_path, headers_path) = if has_borrowed {
            (
                quote!(::sark::sark_core::http::HeaderItem::<'req>),
                quote!(::sark::sark_core::http::Headers::<'req, #header_count>),
            )
        } else {
            (
                quote!(::sark::sark_core::http::HeaderItem),
                quote!(::sark::sark_core::http::Headers::<'static, #header_count>),
            )
        };
        let mut dyn_items = Vec::with_capacity(dynamic.len());
        for (ident, header_name) in dynamic {
            validate_header_name(header_name)?;
            dyn_items.push(quote! {
                #item_path::from_value(
                    const {
                        ::sark::sark_core::http::HeaderNameToken::new(#header_name)
                    },
                    #ident,
                )
            });
        }
        let mut wire = Vec::new();
        let mut static_fields = Vec::with_capacity(static_headers.len());
        for (name, value) in static_headers {
            validate_header_name(name)?;
            validate_header_value(value)?;
            let name = LitByteStr::new(name.value().as_bytes(), name.span());
            let value = LitByteStr::new(value.value().as_bytes(), value.span());
            wire.extend_from_slice(&name.value());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(&value.value());
            wire.extend_from_slice(b"\r\n");
            static_fields.push((name, value));
        }
        let static_wire = LitByteStr::new(&wire, Span::call_site());
        Ok(Self {
            headers_path,
            dyn_items,
            static_wire,
            static_fields,
        })
    }

    fn build_expr(&self) -> TokenStream {
        let headers_path = &self.headers_path;
        let items = &self.dyn_items;
        let static_wire = &self.static_wire;
        let static_fields = self
            .static_fields
            .iter()
            .map(|(name, value)| quote!(::sark::sark_core::http::Field::new(#name, #value)));
        quote! {
            const __RESP_HEADER_TEMPLATE: ::sark::sark_core::http::HeaderTemplate =
                ::sark::sark_core::http::HeaderTemplate::new(
                    #static_wire,
                    &[ #( #static_fields, )* ],
                );
            let __resp_headers = #headers_path::from_items([
                #( #items, )*
            ]);
        }
    }
}

fn validate_header_name(name: &LitStr) -> Result<()> {
    match sark_protocol::validate_response_header_name(&name.value()) {
        Ok(()) => Ok(()),
        Err(sark_protocol::ResponseHeaderNameError::Empty) => Err(Error::new(
            name.span(),
            "response header name must not be empty",
        )),
        Err(sark_protocol::ResponseHeaderNameError::InvalidByte { index, byte }) => {
            Err(Error::new(
                name.span(),
                format!(
                    "response header name contains invalid HTTP token byte 0x{byte:02x} at byte {index}"
                ),
            ))
        }
        Err(sark_protocol::ResponseHeaderNameError::Managed) => Err(Error::new(
            name.span(),
            format!(
                "response header `{}` is managed by Sark and cannot be overridden",
                name.value()
            ),
        )),
    }
}

fn validate_header_value(value: &LitStr) -> Result<()> {
    match sark_protocol::validate_header_value(value.value().as_bytes()) {
        Ok(()) => Ok(()),
        Err(error) => Err(Error::new(
            value.span(),
            format!(
                "static response header value contains CR/LF at byte {}",
                error.index
            ),
        )),
    }
}
