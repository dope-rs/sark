use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Fields, Ident, ItemStruct, LitByteStr, LitStr, Result, Token, Type};

use crate::util::{AttributeSliceExt, TypeExt};

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Json,
    Raw,
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
                    "#[sark_gen::response] supports only `json` or `raw`",
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
                        "#[sark_gen::response] supports only `json` or `raw`",
                    ));
                }
            }
            return Ok(Self::Raw);
        }
        Err(Error::new_spanned(
            ident,
            "#[sark_gen::response] supports only `json` or `raw`",
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
            Fields::Named(fields) => fields
                .named
                .iter()
                .any(|field| field.ty.has_borrowed_bytes()),
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
        let mut all_field_types = Vec::new();
        for field in fields.iter_mut() {
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| Error::new(Span::call_site(), "named field required"))?;
            all_fields.push(ident.clone());
            all_field_types.push(field.ty.clone());
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

        let body_build = match self {
            Mode::Json => quote! {
                let __resp_body = ::sark::json::JsonBody::new(#body_ident);
            },
            Mode::Raw => quote! {
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
                ::sark::sark_core::http::EncodedResponseInner<
                    #serve_lt,
                    ::sark::json::JsonBody<#body_ty>,
                    #header_count,
                >
            },
            Mode::Raw if has_borrowed => {
                quote!(::sark::sark_core::http::FixedResponseInner<'req, #header_count>)
            }
            Mode::Raw => {
                quote!(::sark::sark_core::http::FixedResponseInner<'static, #header_count>)
            }
        };
        let destructure = quote! { let Self { #( #all_fields, )* } = self; };
        let headers_build = headers.build_expr();
        let static_wire = &headers.static_wire;
        let response_ctor = match self {
            Mode::Json => quote!(::sark::sark_core::http::EncodedResponseInner::direct),
            Mode::Raw => quote!(::sark::sark_core::http::FixedResponseInner::direct),
        };
        let into_fixed_body = quote! {
            #destructure
            #body_build
            #headers_build
            #response_ctor(#status_ident, #static_wire, __resp_headers, __resp_body)
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
                    ) -> ::sark::sark_core::http::StaticResponseInner<#serve_lt, #header_count> {
                        #destructure
                        #headers_build
                        ::sark::sark_core::http::StaticResponseInner::direct(
                            #status_ident,
                            #static_wire,
                            __resp_headers,
                            #body_ident,
                        )
                    }
                }

            }
        } else {
            quote!()
        };
        let native_body_kind = if body_is_static_slice {
            quote!(::sark::sark_core::http::body_kind::ResponseKind::Static)
        } else {
            quote!(::sark::sark_core::http::body_kind::ResponseKind::Inline)
        };
        let owned_shape_impl = if !has_borrowed && st.generics.params.is_empty() {
            let (owned_shape, into_shape) = if matches!(self, Mode::Json) {
                (quote!(#fixed_ret), quote!(self.into_fixed()))
            } else if body_is_static_slice {
                (
                    quote!(::sark::sark_core::http::StaticResponseInner<'static, #header_count>),
                    quote!(self.into_static_response()),
                )
            } else {
                (quote!(#fixed_ret), quote!(self.into_fixed()))
            };
            quote! {
                impl ::sark::sark_core::http::__private::GeneratedResponse for #inner_name {
                    type Fields = ( #( #all_field_types, )* );
                    type Shape = #owned_shape;

                    const BODY_KIND: ::sark::sark_core::http::body_kind::ResponseKind =
                        #native_body_kind;

                    fn into_owned_shape(self) -> Self::Shape {
                        #into_shape
                    }
                }
            }
        } else {
            quote!()
        };
        let native_response_impl = if matches!(self, Mode::Json) {
            quote! {
                impl #impl_generics ::sark::service::manifold::NativeResponse<#serve_lt>
                    for #inner_name #ty_lifetime
                {
                    type Kind = ::sark::service::manifold::Sync;
                    type Shape = #fixed_ret;
                    type Stream = ::sark::sark_core::http::NeverStream;

                    const BODY_KIND: ::sark::sark_core::http::body_kind::ResponseKind =
                        #native_body_kind;

                    fn into_route_response(self) -> Self::Shape {
                        self.into_fixed()
                    }
                }
            }
        } else if has_borrowed {
            if body_is_static_slice {
                quote! {
                    impl<'req> ::sark::service::manifold::NativeResponse<'req>
                        for #inner_name<'req>
                    {
                        type Kind = ::sark::service::manifold::Sync;
                        type Shape = ::sark::sark_core::http::StaticResponseInner<'req, #header_count>;
                        type Stream = ::sark::sark_core::http::NeverStream;

                        const BODY_KIND: ::sark::sark_core::http::body_kind::ResponseKind =
                            #native_body_kind;

                        fn into_route_response(self) -> Self::Shape {
                            self.into_static_response()
                        }
                    }
                }
            } else {
                quote! {
                    impl<'req> ::sark::service::manifold::NativeResponse<'req>
                        for #inner_name<'req>
                    {
                        type Kind = ::sark::service::manifold::Sync;
                        type Shape = ::sark::sark_core::http::FixedResponseInner<'req, #header_count>;
                        type Stream = ::sark::sark_core::http::NeverStream;

                        const BODY_KIND: ::sark::sark_core::http::body_kind::ResponseKind =
                            #native_body_kind;

                        fn into_route_response(self) -> Self::Shape {
                            self.into_fixed()
                        }
                    }
                }
            }
        } else {
            let (native_shape, into_native) = if body_is_static_slice {
                (
                    quote!(::sark::sark_core::http::StaticResponseInner<'req, #header_count>),
                    quote!(self.into_static_response()),
                )
            } else {
                (
                    quote!(::sark::sark_core::http::FixedResponseInner<'req, #header_count>),
                    quote!(self.into_fixed()),
                )
            };
            quote! {
                impl<'req> ::sark::service::manifold::NativeResponse<'req> for #inner_name {
                    type Kind = ::sark::service::manifold::Sync;
                    type Shape = #native_shape;
                    type Stream = ::sark::sark_core::http::NeverStream;

                    const BODY_KIND: ::sark::sark_core::http::body_kind::ResponseKind =
                        #native_body_kind;

                    fn into_route_response(self) -> Self::Shape {
                        #into_native
                    }
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
                quote!(::sark::sark_core::http::HeaderItemInner::<'req>),
                quote!(::sark::sark_core::http::HeadersInner::<'req, #header_count>),
            )
        } else {
            (
                quote!(::sark::sark_core::http::HeaderItem),
                quote!(::sark::sark_core::http::HeadersInner::<'static, #header_count>),
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
        for (name, value) in static_headers {
            validate_header_name(name)?;
            validate_header_value(value)?;
            let name = name.value();
            let value = value.value();
            wire.extend_from_slice(name.as_bytes());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(value.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        let static_wire = LitByteStr::new(&wire, Span::call_site());
        Ok(Self {
            headers_path,
            dyn_items,
            static_wire,
        })
    }

    fn build_expr(&self) -> TokenStream {
        let headers_path = &self.headers_path;
        let items = &self.dyn_items;
        quote! {
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
