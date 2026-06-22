use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Fields, Ident, ItemStruct, LitByteStr, LitStr, Result, Token};

use crate::util::{AttributeSliceExt, TypeExt};

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
        if ident == "raw" {
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
                if !input.is_empty() {
                    let extra = input.parse::<Ident>()?;
                    return Err(Error::new_spanned(
                        extra,
                        "#[sark_gen::response] supports only `raw`",
                    ));
                }
            }
            return Ok(Self::Raw);
        }
        Err(Error::new_spanned(
            ident,
            "#[sark_gen::response] supports only `raw`",
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
        let has_local = match &st.fields {
            Fields::Named(fields) => fields
                .named
                .iter()
                .any(|f| f.ty.is_plain_ident("LocalFrameBytes")),
            _ => false,
        };
        let inner_name = if has_local {
            syn::Ident::new(&format!("{}Inner", public_name), public_name.span())
        } else {
            public_name.clone()
        };
        if has_local {
            st.ident = inner_name.clone();
        }
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
                body_is_static_slice = field.ty.is_static_byte_slice();
            }
            if let Some(header) = field.attrs.header_name()? {
                dynamic.push((ident.clone(), header));
            }
            field.attrs.retain(|attr| !attr.path().is_ident("header"));
            if field.ty.is_plain_ident("LocalFrameBytes") {
                field.ty = syn::parse_quote!(::sark::sark_core::http::LocalFrameBytesRef<'req>);
            }
        }
        if has_local
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

        let headers = HeaderEmit::new(has_local, &dynamic, &static_headers);

        let body_build = match self {
            Mode::Json => quote! {
                let __resp_body = ::sark::json::JsonEncode::encode_json(&#body_ident);
            },
            Mode::Raw => quote! {
                let __resp_body = #body_ident;
            },
        };

        let alias_decl = if has_local {
            quote! { #vis type #public_name = #inner_name<'static>; }
        } else {
            quote! {}
        };
        let (impl_generics, ty_lifetime, fixed_ret) = if has_local {
            (
                quote!(<'req>),
                quote!(<'req>),
                quote!(::sark::sark_core::http::FixedResponseInner<'req>),
            )
        } else {
            (
                quote!(),
                quote!(),
                quote!(::sark::sark_core::http::FixedResponse),
            )
        };
        let serve_ret = if has_local {
            quote!(::sark::sark_core::http::ServeInner<'req>)
        } else {
            quote!(::sark::sark_core::http::Serve)
        };
        let serve_lt = if has_local {
            quote!('req)
        } else {
            quote!('static)
        };
        let destructure = quote! { let Self { #( #all_fields, )* } = self; };
        let headers_build = headers.build_expr();
        let static_wire = &headers.static_wire;
        let into_fixed_body = quote! {
            #destructure
            #body_build
            #headers_build
            ::sark::sark_core::http::FixedResponseInner::direct(
                #status_ident,
                #static_wire,
                __resp_headers,
                __resp_body,
            )
        };
        let static_slice_emit = if body_is_static_slice {
            quote! {
                impl #impl_generics #inner_name #ty_lifetime {
                    #vis fn into_mono_static_slice(
                        self,
                    ) -> ::sark::sark_core::http::MonoResponseInner<#serve_lt> {
                        #destructure
                        #headers_build
                        ::sark::sark_core::http::MonoResponseInner::from_static_slice_body(
                            #status_ident,
                            #static_wire,
                            __resp_headers,
                            #body_ident,
                        )
                    }
                }

                impl #impl_generics
                    ::sark::sark_core::http::IntoServeResponseStatic<#serve_lt>
                    for #inner_name #ty_lifetime
                {
                    fn into_serve_response_static(
                        self,
                    ) -> ::sark::sark_core::http::ServeInner<#serve_lt> {
                        ::sark::sark_core::http::ServeInner::Mono(
                            self.into_mono_static_slice(),
                        )
                    }
                }
            }
        } else {
            quote!()
        };

        Ok(quote! {
            #st

            #alias_decl

            impl #impl_generics #inner_name #ty_lifetime {
                #vis fn into_fixed(self) -> #fixed_ret {
                    #into_fixed_body
                }
            }

            impl #impl_generics ::sark::sark_core::http::IntoServeResponse<#serve_lt> for #inner_name #ty_lifetime {
                fn into_serve_response(self) -> #serve_ret {
                    ::sark::sark_core::http::ServeInner::Fixed(self.into_fixed())
                }
            }

            impl #impl_generics From<#inner_name #ty_lifetime> for #fixed_ret {
                fn from(value: #inner_name #ty_lifetime) -> #fixed_ret {
                    value.into_fixed()
                }
            }

            #static_slice_emit
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
        has_local: bool,
        dynamic: &[(syn::Ident, LitStr)],
        static_headers: &[(LitStr, LitStr)],
    ) -> Self {
        let (item_path, headers_path) = if has_local {
            (
                quote!(::sark::sark_core::http::HeaderItemInner::<'req>),
                quote!(::sark::sark_core::http::HeadersInner::<'req>),
            )
        } else {
            (
                quote!(::sark::sark_core::http::HeaderItem),
                quote!(::sark::sark_core::http::Headers),
            )
        };
        let dyn_items = dynamic
            .iter()
            .map(|(ident, header)| {
                let header_name = LitStr::new(&header.value(), header.span());
                quote! {
                    #item_path::from_value(
                        ::sark::sark_core::http::HeaderNameToken::new(#header_name),
                        #ident,
                    )
                }
            })
            .collect();
        let mut wire = Vec::new();
        for (name, value) in static_headers {
            let name = name.value();
            let value = value.value();
            wire.extend_from_slice(name.as_bytes());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(value.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        let static_wire = LitByteStr::new(&wire, Span::call_site());
        Self {
            headers_path,
            dyn_items,
            static_wire,
        }
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
