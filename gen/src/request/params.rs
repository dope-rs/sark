use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Ident, LitStr, Result, Type};

use crate::codegen::value::Value;
use crate::model::PathAttrField;
use crate::util::{TypeExt, ValueKind};

pub(super) struct Params<'a> {
    pub(super) vis: &'a syn::Visibility,
    pub(super) inner_ident: &'a Ident,
    pub(super) raw_ident: &'a Ident,
    pub(super) fields: &'a [PathAttrField],
}

impl<'a> Params<'a> {
    pub(super) fn build(self) -> Result<TokenStream> {
        let Self {
            vis,
            inner_ident,
            raw_ident,
            fields,
        } = self;
        if fields.is_empty() {
            return Ok(quote! {
                #[allow(non_camel_case_types, dead_code)]
                #vis struct #inner_ident<'req> {
                    marker: ::core::marker::PhantomData<&'req ()>,
                }

                #[allow(non_camel_case_types, dead_code)]
                #[derive(Default)]
                #vis struct #raw_ident;

                impl ::sark::service::RawRouteParams for #raw_ident {
                    type Captures = ();

                    fn from_captures<P: ::sark::service::PathProbe>(
                        _path: &P,
                        _captures: Self::Captures,
                    ) -> ::core::option::Option<Self> {
                        ::core::option::Option::Some(Self)
                    }
                }

                impl<'req> ::sark::service::RouteParams<'req> for #inner_ident<'req> {
                    type Raw = #raw_ident;

                    fn from_raw(
                        _req: &::sark::request::Ref<'req>,
                        _raw: Self::Raw,
                    ) -> ::core::option::Option<Self> {
                        ::core::option::Option::Some(Self {
                            marker: ::core::marker::PhantomData,
                        })
                    }
                }
            });
        }
        let field_ident: Vec<&Ident> = fields.iter().map(|f| &f.ident).collect();
        let path_ident: Vec<Ident> = fields
            .iter()
            .map(|f| format_ident!("{}", f.path.value()))
            .collect();
        let field_ty_ref: Vec<Type> = fields
            .iter()
            .map(|f| {
                let mut ty = f.ty.clone();
                ty.rewrite_retained_to_borrowed();
                ty
            })
            .collect();
        let raw_field_ty: Vec<TokenStream> = fields
            .iter()
            .map(|f| match f.ty.value_kind()? {
                ValueKind::Bytes => Ok(quote! { Option<std::ops::Range<usize>> }),
                _ if f.ty.value_optional() => {
                    let ty = &f.ty;
                    Ok(quote! { #ty })
                }
                _ => {
                    let ty = &f.ty;
                    Ok(quote! { Option<#ty> })
                }
            })
            .collect::<Result<_>>()?;
        let build_borrowed_field_exprs: Vec<_> = fields
            .iter()
            .zip(path_ident.iter())
            .map(|(f, raw)| Self::path_field_expr(&f.ident, raw, &f.ty, f.default.as_ref(), true))
            .collect::<Result<_>>()?;
        let capture_binds: Vec<Ident> = (0..fields.len())
            .map(|idx| format_ident!("cap{}", idx))
            .collect();
        let capture_ty: Vec<TokenStream> = capture_binds
            .iter()
            .map(|_| quote!(sark::service::PathCapture))
            .collect();
        let raw_capture_inits: Vec<TokenStream> = fields
            .iter()
            .zip(path_ident.iter())
            .zip(capture_binds.iter())
            .map(|((f, raw), cap)| {
                let value = match f.ty.value_kind()? {
                    ValueKind::Bytes | ValueKind::Range => {
                        quote! { Some(#cap.start..#cap.end) }
                    }
                    _ => {
                        let inner = f.ty.value_inner();
                        quote! {
                            Some(<#inner as sark::service::FieldValue>::parse_path(
                                path, #cap.start, #cap.end,
                            )?)
                        }
                    }
                };
                Ok(quote! { #raw: #value, })
            })
            .collect::<Result<_>>()?;
        Ok(quote! {
            #[allow(non_camel_case_types, dead_code)]
            #vis struct #inner_ident<'req> {
                #( pub #field_ident: #field_ty_ref, )*
                #[doc(hidden)]
                pub __sark_m: ::core::marker::PhantomData<&'req ()>,
            }

            #[allow(non_camel_case_types)]
            #[derive(Default)]
            #vis struct #raw_ident { #( pub #path_ident: #raw_field_ty, )* }

            impl sark::service::RawRouteParams for #raw_ident {
                type Captures = ( #( #capture_ty, )* );

                fn from_captures<P: sark::service::PathProbe>(
                    path: &P,
                    captures: Self::Captures,
                ) -> Option<Self> {
                    let _ = path;
                    let ( #( #capture_binds, )* ) = captures;
                    Some(Self { #( #raw_capture_inits )* })
                }
            }

            impl<'req> sark::service::RouteParams<'req> for #inner_ident<'req> {
                type Raw = #raw_ident;

                fn from_raw(
                    req: &sark::request::Ref<'req>,
                    raw: Self::Raw,
                ) -> Option<Self> {
                    let #raw_ident { #( #path_ident, )* } = raw;
                    Some(Self {
                        #( #build_borrowed_field_exprs, )*
                        __sark_m: ::core::marker::PhantomData,
                    })
                }
            }
        })
    }

    fn path_field_expr(
        ident: &Ident,
        raw: &Ident,
        ty: &Type,
        default: Option<&LitStr>,
        borrowed: bool,
    ) -> Result<TokenStream> {
        Ok(match ty.value_kind()? {
            ValueKind::Bytes if ty.value_optional() => quote! {
                #ident: match #raw {
                    Some(range) => Some(req.path_frame(range)?),
                    None => None,
                }
            },
            ValueKind::Bytes => {
                let default = default.ok_or_else(|| {
                    Error::new_spanned(
                        ty,
                        "non-Option request path Bytes<Retained> fields require default = \"...\"",
                    )
                })?;
                let fallback = if borrowed {
                    Value::build_default_borrowed_expr(default)
                } else {
                    Value::build_default_retained_expr(default)
                };
                quote! {
                    #ident: match #raw {
                        Some(range) => req.path_frame(range)?,
                        None => #fallback,
                    }
                }
            }
            _ if ty.value_optional() => quote! { #ident: #raw },
            _ => {
                let unwrap = Value::build_required_or_default_expr(ty, default, quote!(#raw))?;
                quote! { #ident: #unwrap }
            }
        })
    }
}
