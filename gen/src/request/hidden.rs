use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Result, Type};

use crate::codegen::value::ValueBinding;
use crate::model::{HeaderAttrField, QueryAttrField};
use crate::util::TypeExt;

pub(super) struct Hidden<'a> {
    pub(super) inner_name: &'a Ident,
    pub(super) raw_name: &'a Ident,
    pub(super) headers: &'a [HeaderAttrField],
    pub(super) queries: &'a [QueryAttrField],
    pub(super) ordered_query_state: bool,
}

impl<'a> Hidden<'a> {
    pub(super) fn header_query_exprs(
        headers: &[HeaderAttrField],
        queries: &[QueryAttrField],
        borrowed: bool,
    ) -> Result<Vec<TokenStream>> {
        headers
            .iter()
            .map(|f| {
                ValueBinding::new(&f.ty, f.default.as_ref()).header_query_field(
                    &f.ident,
                    quote!(req.frame_at(range)),
                    "request header/query",
                    borrowed,
                )
            })
            .chain(queries.iter().map(|f| {
                ValueBinding::new(&f.ty, f.default.as_ref()).header_query_field(
                    &f.ident,
                    quote!(req.frame_at(range)),
                    "request header/query",
                    borrowed,
                )
            }))
            .collect()
    }

    pub(super) fn build(self) -> Result<TokenStream> {
        let Self {
            inner_name,
            raw_name,
            headers,
            queries,
            ordered_query_state,
        } = self;
        let all_ident: Vec<&Ident> = headers
            .iter()
            .map(|f| &f.ident)
            .chain(queries.iter().map(|f| &f.ident))
            .collect();
        let all_ty: Vec<&Type> = headers
            .iter()
            .map(|f| &f.ty)
            .chain(queries.iter().map(|f| &f.ty))
            .collect();
        let all_ty_ref: Vec<Type> = all_ty
            .iter()
            .map(|ty| {
                let mut ty = (*ty).clone();
                ty.rewrite_retained_to_borrowed();
                ty
            })
            .collect();
        let raw_ty: Vec<Type> = all_ty
            .iter()
            .map(|ty| ty.raw_field_ty())
            .collect::<Result<_>>()?;
        let query_state = if ordered_query_state {
            quote! { __query: u8, }
        } else {
            quote! {}
        };
        let typed_field_expr = Self::header_query_exprs(headers, queries, true)?;
        Ok(quote! {
            #[allow(non_camel_case_types, dead_code)]
            struct #inner_name<'req> {
                #( #all_ident: #all_ty_ref, )*
                #[doc(hidden)]
                __sark_m: ::core::marker::PhantomData<&'req ()>,
            }

            #[derive(Default)]
            struct #raw_name { #( #all_ident: #raw_ty, )* #query_state }

            impl<'req> #inner_name<'req> {
                #[allow(dead_code, unused_variables)]
                fn from_raw(
                    req: &sark::request::Ref<'req>,
                    headers: #raw_name,
                ) -> sark::error::Result<Self> {
                    let #raw_name { #( #all_ident, )* .. } = headers;
                    Ok(Self {
                        #( #all_ident: #typed_field_expr, )*
                        __sark_m: ::core::marker::PhantomData,
                    })
                }
            }
        })
    }
}
