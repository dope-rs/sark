use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Result;

use crate::codegen::header::BytesMatch;
use crate::codegen::value::{ParsedValue, QueryScan};
use crate::model::QueryAttrField;
use crate::util::TypeExt;

pub(super) struct Query<'a> {
    fields: &'a [QueryAttrField],
}

impl<'a> Query<'a> {
    pub(super) fn new(fields: &'a [QueryAttrField]) -> Self {
        Self { fields }
    }

    pub(super) fn set_name_direct(&self) -> Result<TokenStream> {
        self.states_loop(|_| Ok(quote! { Some(sark::service::FieldValue::parse_value(value)?) }))
    }

    pub(super) fn set_slice_direct(&self) -> Result<TokenStream> {
        self.states_loop(|field| {
            Ok(ParsedValue::new(field.ty.value_kind()?, quote!(range.clone())).emit())
        })
    }

    pub(super) fn parse_direct() -> TokenStream {
        let per_segment = quote! {
            Self::set_query_slice_raw(
                headers,
                name,
                input,
                value_start_abs..value_end_abs,
            )?;
        };
        QueryScan::new(per_segment).emit()
    }

    fn skippable(field: &QueryAttrField) -> bool {
        field.ty.value_optional() || field.default.is_some()
    }

    fn states_loop<F>(&self, assign_for: F) -> Result<TokenStream>
    where
        F: Fn(&QueryAttrField) -> Result<TokenStream>,
    {
        let states: Vec<_> = self
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let ident = &field.ident;
                let name =
                    BytesMatch::Exact.emit(&format_ident!("name"), field.query.value().as_bytes());
                let state = idx as u8;
                let next = state.saturating_add(1);
                let assign = assign_for(field)?;
                let miss = if Self::skippable(field) {
                    quote! {
                        headers.__query = #next;
                        continue;
                    }
                } else {
                    quote! {
                        return Err(sark_core::error::Error::BadRequest(
                            "Invalid query field".into(),
                        ));
                    }
                };
                Ok(quote! {
                    #state => {
                        if #name {
                            headers.#ident = #assign;
                            headers.__query = #next;
                            return Ok(());
                        }
                        #miss
                    }
                })
            })
            .collect::<Result<_>>()?;
        Ok(quote! {
            loop {
                match headers.__query {
                    #( #states, )*
                    _ => {
                        return Err(sark_core::error::Error::BadRequest(
                            "Invalid query field".into(),
                        ));
                    }
                }
            }
        })
    }
}
