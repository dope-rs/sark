use std::num::NonZero;

use crate::{runtime, types};

pub(crate) fn expand(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(&input, "Message requires a struct"));
    };
    let mut fields = data.fields.iter();
    let (Some(field), None) = (fields.next(), fields.next()) else {
        return Err(syn::Error::new_spanned(
            &input,
            "Message requires one bytes field",
        ));
    };
    let field_name = field
        .ident
        .as_ref()
        .ok_or_else(|| syn::Error::new_spanned(field, "Message requires named fields"))?;
    let mut tag = None;
    let mut bytes = None;
    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("prost"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("bytes") {
                let value: syn::LitStr = meta.value()?.parse()?;
                if value.value() != "vec" || bytes.replace(()).is_some() {
                    return Err(meta.error("expected bytes = \"vec\" exactly once"));
                }
            } else if meta.path.is_ident("tag") {
                let value: syn::LitStr = meta.value()?.parse()?;
                let value = value
                    .value()
                    .parse::<u32>()
                    .map_err(|error| syn::Error::new_spanned(value, error))?;
                let value = NonZero::new(value)
                    .filter(|value| value.get() < 1 << 29 && !(19000..20000).contains(&value.get()))
                    .ok_or_else(|| meta.error("invalid protobuf field tag"))?;
                if tag.replace(value).is_some() {
                    return Err(meta.error("duplicate protobuf field tag"));
                }
            } else {
                return Err(meta.error("Message requires a bytes field"));
            }
            Ok(())
        })?;
    }
    if bytes.is_none() {
        return Err(syn::Error::new_spanned(
            field,
            "expected #[prost(bytes = \"vec\", tag = \"...\")]",
        ));
    }
    let tag = tag.ok_or_else(|| syn::Error::new_spanned(field, "missing protobuf field tag"))?;
    let root = runtime::require("sark-grpc", "Message")?;
    let name = &input.ident;
    let (generics, args, clause) = input.generics.split_for_impl();
    let mut output = quote::quote! {
        impl #generics #root::encode::Message for #name #args #clause
    };
    let lifetime = types::Generics::new(input.generics).lifetime("a");
    let tag = tag.get();
    output.extend(quote::quote! {
        {
            type Plan<#lifetime> = #root::encode::Field<#lifetime, #tag> where Self: #lifetime;

            fn plan(&self) -> Self::Plan<'_> {
                #root::encode::Field::new(self.#field_name.as_slice())
            }
        }
    });
    Ok(output)
}
