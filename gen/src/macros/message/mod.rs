use crate::{runtime, types};

mod field;

pub(crate) struct Message {
    input: syn::DeriveInput,
    field: field::Field,
}

impl Message {
    pub(crate) fn new(input: syn::DeriveInput) -> syn::Result<Self> {
        let syn::Data::Struct(data) = &input.data else {
            return Err(syn::Error::new_spanned(&input, "Message requires a struct"));
        };
        let mut fields = data.fields.iter();
        let field = fields
            .next()
            .ok_or_else(|| syn::Error::new_spanned(&input, "Message requires one bytes field"))?;
        if fields.next().is_some() {
            return Err(syn::Error::new_spanned(
                &input,
                "Message requires one bytes field",
            ));
        }
        let field = field::Field::new(field)?;
        Ok(Self { input, field })
    }

    pub(crate) fn expand(self) -> syn::Result<proc_macro2::TokenStream> {
        let root = runtime::require("sark-grpc", "Message")?;
        let name = &self.input.ident;
        let lifetime = types::Generics::new(self.input.generics.clone()).lifetime("a");
        let (generics, args, clause) = self.input.generics.split_for_impl();
        let field = self.field.name();
        let tag = self.field.tag().get();
        Ok(quote::quote! {
            impl #generics #root::encode::Message for #name #args #clause {
                type Plan<#lifetime> = #root::encode::Field<#lifetime, #tag> where Self: #lifetime;

                fn plan(&self) -> Self::Plan<'_> {
                    #root::encode::Field::new(self.#field.as_slice())
                }
            }
        })
    }
}
