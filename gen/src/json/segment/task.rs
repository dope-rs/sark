use plan::encoding;

use crate::{
    json::{field, mode, plan, plan::Scope},
    runtime::paths::Paths,
};

pub(in crate::json) fn expand(
    paths: &Paths,
    name: &syn::Ident,
    vis: &syn::Visibility,
    generics: &syn::Generics,
    fields: &[plan::JsonField],
    mode: &mode::Mode,
    marker: &syn::Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let scoped = quote::format_ident!("{}Scoped", name);
    let mapped = fields
        .iter()
        .map(|source| {
            Ok(plan::JsonField {
                ident: source.ident.clone(),
                bind: source.bind.clone(),
                ty: source
                    .mode
                    .task_type(paths, &source.ty, &syn::parse_quote!('d))?,
                vis: source.vis.clone(),
                name: source.name.clone(),
                mode: source.mode,
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let shape = plan::Plan {
        mode,
        fields: mapped,
    };
    let decls = shape
        .fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let ty = field::proof_type(paths, &field.ty, field.mode)?;
            let vis = &field.vis;
            Ok(quote::quote!(#vis #ident: #ty))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let mut req_gens = generics.clone();
    req_gens.params.insert(0, syn::parse_quote!('d));
    let (req_impl, req_ty, req_pred) = req_gens.split_for_impl();
    let (own_impl, owned_ty, own_pred) = generics.split_for_impl();
    let names = encoding::Names::new(&req_gens);
    let plan::Emission { lengths, writes } = shape.encoding(paths, Scope::Object, &names)?;
    let encode = encoding::emit(
        paths,
        &req_gens,
        quote::quote!(#scoped #req_ty),
        &names,
        &lengths,
        &writes,
    );

    Ok(quote::quote! {
        #[doc(hidden)]
        #vis struct #scoped #req_impl #req_pred {
            #(#decls,)*
            #[doc(hidden)]
            pub #marker: ::core::marker::PhantomData<&'d ()>,
        }

        impl #own_impl #ns_json::segment::IntoRequest for #name #owned_ty #own_pred {
            type Request<'d> = #scoped #req_ty;
        }

        #encode
    })
}
