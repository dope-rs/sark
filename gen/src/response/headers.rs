//! Static and dynamic response headers.

use syn::parse;

use crate::runtime::paths::Paths;

pub(super) fn name(attrs: &[syn::Attribute]) -> syn::Result<Option<syn::LitStr>> {
    let mut found = None;
    for attr in attrs {
        if !attr.path().is_ident("header") {
            continue;
        }
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate #[header(...)] attribute",
            ));
        }
        found = Some(attr.parse_args::<syn::LitStr>()?);
    }
    Ok(found)
}

pub(super) fn statics(attrs: &[syn::Attribute]) -> syn::Result<Vec<(syn::LitStr, syn::LitStr)>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("header_static") && !attr.path().is_ident("header") {
            continue;
        }
        let (name, value) = attr.parse_args_with(|input: parse::ParseStream<'_>| {
            let name = input.parse::<syn::LitStr>()?;
            input.parse::<syn::Token![,]>()?;
            let value = input.parse()?;
            Ok((name, value))
        })?;
        out.push((name, value));
    }
    Ok(out)
}

pub(super) fn build(
    paths: &Paths,
    name: &syn::Ident,
    vars: &[(syn::Ident, syn::LitStr)],
    fixed: &[(syn::LitStr, syn::LitStr)],
    value: &syn::Ident,
    names: &crate::types::Generics,
) -> proc_macro2::TokenStream {
    if vars.is_empty() && fixed.is_empty() {
        return quote::quote!(());
    }
    let ns_http = &paths.http;
    let sink = names.ident("sink");
    let source = names.ident("__SarkFields");
    let mut values = Vec::with_capacity(vars.len());
    let mut emits = Vec::with_capacity(vars.len());
    for (index, (ident, header)) in vars.iter().enumerate() {
        let name = super::name(paths, header);
        let index = syn::Index::from(index);
        values.push(quote::quote!(#value.#ident));
        emits.push(quote::quote! {
            #sink.field(
                #name,
                #ns_http::field::AsValue::as_value(&self.#index),
            );
        });
    }

    let mut fields = Vec::with_capacity(fixed.len());
    let mut wire = Vec::new();
    for (name, value) in fixed {
        let key = super::name(paths, name);
        let bytes = name.value().to_ascii_lowercase();
        let value = syn::LitByteStr::new(value.value().as_bytes(), value.span());
        let content = quote::quote_spanned! {value.span()=>
            #ns_http::field::literal!(#ns_http::field::Value::try_bytes(#value))
        };
        wire.extend_from_slice(bytes.as_bytes());
        wire.extend_from_slice(b": ");
        wire.extend_from_slice(&value.value());
        wire.extend_from_slice(b"\r\n");
        fields.push(quote::quote! {
            #ns_http::payload::response::headers::Entry::new(#key, #content)
        });
    }
    let writes = if fields.is_empty() {
        quote::quote!()
    } else {
        let wire = syn::LitByteStr::new(&wire, name.span());
        let entries = names.ident("__SARK_FIELDS");
        let group = names.ident("__SARK_GROUP");
        quote::quote! {
            const #entries: &[#ns_http::payload::response::headers::Entry<'static>] = &[#(#fields),*];
            const #group: #ns_http::payload::response::headers::Group<'static> =
                #ns_http::field::literal!(
                    #ns_http::payload::response::headers::Group::try_new(#entries, #wire)
                );
            #sink.group(&#group);
        }
    };

    let lifetime = names.lifetime("field");
    let params = (0..vars.len())
        .map(|index| names.ident(&format!("__Header{index}")))
        .collect::<Vec<_>>();
    let args = (!params.is_empty()).then(|| quote::quote!(<#(#params),*>));
    let bounds =
        (!params.is_empty()).then(|| quote::quote!(<#(#params: #ns_http::field::AsValue),*>));
    quote::quote! {{
        struct #source #args (#(#params,)*);

        impl #bounds #ns_http::field::Source<#ns_http::payload::response::headers::Application>
            for #source #args
        {
            #[inline]
            fn emit<#lifetime>(
                &#lifetime self,
                #sink: &mut impl #ns_http::field::Sink<#lifetime, #ns_http::payload::response::headers::Application>,
            ) {
                #writes
                #(#emits)*
            }
        }
        #source(#(#values,)*)
    }}
}
