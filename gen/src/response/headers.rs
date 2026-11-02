//! Static and dynamic response headers.

use syn::parse;

use crate::runtime::paths::Paths;

pub(super) struct Headers {
    pub(super) decl: proc_macro2::TokenStream,
    pub(super) ty: proc_macro2::TokenStream,
    pub(super) build: proc_macro2::TokenStream,
}

impl Headers {
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

    pub(super) fn statics(
        attrs: &[syn::Attribute],
    ) -> syn::Result<Vec<(syn::LitStr, syn::LitStr)>> {
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

    pub(super) fn new(
        paths: &Paths,
        name: &syn::Ident,
        vis: &syn::Visibility,
        args: &syn::Generics,
        vars: &[(syn::Ident, syn::LitStr, syn::Type)],
        fixed: &[(syn::LitStr, syn::LitStr)],
    ) -> Self {
        let ns_http = &paths.http;
        let names = crate::types::Generics::new(args.clone());
        let sink = names.ident("sink");
        let source = quote::format_ident!("__{}Fields", name);
        let module = source
            .to_string()
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let module = quote::format_ident!("__sark_response_{module}");
        let mut types = Vec::with_capacity(vars.len());
        let mut values = Vec::with_capacity(vars.len());
        let mut emits = Vec::with_capacity(vars.len());
        for (index, (ident, header, ty)) in vars.iter().enumerate() {
            let name = super::name(paths, header);
            let index = syn::Index::from(index);
            types.push(ty);
            values.push(quote::quote!(self.#ident));
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
            let name =
                syn::LitByteStr::new(name.value().to_ascii_lowercase().as_bytes(), name.span());
            let value = syn::LitByteStr::new(value.value().as_bytes(), value.span());
            let content = quote::quote_spanned! {value.span()=>
                #ns_http::field::literal!(#ns_http::field::Value::try_bytes(#value))
            };
            wire.extend_from_slice(&name.value());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(&value.value());
            wire.extend_from_slice(b"\r\n");
            fields.push(quote::quote! {
                #ns_http::payload::response::headers::Entry::new(
                    #key,
                    #content,
                ).as_field()
            });
        }

        let (_, ty_args, _) = args.split_for_impl();
        let lifetime = names.lifetime("field");
        let marker = quote::quote!(
            ::core::marker::PhantomData<fn() -> *const #name #ty_args>
        );
        let params = (0..vars.len())
            .map(|index| names.ident(&format!("__Header{index}")))
            .collect::<Vec<_>>();
        let marker_param = names.ident("__Response");
        let ty = quote::quote!(#source<#(#types,)* #marker>);
        let writes = if fields.is_empty() {
            quote::quote!()
        } else {
            let wire = syn::LitByteStr::new(&wire, name.span());
            let entries = names.ident("__SARK_FIELDS");
            let group = names.ident("__SARK_GROUP");
            quote::quote! {
                const #entries: &[#ns_http::field::Entry<'static>] = &[ #( #fields, )* ];
                const #group: #ns_http::payload::response::headers::Group<'static> =
                    #ns_http::field::literal!(
                        #ns_http::payload::response::headers::Group::try_new(
                            #ns_http::field::literal!(#ns_http::field::Group::try_new(
                                #entries,
                                #wire,
                            )),
                        )
                    );
                #sink.group(&#group);
            }
        };
        let decl = quote::quote! {
            #[doc(hidden)]
            #vis use #module::#source;

            mod #module {
            #[derive(::core::marker::Copy)]
            pub struct #source<#(#params,)* #marker_param> (
                #( pub(super) #params, )*
                pub(super) #marker_param,
            );

            impl<#(#params: ::core::marker::Copy,)* #marker_param: ::core::marker::Copy>
                ::core::clone::Clone for #source<#(#params,)* #marker_param>
            {
                fn clone(&self) -> Self {
                    *self
                }
            }

            impl<#(#params: #ns_http::field::AsValue,)* #marker_param>
                #ns_http::payload::response::headers::Source
                for #source<#(#params,)* #marker_param>
            {
                #[inline]
                fn emit<#lifetime>(
                    &#lifetime self,
                    #sink: &mut impl #ns_http::payload::response::headers::Sink<#lifetime>,
                ) {
                    #writes
                    #( #emits )*
                }
            }
            }
        };
        let build = quote::quote! {
            #source(
                #( #values, )*
                ::core::marker::PhantomData,
            )
        };
        Self { decl, ty, build }
    }
}
