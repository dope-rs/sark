use crate::{macros::tokens, runtime::paths::Paths};

mod field;
pub(super) mod mode;
mod plan;
mod scalar;
mod segment;
mod sources;

use plan::{Scope, encoding};
use segment::task;

pub(super) fn build(
    paths: &Paths,
    mode: mode::Mode,
    mut st: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let ns_http = &paths.http;
    let [v_raw, v_idx, v_depth] = ["__raw", "__idx", "__depth"].map(tokens::local);
    let [v_bytes, v_decode, v_done, v_error] =
        ["__bytes", "__decoder", "__decoded", "__error"].map(tokens::local);
    use std::collections::BTreeMap;

    use proc_macro2::TokenStream;
    use syn::{Fields, ext::IdentExt, visit::Visit};
    if matches!(mode.purpose, mode::Purpose::Decode) && !st.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &st.generics,
            "generic JSON declarations require `encode` or `try_encode`",
        ));
    }
    let mut symbols = crate::types::Generics::new(st.generics.clone());
    symbols.reserve().visit_item_struct(&st);
    let marker = symbols.ident("__sark_json_marker");
    let name = st.ident.clone();
    let vis = st.vis.clone();
    let fields = match &mut st.fields {
        Fields::Named(named) => {
            use crate::json::field::FieldMode;
            let parsed = named
                .named
                .iter()
                .map(|field| FieldMode::from_field(field, mode.plain))
                .collect::<syn::Result<Vec<_>>>()?;
            for field in &mut named.named {
                field.attrs.retain(|attr| {
                    !attr.path().is_ident("raw")
                        && !attr.path().is_ident("plain")
                        && !attr.path().is_ident("field")
                });
            }

            named
                .named
                .iter()
                .zip(parsed)
                .enumerate()
                .map(|(idx, (field, (kind, rename)))| {
                    let ident = field.ident.clone().ok_or_else(|| {
                        syn::Error::new(proc_macro2::Span::call_site(), "named field required")
                    })?;
                    let label = match rename {
                        Some(name) => name,
                        None => ident.unraw().to_string(),
                    };
                    Ok(plan::JsonField {
                        bind: tokens::local(&format!("__f{idx}")),
                        ident,
                        ty: field.ty.clone(),
                        vis: field.vis.clone(),
                        name: label,
                        mode: kind,
                    })
                })
                .collect::<syn::Result<Vec<_>>>()?
        }
        Fields::Unnamed(unnamed) => {
            return Err(syn::Error::new_spanned(
                unnamed,
                "#[sark_gen::json] requires a named-field struct",
            ));
        }
        Fields::Unit => {
            return Err(syn::Error::new_spanned(
                &name,
                "#[sark_gen::json] requires a named-field struct",
            ));
        }
    };

    let mut names = BTreeMap::new();
    for field in fields.iter().filter(|field| !field.mode.flatten) {
        if let Some(first) = names.insert(field.name.as_str(), &field.ident) {
            let mut error = syn::Error::new_spanned(&field.ident, "duplicate JSON wire field name");
            error.combine(syn::Error::new_spanned(
                first,
                "first field with this wire name",
            ));
            return Err(error);
        }
    }

    let plan = plan::Plan {
        mode: &mode,
        fields,
    };

    if !matches!(mode.purpose, mode::Purpose::Encode)
        && let Some(field) = plan.fields.iter().find(|field| field.mode.flatten)
    {
        return Err(syn::Error::new_spanned(
            &field.ident,
            "`flatten` requires encoding-only `#[json(encode)]`",
        ));
    }

    if mode.exact
        && plan
            .fields
            .iter()
            .any(|field| field.mode.nested || field.mode.seq)
    {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[field(nested)] and #[field(seq)] are not supported with `exact`",
        ));
    }

    let names = encoding::Names::new(&st.generics);
    let plan::Emission { lengths, writes } = plan.encoding(paths, Scope::Object, &names)?;
    let (_, enc_ty, _) = st.generics.split_for_impl();
    let encoder = encoding::emit(
        paths,
        &st.generics,
        quote::quote!(#name #enc_ty),
        &names,
        &lengths,
        &writes,
    );
    if let syn::Fields::Named(named) = &mut st.fields {
        for (decl, field) in named.named.iter_mut().zip(&plan.fields) {
            decl.ty = field::proof_type(paths, &field.ty, field.mode)?;
        }
    }

    let members = if matches!(mode.purpose, mode::Purpose::TryEncode) {
        TokenStream::new()
    } else {
        plan.members(paths, &st)?
    };
    if !matches!(mode.purpose, mode::Purpose::Decode) {
        st.attrs.retain(|attr| !attr.path().is_ident("json"));
        return scope(
            &st,
            quote::quote! {
                #st

                #encoder
                #members
            },
        );
    }
    let scoped = task::expand(
        paths,
        &name,
        &vis,
        &st.generics,
        &plan.fields,
        &mode,
        &marker,
    )?;

    let borrow = plan.borrowed(paths);

    let stream = {
        let order = match mode.kind {
            mode::JsonKind::Ordered => quote::quote!(#ns_json::segment::Order::Ordered),
            mode::JsonKind::Unordered => quote::quote!(#ns_json::segment::Order::Unordered),
        };
        let schema = plan
            .fields
            .iter()
            .map(|field| {
                let name =
                    syn::LitByteStr::new(field.name.as_bytes(), proc_macro2::Span::call_site());
                let view = if field.mode.nested || field.mode.seq {
                    quote::quote!(#ns_json::segment::Projection::Segments)
                } else {
                    let class = scalar::Classified::of(&field.ty)?;
                    match class.scalar {
                        scalar::Scalar::Shared if !field.mode.raw && !field.mode.plain => {
                            quote::quote!(#ns_json::segment::Projection::Frame)
                        }
                        scalar::Scalar::Shared | scalar::Scalar::Token => {
                            quote::quote!(#ns_json::segment::Projection::Bytes)
                        }
                        _ => {
                            quote::quote!(#ns_json::segment::Projection::Scalar)
                        }
                    }
                };
                Ok(quote::quote!(#ns_json::segment::Field::new(
                    #name,
                    #view,
                )))
            })
            .collect::<syn::Result<Vec<_>>>()?;
        segment::decode(paths, &name, &vis, &plan.fields, order, schema, &marker)?
    };

    let decode = quote::quote! {
        let mut #v_decode = #ns_json::segment::Decoder::<Self>::one(#v_bytes)?;
        loop {
            match #v_decode.poll(::core::num::NonZero::<::core::primitive::usize>::MAX) {
                #ns_json::segment::Progress::Ready(#v_done) => return ::core::result::Result::Ok(#v_done),
                #ns_json::segment::Progress::Failed(#v_error) => return ::core::result::Result::Err(#v_error),
                #ns_json::segment::Progress::NeedInput => {
                    return ::core::result::Result::Err(#ns_json::Error::Invalid.into());
                }
                #ns_json::segment::Progress::Pending => {}
            }
        }
    };

    let request = {
        let view = quote::format_ident!("{}JsonView", name);
        let items = plan
            .fields
            .iter()
            .map(|field| {
                Ok(plan::JsonField {
                    ident: field.ident.clone(),
                    bind: field.bind.clone(),
                    ty: field::request_type(paths, &field.ty, field.mode)?,
                    vis: field.vis.clone(),
                    name: field.name.clone(),
                    mode: field.mode,
                })
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let shape = plan::Plan {
            mode: &mode,
            fields: items,
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
        let locals = shape.locals(paths)?;
        let finals = shape.finals(paths);
        let body = shape.decode_body(paths)?;
        let generics = syn::parse_quote!(<'req>);
        let names = encoding::Names::new(&generics);
        let plan::Emission { lengths, writes } = shape.encoding(paths, Scope::Object, &names)?;
        let encode = encoding::emit(
            paths,
            &generics,
            quote::quote!(#view<'req>),
            &names,
            &lengths,
            &writes,
        );
        quote::quote! {
            #vis struct #view<'req> {
                #( #decls, )*
                #[doc(hidden)]
                #marker: ::core::marker::PhantomData<&'req ()>,
            }

        impl #ns_json::parsing::Request for #name {
                type View<'req> = #view<'req>;

                fn prefix<'req>(
                    #v_raw: &'req [::core::primitive::u8],
                    mut #v_idx: ::core::primitive::usize,
                    #v_depth: #ns_json::parsing::Depth,
                ) -> ::core::result::Result<
                    (Self::View<'req>, ::core::primitive::usize),
                    #ns_json::parsing::Error<'req>,
                > {
                    #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                    #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b'{')?;
                    #(#locals)*
                    #body
                    ::core::result::Result::Ok((#view {
                        #(#finals,)*
                        #marker: ::core::marker::PhantomData,
                    }, #v_idx))
                }
            }

            impl<'view> #ns_json::parsing::Request for #view<'view> {
                type View<'req> = #view<'req>;

                fn prefix<'req>(
                    #v_raw: &'req [::core::primitive::u8],
                    #v_idx: ::core::primitive::usize,
                    #v_depth: #ns_json::parsing::Depth,
                ) -> ::core::result::Result<
                    (Self::View<'req>, ::core::primitive::usize),
                    #ns_json::parsing::Error<'req>,
                > {
                    <#name as #ns_json::parsing::Request>::prefix(#v_raw, #v_idx, #v_depth)
                }
            }

            #encode
        }
    };

    st.attrs.retain(|attr| !attr.path().is_ident("json"));
    scope(
        &st,
        quote::quote! {
            #st
            #request
            #scoped

            impl #ns_json::parsing::Decode for #name {
                fn decode<'__e>(#v_bytes: #ns_http::o3::buffer::storage::Shared) ->
                    ::core::result::Result<Self, #ns_json::parsing::Error<'__e>> {
                    #decode
                }
                #borrow
            }

            #stream

            #encoder
            #members
        },
    )
}

fn scope(
    source: &syn::ItemStruct,
    output: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    use quote::ToTokens;
    use syn::{visit::Visit, visit_mut::VisitMut};

    let mut names = crate::types::Generics::new(source.generics.clone());
    names.reserve().visit_item_struct(source);
    let prefix = crate::handler::names::encoded("__SarkJson", &source.ident).to_string();
    let module = quote::format_ident!("{}", prefix.to_lowercase());
    let mut imports = crate::types::Imports::new(names, prefix);
    let mut declarations = proc_macro2::TokenStream::new();
    let mut implementations = proc_macro2::TokenStream::new();
    for item in syn::parse2::<syn::File>(output)?.items {
        if let syn::Item::Impl(mut item) = item {
            imports.bind(&item.generics);
            imports.visit_item_impl_mut(&mut item);
            item.to_tokens(&mut implementations);
        } else {
            item.to_tokens(&mut declarations);
        }
    }
    let (outer, inner) = imports.finish()?;
    Ok(quote::quote! {
        #declarations
        #outer
        mod #module {
            #inner
            #implementations
        }
    })
}
