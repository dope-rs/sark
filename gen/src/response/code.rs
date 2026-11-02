use crate::{
    response::{headers::Headers, mode::Mode},
    runtime::paths::Paths,
    types,
};

pub(crate) fn build(
    paths: &Paths,
    mode: Mode,
    mut st: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_http = &paths.http;
    let ns_json = &paths.json;
    use syn::{Fields, Type, ext::IdentExt};
    let name = st.ident.clone();
    let vis = st.vis.clone();
    let fixed = Headers::statics(&st.attrs)?;
    let has_req = st
        .generics
        .lifetimes()
        .any(|lifetime| lifetime.lifetime.ident.unraw() == "req");
    let has_d = st
        .generics
        .lifetimes()
        .any(|lifetime| lifetime.lifetime.ident.unraw() == "d");
    let mut shape = st.generics.clone();
    if !has_d {
        shape.params.insert(0, syn::parse_quote!('d));
    }
    if !has_req {
        shape.params.insert(0, syn::parse_quote!('req));
    }
    shape
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!('d: 'req));
    let fields = match &mut st.fields {
        Fields::Named(fields) => &mut fields.named,
        Fields::Unnamed(fields) => {
            return Err(syn::Error::new_spanned(
                fields,
                "#[sark_gen::response] requires a struct with named fields",
            ));
        }
        Fields::Unit => {
            return Err(syn::Error::new_spanned(
                st.struct_token,
                "#[sark_gen::response] requires a struct with named fields",
            ));
        }
    };

    let mut status = None::<syn::Ident>;
    let mut body = None::<(syn::Ident, Type, bool)>;
    let mut vars = Vec::new();
    for field in fields.iter() {
        let ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "named field required")
        })?;
        if ident.unraw() == "status" {
            status = Some(ident.clone());
        }
        if ident.unraw() == "body" {
            body = Some((
                ident.clone(),
                field.ty.clone(),
                types::View::new(&field.ty).is_fixed(),
            ));
        }
        if let Some(header) = Headers::name(&field.attrs)? {
            let ty = field.ty.clone();
            shape
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(#ty: #ns_http::field::AsValue));
            vars.push((ident.clone(), header, ty));
        }
    }
    let status = status.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[sark_gen::response] requires `status` field",
        )
    })?;
    let (body, body_ty, is_fixed) = body.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[sark_gen::response] requires `body` field",
        )
    })?;
    let raw = matches!(mode, Mode::Raw);
    if raw && is_fixed {
        shape
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#body_ty: 'd));
    }

    let headers = Headers::new(paths, &name, &vis, &st.generics, &vars, &fixed);
    st.attrs
        .retain(|attr| !attr.path().is_ident("header") && !attr.path().is_ident("header_static"));
    for field in fields.iter_mut() {
        field.attrs.retain(|attr| !attr.path().is_ident("header"));
    }
    let source = &headers.ty;
    let (ty_impl, ty_args, ty_where) = st.generics.split_for_impl();
    let (sh_impl, _, sh_where) = shape.split_for_impl();
    let mut fn_args = syn::Generics::default();
    if !has_req {
        fn_args.params.push(syn::parse_quote!('req));
    }
    if !has_d {
        fn_args.params.push(syn::parse_quote!('d));
    }
    let fn_where = if raw && is_fixed {
        quote::quote!(where 'd: 'req, #body_ty: 'd)
    } else {
        quote::quote!(where 'd: 'req)
    };
    let init = if matches!(mode, Mode::Json) {
        quote::quote!(#ns_json::writing::Body::new(self.#body))
    } else {
        quote::quote!(self.#body)
    };
    let encoded = if matches!(mode, Mode::Json) {
        quote::quote!(#ns_json::writing::Body<#body_ty>)
    } else {
        quote::quote!(#body_ty)
    };
    let (output, ctor) = if raw {
        (
            quote::quote!(#ns_http::payload::response::fixed::Direct<#source, #body_ty>),
            quote::quote!(#ns_http::payload::response::fixed::Direct::new),
        )
    } else {
        (
            quote::quote!(#ns_http::payload::response::exact::Direct<#source, #encoded>),
            quote::quote!(#ns_http::payload::response::exact::Direct::new),
        )
    };
    let head = &headers.build;
    let convert = quote::quote! {
        #ctor(
            self.#status,
            #head,
            #init,
        )
    };
    let api = if raw && is_fixed {
        quote::quote!()
    } else {
        quote::quote! {
            impl #ty_impl #name #ty_args #ty_where {
                #vis fn into_fixed #fn_args (self) -> #output
                #fn_where
                {
                    #convert
                }
            }
        }
    };
    let slice = if raw && is_fixed {
        quote::quote! {
            impl #ty_impl #name #ty_args #ty_where {
                #vis fn into_static_response #fn_args (
                    self,
                ) -> #ns_http::payload::response::statics::Direct<'d, #source>
                #fn_where
                {
                    #ns_http::payload::response::statics::Direct::new(
                        self.#status,
                        #head,
                        self.#body,
                    )
                }
            }

        }
    } else {
        quote::quote!()
    };
    let target = quote::quote!(#name #ty_args);
    let (native, lower) = if raw && is_fixed {
        (
            quote::quote!(
                #ns_http::payload::response::statics::Direct<'d, #source>
            ),
            quote::quote!(self.into_static_response()),
        )
    } else {
        (output.clone(), quote::quote!(self.into_fixed()))
    };
    let impls = quote::quote! {
        impl #sh_impl
            #ns_http::payload::response::dispatch::IntoOutput<'req, 'd> for #target
            #sh_where
        {
            type Output = #native;

            fn into_output(self) -> Self::Output {
                #lower
            }
        }
    };

    let decl = &headers.decl;
    Ok(quote::quote! {
        #st

        #decl

        #api
        #slice

        #impls
    })
}
