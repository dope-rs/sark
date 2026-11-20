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

    let symbols = types::Generics::new(st.generics.clone());
    let g_sink = symbols.ident("__SINK");
    let [v_response, v_status, v_headers, v_body, v_sink] =
        symbols.locals(["response", "status", "headers", "body", "sink"]);
    let headers = Headers::new(
        paths,
        &name,
        &vis,
        &st.generics,
        &vars,
        &fixed,
        &v_response,
        &symbols,
    );
    st.attrs
        .retain(|attr| !attr.path().is_ident("header") && !attr.path().is_ident("header_static"));
    for field in fields.iter_mut() {
        field.attrs.retain(|attr| !attr.path().is_ident("header"));
    }
    let (_, ty_args, _) = st.generics.split_for_impl();
    let (sh_impl, _, sh_where) = shape.split_for_impl();
    let init = if matches!(mode, Mode::Json) {
        quote::quote!(#ns_json::writing::Body::new(#v_response.#body))
    } else {
        quote::quote!(#v_response.#body)
    };
    let (accept, bound, kind) = if raw && is_fixed {
        (
            quote::format_ident!("statics"),
            quote::quote!(#ns_http::o3::buffer::resident::bound::AtMost<0>),
            quote::quote!(#ns_http::payload::Kind::Static),
        )
    } else {
        (
            if raw {
                quote::format_ident!("fixed")
            } else {
                quote::format_ident!("exact")
            },
            quote::quote!(#ns_http::o3::buffer::resident::bound::Unbounded),
            quote::quote!(#ns_http::payload::Kind::Inline),
        )
    };
    let head = &headers.build;
    let target = quote::quote!(#name #ty_args);
    let impls = quote::quote! {
        impl #sh_impl
            #ns_http::payload::response::dispatch::Complete<'req, 'd> for #target
            #sh_where
        {
            type Bound = #bound;
            const BODY: #ns_http::payload::Kind = #kind;

            fn complete<#g_sink>(self, #v_sink: #g_sink) -> ::core::result::Result<
                #g_sink::Output, #g_sink::Error,
            >
            where
                #g_sink: #ns_http::payload::response::dispatch::Accept<'d>,
            {
                let #v_status;
                let #v_headers;
                let #v_body;
                {
                    let #v_response = self;
                    #v_status = #v_response.#status;
                    #v_headers = #head;
                    #v_body = #init;
                }
                #ns_http::payload::response::dispatch::Accept::#accept(
                    #v_sink, #v_status, #v_headers, #v_body,
                )
            }
        }
    };

    let decl = &headers.decl;
    Ok(quote::quote! {
        #st

        #decl

        #impls
    })
}
