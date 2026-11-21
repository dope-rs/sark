use crate::{macros::tokens::local, runtime::paths::Paths};

mod binding;
mod bodies;
mod fields;
mod header;
pub(super) mod mode;
mod params;
mod query;
mod values;

use crate::types;

pub(super) fn build(
    paths: &Paths,
    mode: mode::Mode,
    mut st: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    use quote::{format_ident, quote};
    use syn::{Fields, ext::IdentExt, visit::Visit};

    use self::{
        bodies::{Plan, task},
        header::{prefixes, scan},
        values::FieldPlan,
    };
    let [
        v_auth,
        v_rqst,
        v_parts,
        v_heads,
        v_input,
        v_range,
        v_id,
        v_err,
    ] = [
        "__sark_authority",
        "request",
        "parts",
        "headers",
        "input",
        "range",
        "id",
        "error",
    ]
    .map(local);
    let mode::Mode { ordered, full } = mode;
    let name = st.ident.clone();
    let vis = st.vis.clone();
    let slot = format_ident!("{}HeaderSlot", name);
    let raw_path = format_ident!("{}ParamsRaw", name);
    let headers = format_ident!("{}Headers", name);
    let params = format_ident!("{}Params", name);
    let builder = format_ident!("{}TaskBuilder", name);
    let mut body = Plan::from_attrs(&st.attrs)?;
    let json = body.scope(&name);
    let module = format_ident!(
        "{}",
        crate::handler::names::encoded("__SarkRequest", &name)
            .to_string()
            .to_lowercase()
    );
    let mut imports = vec![name.clone(), headers.clone(), builder.clone()];
    let json_decl = json.as_ref().map(|(name, declaration)| {
        imports.push(name.clone());
        declaration
    });
    let mut symbols = types::Generics::new(st.generics.clone());
    symbols.reserve().visit_item_struct(&st);
    let marker = symbols.ident("__sark_mode");
    let [bytes_param, body_param] = symbols.idents(["__SarkBytes", "__SarkBody"]);
    let v_req = symbols.local("req");
    let v_body = symbols.local("__sark_request_body");
    let mut view_fields = Vec::new();
    let Fields::Named(named) = &mut st.fields else {
        use proc_macro2::Span;

        return Err(syn::Error::new(
            Span::call_site(),
            "#[sark_gen::request] requires a named-field struct",
        ));
    };

    let fields = fields::Fields::collect(&body_param, named, body)?;

    let has_peer = fields.peer.is_some();
    let frame = match has_peer {
        true => quote!(#ns_core::request::frame::proof::Peer),
        false => quote!(#ns_core::request::frame::proof::Maybe),
    };
    let ref_peer = if has_peer {
        Some(quote! {
            use #ns_core::request::frame::raw::Bound;
            let #v_auth = Bound::<#ns_core::request::raw::Parsed<
                #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, Self,
            >>::required(#v_req).get();
        })
    } else {
        None
    };
    let own_peer = if has_peer {
        Some(quote! {
            use #ns_core::request::frame::raw::Bound;
            let #v_auth = Bound::<#ns_core::request::raw::Parsed<
                #ns_core::request::Parts<'d, #frame>, Self,
            >>::required(&#v_rqst).try_get()
                .map_err(#ns_core::route::Input::Retain)?;
        })
    } else {
        None
    };
    let peer_ty = fields.peer.as_ref().map(|(ident, ty)| {
        view_fields.push(quote!(#ident: #v_auth));
        types::retained(ty, paths)
    });
    let (headdecl, headcode, read_headers, kinds, raw_head) = binding::build(
        paths,
        &vis,
        &name,
        &headers,
        &fields,
        ordered,
        &symbols,
        &mut view_fields,
    )?;
    imports.push(raw_head.clone());
    let fields::Fields {
        headers: head,
        queries,
        paths: captures,
        init: mut own_init,
        body,
        ..
    } = fields;

    let (hkinds, qkinds) = kinds.split_at(head.len());
    let headplan = FieldPlan::collect(
        head.iter()
            .zip(hkinds)
            .map(|(field, kind)| (&field.ident, &field.name, *kind)),
    )?;
    let admitted = head.iter().map(|field| {
        let name = syn::LitByteStr::new(field.name.value().as_bytes(), field.name.span());
        quote::quote_spanned! {name.span()=>
            const _: #ns_http::field::Name<&'static [::core::primitive::u8]> =
                #ns_http::field::literal!(#ns_http::field::Name::try_bytes(#name));
        }
    });
    let qplan = FieldPlan::collect(
        queries
            .iter()
            .zip(qkinds)
            .map(|(field, kind)| (&field.ident, &field.name, *kind)),
    )?;
    let captures = captures
        .iter()
        .map(|field| Ok((field, types::View::new(&field.ty).kind()?)))
        .collect::<syn::Result<Vec<_>>>()?;
    let identity = format_ident!("{}Identity", name);
    let mut ids: Vec<proc_macro2::Ident> = (0..head.len())
        .map(|idx| format_ident!("Header{idx}"))
        .chain((0..queries.len()).map(|idx| format_ident!("Query{idx}")))
        .collect();
    for (idx, (_, kind)) in captures.iter().enumerate() {
        match kind {
            types::ValueKind::Bytes => {}
            _ => ids.push(format_ident!("Path{idx}")),
        }
    }
    let (id_def, proof, error) = if ids.is_empty() {
        (
            proc_macro2::TokenStream::new(),
            proc_macro2::TokenStream::new(),
            quote!(::core::convert::Infallible),
        )
    } else {
        imports.push(identity.clone());
        (
            quote! {
                #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::fmt::Debug, ::core::cmp::PartialEq, ::core::cmp::Eq)]
                #vis enum #identity { #( #ids, )* }
            },
            quote! {
                impl #ns_core::route::Fields for #name<'static> {
                    type Identity = #identity;
                }
            },
            quote!(#ns_core::route::Error<#identity>),
        )
    };

    let (hkind, slot_decl, slot_def, himpl) = if head.is_empty() {
        (
            quote!(#ns_core::route::field::header::Discard),
            proc_macro2::TokenStream::new(),
            proc_macro2::TokenStream::new(),
            proc_macro2::TokenStream::new(),
        )
    } else {
        imports.push(slot.clone());
        let (decl, def, get, spans, set, encoding) =
            headplan.slots(paths, &vis, &slot, &identity, &raw_head);
        let policy = prefixes::build(paths, &headplan, &name, &slot)?;
        let methods = scan::build(paths, &raw_head, &error, &get, &spans, &set);
        (
            quote!(#slot),
            decl,
            def,
            quote! {
                #policy
                impl #ns_core::route::field::header::Fields<#name<'static>> for #slot {
                    #methods
                    #encoding
                }
            },
        )
    };
    let (qkind, qimpl) = if queries.is_empty() {
        (
            quote!(#ns_core::route::field::query::Discard),
            proc_macro2::TokenStream::new(),
        )
    } else {
        let parse = if ordered {
            use query::try_ordered;
            try_ordered(paths, &qplan, &identity, &symbols)?
        } else {
            qplan.parse_query(paths, &identity)
        };
        (
            quote!(#raw_head),
            quote! {
                impl #ns_core::route::field::query::Fields<#name<'static>> for #raw_head {
                    fn parse(
                        #v_parts: #ns_core::route::field::query::Input<'_, #name<'static>>,
                    ) -> ::core::result::Result<(), #error> {
                        let (#v_heads, #v_input, #v_range) = #v_parts.into_parts();
                        #parse
                    }
                }
            },
        )
    };
    let expand = params::Expansion::try_new(
        paths,
        &captures,
        &vis,
        &name,
        &params,
        &raw_path,
        &symbols,
        &mut view_fields,
    )?;
    if !captures.is_empty() {
        imports.extend([params.clone(), raw_path.clone()]);
    }
    let paramdecl = &expand.declaration;
    let paramsrc = &expand.code;
    let path_err = &expand.error;
    let p_reject = &expand.reject;
    let ref_p = &expand.view;
    fields::Fields::commit(&mut st.attrs, named);
    let storage = body.storage(paths, named, &vis, &body_param);
    let empty = named.named.is_empty().then(|| {
        quote! {
            fn empty_task<'d>(
                #v_id: #ns_http::request::Id<'d>,
            ) -> ::core::option::Option<#ns_core::route::Built<'d, Self::Task<'d>>> {
                if ::core::mem::needs_drop::<Self::Task<'d>>()
                    || ::core::mem::needs_drop::<Self::TaskBuilder<'d>>()
                {
                    return ::core::option::Option::None;
                }
                ::core::option::Option::Some(#ns_core::route::Built::new(
                    #v_id,
                    #name::<'d> {
                        #marker: ::core::marker::PhantomData,
                    },
                ))
            }
        }
    });
    let body_len = local("__sark_body_len");
    own_init.extend(body.fields(paths, local("__sark_task_body"), quote!(#body_len)));
    if !st
        .generics
        .lifetimes()
        .any(|lifetime| lifetime.lifetime.ident.unraw() == "d")
    {
        st.generics.params.insert(0, syn::parse_quote!('d));
    }
    st.generics.params.push(syn::parse_quote!(
        #bytes_param = #ns_http::request::Bytes<'d>
    ));
    let bytes_ref = quote!(&'req [::core::primitive::u8]);
    let (view_args, storage_args) = match storage {
        Some((owned, borrowed)) => {
            st.generics
                .params
                .push(syn::parse_quote!(#body_param = #owned));
            (
                quote!(#bytes_ref, #borrowed),
                quote!(#bytes_param, #body_param),
            )
        }
        None => (bytes_ref, quote!(#bytes_param)),
    };
    for field in &mut named.named {
        if let Some(bytes) = types::shared_bytes(&mut field.ty) {
            *bytes = syn::parse_quote!(#bytes_param);
        }
    }
    named.named.push(syn::parse_quote! {
        #[doc(hidden)]
        pub #marker: ::core::marker::PhantomData<
            fn(&'d (), #storage_args) -> (&'d (), #storage_args)
        >
    });
    own_init.push(quote!(#marker: ::core::marker::PhantomData));
    let (in_err, read_body) = body.project(paths, path_err, &v_body, &mut view_fields);
    let (body_ty, source) = body.task(paths);

    let (raw_ty, own_path) = if captures.is_empty() {
        (quote!(()), quote!(()))
    } else {
        (quote!(#raw_path), quote!(#params<'d>))
    };
    let own_head = quote!(#headers<'d>);
    let pstart = &expand.start;
    let owned = quote! {
        #pstart
        let #v_heads = #headers::<'d>
            ::try_from_task(&#v_rqst).map_err(#ns_core::route::Input::Retain)?;
        #own_peer
    };
    let task = task::Task {
        names: task::Names {
            request: &name,
            builder: &builder,
            params: &own_path,
            headers: &own_head,
        },
        authority: peer_ty.as_ref(),
        params: &expand,
        head: &owned,
        fields: &own_init,
        vis: &vis,
    };
    let task::Output {
        declaration: task_decl,
        definition: task_def,
        reject,
        ty: task_ty,
        start,
    } = task.build(paths, source);
    Ok(quote! {
        #( #admitted )*
        #json_decl
        #st
        #headdecl
        #paramdecl
        #task_decl
        #slot_decl
        #id_def

        mod #module {
        use super::{ #( #imports, )* };

        #headcode
        #paramsrc
        #task_def
        #slot_def
        #himpl
        #qimpl

        impl #ns_core::route::Schema for #name<'static> {
            type Header = #hkind;
            type Query = #qkind;
            type RawHeaders = #raw_head;
            type RawParams = #raw_ty;
            type Body = #body_ty;
            type Error = #error;
            type Frame = #frame;

            const FULL: ::core::primitive::bool = #full;
            const AUTHORITY: ::core::primitive::bool = #has_peer;
        }
        #proof
        impl #ns_core::route::Project for #name<'static> {
            type Error<'req> = #in_err;
            fn reject<'req>(
                #v_err: <Self as #ns_core::route::Project>::Error<'req>,
            ) -> #ns_core::route::Reject {
                match #v_err {
                    #ns_core::route::Input::Params(#v_err) => #p_reject,
                    #ns_core::route::Input::Body(#v_err) => #reject,
                    #ns_core::route::Input::Retain(#v_err) => match #v_err {},
                }
            }
            type View<'req, 'd> = #name<'d, #view_args> where 'd: 'req;

            fn try_view<'req, 'body: 'req, 'd: 'body>(
                #v_req: &#ns_core::request::raw::Parsed<
                    #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, Self,
                >,
                #v_body: #ns_core::body::Scoped<'body, 'd, Self::Body>,
            ) -> ::core::result::Result<Self::View<'req, 'd>, <Self as #ns_core::route::Project>::Error<'body>> {
                #ref_p
                #read_headers
                #ref_peer
                #read_body
                ::core::result::Result::Ok(#name {
                    #(#view_fields,)*
                    #marker: ::core::marker::PhantomData,
                })
            }
        }
        impl #ns_core::route::Own for #name<'static> {
            type Task<'d> = #name<'d>;
            type TaskBuilder<'d> = #task_ty;
            type Mode = #ns_core::route::Streaming;
            #empty
            #start
        }
        }
    })
}
