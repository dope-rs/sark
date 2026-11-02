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
        v_req,
        v_rqst,
        v_parts,
        v_heads,
        v_input,
        v_range,
        v_id,
        v_params,
        v_err,
    ] = [
        "__sark_authority",
        "req",
        "request",
        "parts",
        "headers",
        "input",
        "range",
        "id",
        "params",
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
    let storage = format_ident!("__{}Mode", name);
    let mode_ty = format_ident!("__{}Storage", name);
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
    let Fields::Named(named) = &mut st.fields else {
        use proc_macro2::Span;

        return Err(syn::Error::new(
            Span::call_site(),
            "#[sark_gen::request] requires a named-field struct",
        ));
    };

    let fields = fields::Fields::collect(paths, named, body)?;

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
    let peer_ty = fields.peer.as_ref().map(|ty| {
        let mut ty = ty.clone();
        types::Edit::new(&mut ty).retained(paths, types::RetainedAs::Request);
        ty
    });
    let (headdecl, headcode, kinds, raw_head) =
        binding::build(paths, &vis, &name, &headers, &fields, ordered, &symbols)?;
    imports.push(raw_head.clone());
    let fields::Fields {
        headers: head,
        queries,
        paths: captures,
        init: mut ref_init,
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
            types::ValueKind::Usize | types::ValueKind::U64 | types::ValueKind::Bool => {
                ids.push(format_ident!("Path{idx}"))
            }
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
        let (decl, def, get, spans, set) = headplan.slots(paths, &vis, &slot, &identity);
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
    let build = quote! {
        #headers::<
            'req,
            #ns_core::request::mode::Ref<'req>,
        >::from_head(#v_req)
    };
    let expand =
        params::Expansion::try_new(paths, &captures, &vis, &name, &params, &raw_path, &symbols)?;
    if !captures.is_empty() {
        imports.extend([params.clone(), raw_path.clone()]);
    }
    let paramdecl = &expand.declaration;
    let paramsrc = &expand.code;
    let path_err = &expand.error;
    let p_reject = &expand.reject;
    let ref_p = &expand.view;
    fields::Fields::commit(&mut st.attrs, named);
    body.append_json(named, &vis);
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
    let mut own_init = ref_init.clone();
    let body_len = local("__sark_body_len");
    ref_init.extend(body.fields(paths, local("parsed_body"), quote!(#v_req.body_len())));
    own_init.extend(body.fields(paths, local("__sark_task_body"), quote!(#body_len)));
    if !st
        .generics
        .lifetimes()
        .any(|lifetime| lifetime.lifetime.ident.unraw() == "d")
    {
        st.generics.params.insert(0, syn::parse_quote!('d));
    }
    st.generics.params.push(syn::parse_quote!(
        #mode_ty: #storage<'d> = #ns_core::request::mode::Own
    ));
    for field in &mut named.named {
        body.rewrite(field, &storage, &mode_ty);
        types::Edit::new(&mut field.ty).retained(paths, types::RetainedAs::Mode(&mode_ty));
    }
    named.named.push(syn::parse_quote! {
        #[doc(hidden)]
        pub #marker: ::core::marker::PhantomData<
            fn(&'d (), #mode_ty) -> (&'d (), #mode_ty)
        >
    });
    ref_init.push(quote!(#marker: ::core::marker::PhantomData));
    own_init.push(quote!(#marker: ::core::marker::PhantomData));
    let modes = body.mode_impls(paths, &vis, &storage);
    let (in_err, parsed, body_arg) = body.parsing(paths, path_err);
    let (body_ty, plan) = body.task(paths);

    let (raw_ty, path_ty, own_path, path_api) = if captures.is_empty() {
        (
            quote!(()),
            quote!(()),
            quote!(()),
            quote! {
                type Params<'req> = ();

                fn from_params<'req, 'body>(
                    _: &#ns_core::request::raw::Parsed<
                        #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, Self,
                    >,
                ) -> ::core::result::Result<
                    Self::Params<'req>,
                    <Self as #ns_core::route::Project>::Error<'body>,
                > {
                    ::core::result::Result::Ok(())
                }
            },
        )
    } else {
        (
            quote!(#raw_path),
            quote! {
                #params<'req, #ns_core::request::mode::Ref<'req>>
            },
            quote! {
                #params<'d, #ns_core::request::mode::Own>
            },
            quote! {
                type Params<'req> = #params<
                    'req,
                    #ns_core::request::mode::Ref<'req>,
                >;

                fn from_params<'req, 'body>(
                    #v_req: &#ns_core::request::raw::Parsed<
                        #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, Self,
                    >,
                ) -> ::core::result::Result<
                    Self::Params<'req>,
                    <Self as #ns_core::route::Project>::Error<'body>,
                > {
                    #ref_p
                }
            },
        )
    };
    let own_head = quote! {
        #headers<'d, #ns_core::request::mode::Own>
    };
    let pstart = &expand.start;
    let owned = quote! {
        #pstart
        let #v_heads = #headers::<'d, #ns_core::request::mode::Own>
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
    } = task.build(paths, plan);
    let path_in = if captures.is_empty() {
        quote!(())
    } else {
        quote!(#v_params)
    };
    let hbind = if head.is_empty() && queries.is_empty() {
        quote!(_)
    } else {
        quote!(#v_heads)
    };
    let head_in = match has_peer {
        true => quote!((#hbind, #v_auth)),
        false => hbind,
    };
    let hbase = quote! {
        #headers<'req, #ns_core::request::mode::Ref<'req>>
    };
    let head_ty = match has_peer {
        true => quote! {
            (#hbase, &'req [::core::primitive::u8],)
        },
        false => hbase,
    };
    let head_out = match has_peer {
        true => quote! {
            let #v_heads = #build;
            #ref_peer
            ::core::result::Result::Ok((#v_heads, #v_auth))
        },
        false => quote!(::core::result::Result::Ok(#build)),
    };
    let body_in = body.input();
    let req_in = body.request();
    let project = quote! {
            type View<'req, 'd> =
                #name<'d, #ns_core::request::mode::Ref<'req>>
            where
                'd: 'req;

            fn from_parts<'req, 'd>(
                #path_in: #path_ty,
                #head_in: #head_ty,
                #body_in: #body_arg,
                #req_in: &#ns_core::request::View<'req, 'd, #frame>,
            ) -> Self::View<'req, 'd>
            where
                'd: 'req,
            {
                #name::<'d, #ns_core::request::mode::Ref<'req>> {
                    #(#ref_init,)*
                }
            }
    };
    Ok(quote! {
        #( #admitted )*
        #json_decl
        #modes
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
            #path_api
            type Headers<'req> = #head_ty;
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
            #project
            #parsed
            fn from_headers<'req, 'body>(
                #v_req: &#ns_core::request::raw::Parsed<
                    #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, Self,
                >,
            ) -> ::core::result::Result<
                Self::Headers<'req>,
                <Self as #ns_core::route::Project>::Error<'body>,
            > {
                #head_out
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
