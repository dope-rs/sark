use crate::{macros::tokens::local, request::fields, runtime::paths::Paths, types};

fn defaults(
    paths: &Paths,
    kind: types::ValueKind,
    default: &syn::LitStr,
    req: &syn::Ident,
) -> syn::Result<[proc_macro2::TokenStream; 2]> {
    use syn::LitInt;
    let raw = default.value();
    let value = match kind {
        types::ValueKind::Bytes => {
            let ns_http = &paths.http;
            let bytes = syn::LitByteStr::new(raw.as_bytes(), default.span());
            return Ok([
                quote::quote!(&#bytes[..]),
                quote::quote! {
                    #ns_http::request::Bytes::new(
                        #req.owner().id(),
                        #ns_http::o3::buffer::resident::Shared::from(&#bytes[..])
                    )
                },
            ]);
        }
        types::ValueKind::U64 => {
            let n: u64 = match raw.parse() {
                Ok(n) => n,
                Err(error) => return Err(syn::Error::new_spanned(default, error)),
            };
            let lit = LitInt::new(&format!("{n}u64"), default.span());
            quote::quote! { #lit }
        }
        types::ValueKind::Usize => {
            let n: usize = match raw.parse() {
                Ok(n) => n,
                Err(error) => return Err(syn::Error::new_spanned(default, error)),
            };
            let lit = LitInt::new(&format!("{n}usize"), default.span());
            quote::quote! { #lit }
        }
        types::ValueKind::Encoding => {
            return Err(syn::Error::new_spanned(
                default,
                "AcceptEncoding has no default",
            ));
        }
        types::ValueKind::Bool => {
            use syn::LitBool;
            let b = if raw == "true" {
                true
            } else if raw == "false" {
                false
            } else {
                return Err(syn::Error::new_spanned(
                    default,
                    format!("invalid bool default `{raw}`; expected `true` or `false`"),
                ));
            };
            let lit = LitBool::new(b, default.span());
            quote::quote! { #lit }
        }
    };
    Ok([value.clone(), value])
}

fn field(
    paths: &Paths,
    ty: &syn::Type,
    kind: types::ValueKind,
    default: Option<&syn::LitStr>,
    ident: &proc_macro2::Ident,
    req: &syn::Ident,
) -> syn::Result<[proc_macro2::TokenStream; 2]> {
    let ns_core = &paths.core;
    let [range, value] = ["range", "value"].map(local);
    let default = if types::View::new(ty).is_optional() {
        None
    } else {
        let default = default.ok_or_else(|| {
            let message = match kind {
                types::ValueKind::Bytes => {
                    "non-Option request header/query Shared fields require default = \"...\""
                }
                _ => "non-Option request header/query typed fields require default = \"...\"",
            };
            syn::Error::new_spanned(ty, message)
        })?;
        Some(defaults(paths, kind, default, req)?)
    };
    Ok([true, false].map(|borrowed| {
        let read = if borrowed {
            quote::quote!(get())
        } else {
            quote::quote!(try_get()?)
        };
        let read = quote::quote! {
            unsafe { #ns_core::request::frame::raw::Bound::new(#req, #range) }.#read
        };
        let fallback = default
            .as_ref()
            .map(|[view, own]| if borrowed { view } else { own });
        match (kind, fallback) {
            (types::ValueKind::Bytes, None) => quote::quote! {
                match #ident {
                    ::core::option::Option::Some(#range) => ::core::option::Option::Some(#read),
                    ::core::option::Option::None => ::core::option::Option::None,
                }
            },
            (types::ValueKind::Bytes, Some(fallback)) => {
                quote::quote! {
                    match #ident {
                        ::core::option::Option::Some(#range) => #read,
                        ::core::option::Option::None => #fallback,
                    }
                }
            }
            (_, None) => quote::quote! { #ident },
            (_, Some(fallback)) => {
                quote::quote! {
                    match #ident {
                        ::core::option::Option::Some(#value) => #value,
                        ::core::option::Option::None => #fallback,
                    }
                }
            }
        }
    }))
}

pub(super) fn build<'a>(
    paths: &Paths,
    vis: &'a syn::Visibility,
    schema: &'a syn::Ident,
    inner: &'a syn::Ident,
    fields: &'a fields::Fields,
    ordered: bool,
    symbols: &types::Generics,
) -> syn::Result<(
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    Vec<types::ValueKind>,
    syn::Ident,
)> {
    let ordered = ordered && !fields.queries.is_empty();
    let entries = fields.headers.iter().chain(&fields.queries);
    let raw = quote::format_ident!("{}HeadersRaw", schema);
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let req = symbols.local("req");
    let [marker, query] = symbols.idents(["__sark_m", "__query"]);
    use syn::Type;
    let names: Vec<&syn::Ident> = entries.clone().map(|field| &field.ident).collect();
    let locals: Vec<_> = (0..names.len())
        .map(|index| symbols.local(&format!("__sark_field{index}")))
        .collect();
    let tys: Vec<&Type> = entries.clone().map(|field| &field.ty).collect();
    let mode = quote::format_ident!("M");
    let modes: Vec<Type> = tys
        .iter()
        .map(|ty| {
            let mut ty = (*ty).clone();
            types::Edit::new(&mut ty).retained(paths, types::RetainedAs::Mode(&mode));
            ty
        })
        .collect();
    let mut kinds = Vec::with_capacity(tys.len());
    let raw_ty: Vec<Type> = tys
        .iter()
        .map(|ty| {
            let view = types::View::new(ty);
            let kind = view.kind()?;
            kinds.push(kind);
            Ok(view.raw(paths, kind))
        })
        .collect::<syn::Result<_>>()?;
    let state = if ordered {
        quote::quote! { #query: ::core::primitive::u8, }
    } else {
        quote::quote! {}
    };
    let mut views = Vec::new();
    let mut owned = Vec::new();
    for ((spec, kind), local) in entries.zip(&kinds).zip(&locals) {
        let [view, own] = field(paths, &spec.ty, *kind, spec.default.as_ref(), local, &req)?;
        views.push(view);
        owned.push(own);
    }
    let request = if names.is_empty() {
        quote::quote!(_)
    } else {
        quote::quote!(#req)
    };
    let state_in = if ordered {
        quote::quote!(#query: _,)
    } else {
        proc_macro2::TokenStream::new()
    };
    let read = (!names.is_empty()).then(|| {
        quote::quote! {
            let #raw { #( #names: #locals, )* #state_in } = *#req.headers();
        }
    });
    let frame = quote::quote!(<#schema<'static> as #ns_core::route::Schema>::Frame);
    let declaration = quote::quote! {
        #vis struct #inner<
            'd,
            M: #ns_core::request::mode::Mode<'d> =
                #ns_core::request::mode::Ref<'d>,
        > {
            #( #names: #modes, )*
            #[doc(hidden)]
            #marker: ::core::marker::PhantomData<fn(&'d (), M) -> (&'d (), M)>,
        }

        #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::default::Default)]
        #vis struct #raw { #( #names: #raw_ty, )* #state }
    };
    let code = quote::quote! {
        impl<'req> #inner<
            'req,
            #ns_core::request::mode::Ref<'req>,
        > {
            fn from_head(
                #request: &#ns_core::request::raw::Parsed<
                    #ns_core::request::Head<&'req [::core::primitive::u8], #frame>, #schema<'static>,
                >,
            ) -> Self {
                #read
                Self {
                    #( #names: #views, )*
                    #marker: ::core::marker::PhantomData,
                }
            }
        }

        impl<'d> #inner<'d, #ns_core::request::mode::Own> {
            fn try_from_task(
                #request: &#ns_core::request::raw::Parsed<
                    #ns_core::request::Parts<'d, #frame>, #schema<'static>,
                >,
            ) -> ::core::result::Result<Self, #ns_http::o3::mem::rc::Error> {
                #read
                ::core::result::Result::Ok(Self {
                    #( #names: #owned, )*
                    #marker: ::core::marker::PhantomData,
                })
            }
        }
    };
    Ok((declaration, code, kinds, raw))
}
