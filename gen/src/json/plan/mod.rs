use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitByteStr;

use crate::{macros::tokens, runtime::paths::Paths};

pub(super) struct Plan<'a> {
    pub(super) mode: &'a mode::Mode,
    pub(super) fields: Vec<JsonField>,
}

pub(super) struct JsonField {
    pub(super) ident: syn::Ident,
    pub(super) bind: syn::Ident,
    pub(super) ty: syn::Type,
    pub(super) vis: syn::Visibility,
    pub(super) name: String,
    pub(super) mode: field::FieldMode,
}

pub(super) struct Emission {
    pub(super) lengths: Vec<proc_macro2::TokenStream>,
    pub(super) writes: Vec<proc_macro2::TokenStream>,
}

impl<'a> Plan<'a> {
    pub(super) fn locals(&self, paths: &Paths) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        if matches!(self.mode.kind, mode::JsonKind::Ordered) {
            return Ok(Vec::new());
        }
        self.fields
            .iter()
            .map(|field| {
                let bind = &field.bind;
                let ty = field::proof_type(paths, &field.ty, field.mode)?;
                Ok(quote::quote!(let mut #bind: ::core::option::Option<#ty> = ::core::option::Option::None;))
            })
            .collect()
    }

    pub(super) fn match_arms(&self, paths: &Paths) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let ns_json = &paths.json;
        let [v_raw, v_idx, v_depth] = ["__raw", "__idx", "__depth"].map(tokens::local);
        let [v_name, v_hand] = ["__name", "__handled"].map(tokens::local);
        use crate::types::lengths::LengthArms;
        let entries = self
            .fields
            .iter()
            .map(|field| {
                let bind = &field.bind;
                let lit = LitByteStr::new(field.name.as_bytes(), Span::call_site());
                let decode = sources::expr(paths, &field.ty, field.mode)?;
                Ok((
                    field.name.len(),
                    quote::quote! {
                        if !#v_hand && #v_name.is_equal(#lit) {
                            if #bind.is_none() {
                                #bind = ::core::option::Option::Some(#decode);
                            } else {
                                #ns_json::input::Input::skip_depth(#v_raw, &mut #v_idx, #v_depth)?;
                            }
                            #v_hand = true;
                        }
                    },
                ))
            })
            .collect::<syn::Result<Vec<_>>>()?;
        Ok(LengthArms::collect(entries).emit())
    }

    pub(super) fn finals(&self, paths: &Paths) -> Vec<proc_macro2::TokenStream> {
        let ns_json = &paths.json;
        self.fields
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let bind = &field.bind;
                if matches!(self.mode.kind, mode::JsonKind::Ordered) {
                    quote::quote!(#ident: #bind)
                } else if types::View::new(&field.ty).is_optional() {
                    quote::quote!(#ident: #bind.flatten())
                } else {
                    let name = &field.name;
                    quote::quote! {
                        #ident: #bind
                            .ok_or_else(|| {
                                #ns_json::parsing::Error::Missing(#name)
                            })?
                    }
                }
            })
            .collect()
    }

    pub(super) fn ordered(&self, paths: &Paths) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let ns_json = &paths.json;
        let [v_raw, v_idx] = ["__raw", "__idx"].map(tokens::local);
        self.fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let bind = &field.bind;
                let name = LitByteStr::new(field.name.as_bytes(), Span::call_site());
                let first = idx == 0;
                let ty = field::proof_type(paths, &field.ty, field.mode)?;
                let decode = sources::expr(paths, &field.ty, field.mode)?;
                Ok(quote::quote! {
                    #ns_json::input::Input::expect_prop(#v_raw, &mut #v_idx, #first, #name)?;
                    let #bind: #ty = #decode;
                })
            })
            .collect()
    }

    pub(super) fn borrowed(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_json = &paths.json;
        let [v_raw, v_view] = ["__raw", "__view"].map(tokens::local);
        if self.fields.iter().any(|field| {
            field.mode.nested
                || field.mode.seq
                || types::View::new(types::View::new(&field.ty).value()).is_named("Shared")
        }) {
            return proc_macro2::TokenStream::new();
        }
        let fields = self.fields.iter().map(|field| {
            let ident = &field.ident;
            quote::quote!(#ident: #v_view.#ident)
        });
        quote::quote! {
            fn decode_ref<'__a>(#v_raw: &'__a [::core::primitive::u8]) ->
                ::core::result::Result<Self, #ns_json::parsing::Error<'__a>> {
                let #v_view = <Self as #ns_json::parsing::Request>::decode(#v_raw)?;
                ::core::result::Result::Ok(Self { #(#fields,)* })
            }
        }
    }

    pub(super) fn decode_body(&self, paths: &Paths) -> syn::Result<proc_macro2::TokenStream> {
        let ns_json = &paths.json;
        let [v_raw, v_idx, v_depth] = ["__raw", "__idx", "__depth"].map(tokens::local);
        let [v_name, v_hand] = ["__name", "__handled"].map(tokens::local);
        let body = match self.mode.kind {
            mode::JsonKind::Ordered => {
                let steps = self.ordered(paths)?;
                quote::quote! {
                    #(
                        #steps
                    )*
                    #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                    #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b'}')?;
                }
            }
            mode::JsonKind::Unordered => {
                let arms = self.match_arms(paths)?;
                quote::quote! {
                    #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                    if #ns_json::input::Input::take_byte(#v_raw, &mut #v_idx, b'}')
                        == #ns_json::input::Taken::No
                    {
                        loop {
                            let #v_name = #ns_json::input::Input::name(#v_raw, &mut #v_idx)?;
                            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                            #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b':')?;
                            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                            let mut #v_hand = false;
                            match #v_name.len() {
                                #( #arms )*
                                _ => {}
                            }
                            if !#v_hand {
                                #ns_json::input::Input::skip_depth(#v_raw, &mut #v_idx, #v_depth)?;
                            }
                            #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                            if #ns_json::input::Input::take_byte(#v_raw, &mut #v_idx, b',')
                                == #ns_json::input::Taken::Yes
                            {
                                #ns_json::input::Input::ws(#v_raw, &mut #v_idx);
                                continue;
                            }
                            #ns_json::input::Input::expect_byte(#v_raw, &mut #v_idx, b'}')?;
                            break;
                        }
                    }
                }
            }
        };
        Ok(body)
    }
}

use crate::{
    json::{field, mode, sources},
    types,
};

pub(super) mod encoding;

#[derive(Clone, Copy)]
pub(in crate::json) enum Scope<'a> {
    Object,
    Members(&'a syn::Ident),
}

impl Plan<'_> {
    pub(in crate::json) fn members(
        &self,
        paths: &Paths,
        st: &syn::ItemStruct,
    ) -> syn::Result<TokenStream> {
        let ns_json = &paths.json;
        let ns_http = &paths.http;
        if self.fields.is_empty() {
            return Ok(TokenStream::new());
        }
        let names = types::Generics::new(st.generics.clone());
        let first = names.ident("__FIRST");
        let names = encoding::Names::new(&st.generics);
        let lifetime = &names.input;
        let sink = &names.sink;
        let writer = &names.writer;
        let size = &names.size;
        let Emission { lengths, writes } = self.encoding(paths, Scope::Members(&first), &names)?;
        let name = &st.ident;
        let (imp, ty, bounds) = st.generics.split_for_impl();
        Ok(quote! {
            impl #imp #ns_json::writing::Members for #name #ty #bounds {
                fn try_fields(&self) -> ::core::result::Result<::core::primitive::usize, #ns_http::payload::response::exact::Overflow> {
                    let mut #size = 0usize;
                    #( #size = #ns_http::payload::response::exact::Overflow::try_add(#size, #lengths)?; )*
                    ::core::result::Result::Ok(#size)
                }

                fn try_members<#lifetime, #sink: #ns_json::o3::buffer::write::Sink, const #first: ::core::primitive::bool>(
                    &self,
                    mut #writer: #ns_json::writing::Writer<#lifetime, #sink>,
                ) -> ::core::ops::ControlFlow<
                    #ns_json::writing::Failure<#lifetime, #sink>,
                    #ns_json::writing::Writer<#lifetime, #sink>,
                > {
                    #( #writes )*
                    ::core::ops::ControlFlow::Continue(#writer)
                }
            }
        })
    }

    pub(in crate::json) fn encoding(
        &self,
        paths: &Paths,
        scope: Scope<'_>,
        names: &encoding::Names,
    ) -> syn::Result<Emission> {
        let mut lengths = Vec::with_capacity(self.fields.len());
        let mut writes = Vec::with_capacity(self.fields.len());
        for (index, field) in self.fields.iter().enumerate() {
            let emission = if field.mode.flatten {
                field.members(paths, index, scope, names)
            } else {
                field.encoding(paths, index, scope, names)?
            };
            lengths.push(emission.len);
            writes.push(emission.write);
        }
        Ok(Emission { lengths, writes })
    }
}

impl JsonField {
    fn head(&self, prefix: &[u8]) -> LitByteStr {
        let mut bytes = Vec::with_capacity(self.name.len() + 4);
        bytes.extend_from_slice(prefix);
        bytes.push(b'"');
        for byte in self.name.bytes() {
            match byte {
                b'"' | b'\\' => bytes.extend_from_slice(&[b'\\', byte]),
                0..=0x1f => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    bytes.extend_from_slice(b"\\u00");
                    bytes.push(HEX[usize::from(byte >> 4)]);
                    bytes.push(HEX[usize::from(byte & 0xf)]);
                }
                _ => bytes.push(byte),
            }
        }
        bytes.extend_from_slice(b"\":");
        LitByteStr::new(&bytes, Span::call_site())
    }

    fn encoding(
        &self,
        paths: &Paths,
        index: usize,
        scope: Scope<'_>,
        names: &encoding::Names,
    ) -> syn::Result<encoding::Emission> {
        let ns_http = &paths.http;
        let prefix = match (index, scope) {
            (0, Scope::Object) => b"{".as_slice(),
            (0, Scope::Members(_)) => b"".as_slice(),
            (_, Scope::Object | Scope::Members(_)) => b",".as_slice(),
        };
        let head = self.head(prefix);
        let head_len = head.value().len();
        let put = encoding::call(names, quote!(put), quote!(#head));
        let put = if let (0, Scope::Members(first)) = (index, scope) {
            let comma = self.head(b",");
            let comma = encoding::call(names, quote!(put), quote!(#comma));
            quote! { if #first { #put } else { #comma } }
        } else {
            put
        };
        let ident = &self.ident;
        let value =
            encoding::Emission::try_value(paths, &self.ty, self.mode, quote!(self.#ident), names)?;
        let len = value.len;
        let write = value.write;
        Ok(encoding::Emission {
            len: quote!(#ns_http::payload::response::exact::Overflow::try_add(#head_len, #len)?),
            write: quote! { #put #write },
        })
    }

    fn members(
        &self,
        paths: &Paths,
        index: usize,
        scope: Scope<'_>,
        names: &encoding::Names,
    ) -> encoding::Emission {
        let ns_json = &paths.json;
        let ns_http = &paths.http;
        let ident = &self.ident;
        let writer = &names.writer;
        let (prefix, first, extra) = match (index, scope) {
            (0, Scope::Object) => (
                encoding::call(names, quote!(put), quote!(b"{")),
                quote!(true),
                1usize,
            ),
            (0, Scope::Members(first)) => (TokenStream::new(), quote!(#first), 0),
            (_, Scope::Object | Scope::Members(_)) => (TokenStream::new(), quote!(false), 1),
        };
        encoding::Emission {
            len: quote!(#ns_http::payload::response::exact::Overflow::try_add(
                #extra,
                #ns_json::writing::Members::try_fields(&self.#ident)?,
            )?),
            write: quote! {
                #prefix
                #writer = #ns_json::writing::Members::try_members::<_, #first>(&self.#ident, #writer)?;
            },
        }
    }
}
