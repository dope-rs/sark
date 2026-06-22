mod decode;
mod encode;
mod field;
mod scalar;
mod scan;

use decode::Decode;
use encode::Encode;
use field::FieldMode;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use scan::Scan;
use syn::parse::{Parse, ParseStream};
use syn::{Fields, Ident, ItemStruct, Result, Type};

use crate::codegen::value::Value;
use crate::util::TypeExt;

#[derive(Clone, Copy)]
pub(super) enum JsonKind {
    Unordered,
    Ordered,
}

pub(super) struct JsonMode {
    pub(super) kind: JsonKind,
    pub(super) preserve: bool,
    pub(super) exact: bool,
    pub(super) plain: bool,
}

impl Parse for JsonMode {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.is_empty() {
            return Ok(Self {
                kind: JsonKind::Unordered,
                preserve: false,
                exact: false,
                plain: false,
            });
        }
        let mut kind = JsonKind::Unordered;
        let mut preserve = false;
        let mut exact = false;
        let mut plain = false;
        while !input.is_empty() {
            let ident = input.parse::<Ident>()?;
            if ident == "ordered" {
                kind = JsonKind::Ordered;
            } else if ident == "unordered" {
                kind = JsonKind::Unordered;
            } else if ident == "preserve" {
                preserve = true;
            } else if ident == "exact" {
                exact = true;
            } else if ident == "plain" {
                plain = true;
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "#[sark_gen::json] supports only `ordered`, `unordered`, `preserve`, `exact`, or `plain`",
                ));
            }
            if input.is_empty() {
                break;
            }
            input.parse::<syn::Token![,]>()?;
        }
        if exact && !matches!(kind, JsonKind::Ordered) {
            return Err(syn::Error::new(
                Span::call_site(),
                "`exact` requires `ordered`",
            ));
        }
        Ok(Self {
            kind,
            preserve,
            exact,
            plain,
        })
    }
}

struct Plan<'a> {
    mode: &'a JsonMode,
    idents: Vec<Ident>,
    tys: Vec<Type>,
    names: Vec<Vec<u8>>,
    modes: Vec<FieldMode>,
}

impl<'a> Plan<'a> {
    fn locals(&self) -> Vec<TokenStream> {
        self.idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.modes.iter())
            .map(|((ident, ty), fmode)| {
                if fmode.unused && self.mode.preserve {
                    quote!()
                } else if ty.option_inner().is_some() {
                    quote!(let mut #ident = None;)
                } else {
                    quote!(let mut #ident: Option<#ty> = None;)
                }
            })
            .collect()
    }

    fn match_arms(&self) -> Result<Vec<TokenStream>> {
        let entries = self
            .idents
            .iter()
            .zip(self.names.iter())
            .zip(self.tys.iter())
            .zip(self.modes.iter())
            .map(|(((ident, name), ty), fmode)| {
                let lit = syn::LitByteStr::new(name.as_slice(), Span::call_site());
                let decode = Decode::expr(ty, *fmode)?;
                Ok(if fmode.unused && self.mode.preserve {
                    let skip = Plan::skip(fmode.plain);
                    (
                        name.len(),
                        quote! {
                            if __name == #lit {
                                #skip
                                __handled = true;
                            }
                        },
                    )
                } else {
                    (
                        name.len(),
                        quote! {
                            if __name == #lit {
                                if #ident.is_none() {
                                    #ident = Some(#decode);
                                } else {
                                    sark::json::Scan::skip_value(__raw, &mut __idx)?;
                                }
                                __handled = true;
                            }
                        },
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Value::group_arms_by_length(entries))
    }

    fn finals(&self) -> Result<Vec<TokenStream>> {
        self.idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.modes.iter())
            .map(|((ident, ty), fmode)| {
                if fmode.unused && self.mode.preserve {
                    Decode::empty(ty).map(|empty| quote!(#ident: #empty))
                } else if ty.option_inner().is_some() {
                    Ok(quote!(#ident: #ident))
                } else {
                    Ok(quote! {
                        #ident: #ident.ok_or_else(|| {
                            sark::json::Json::bad_request(
                                concat!("Missing JSON field: ", stringify!(#ident)),
                            )
                        })?
                    })
                }
            })
            .collect()
    }

    fn ordered_steps(&self) -> Result<Vec<TokenStream>> {
        self.idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.names.iter())
            .zip(self.modes.iter())
            .enumerate()
            .map(|(idx, (((ident, ty), name), fmode))| {
                let name = syn::LitByteStr::new(name, Span::call_site());
                let decode = Decode::expr(ty, *fmode)?;
                let first = idx == 0;
                Ok(if fmode.unused && self.mode.preserve {
                    let skip = Plan::skip(fmode.plain);
                    quote! {
                        sark::json::Scan::expect_prop(__raw, &mut __idx, #first, #name)?;
                        #skip
                    }
                } else {
                    quote! {
                        sark::json::Scan::expect_prop(__raw, &mut __idx, #first, #name)?;
                        #ident = Some(#decode);
                    }
                })
            })
            .collect()
    }

    fn exact_steps(&self) -> Result<Vec<TokenStream>> {
        self.idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.names.iter())
            .zip(self.modes.iter())
            .map(|(((ident, ty), name), fmode)| {
                let lit = syn::LitByteStr::new(name, Span::call_site());
                let decode = Decode::expr(ty, *fmode)?;
                Ok(if fmode.unused && self.mode.preserve {
                    quote! {
                        sark::json::Scan::seek_name(__raw, &mut __idx, #lit)?;
                    }
                } else {
                    quote! {
                        sark::json::Scan::seek_name(__raw, &mut __idx, #lit)?;
                        #ident = Some(#decode);
                    }
                })
            })
            .collect()
    }

    fn field_heads(&self) -> Vec<Vec<u8>> {
        self.names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let name = std::str::from_utf8(name).expect("field utf8");
                if idx == 0 {
                    format!("{{\"{name}\":").into_bytes()
                } else {
                    format!(",\"{name}\":").into_bytes()
                }
            })
            .collect()
    }

    fn encoders_and_leners(&self) -> Result<(Vec<TokenStream>, Vec<TokenStream>)> {
        let heads = self.field_heads();
        let mut encoders = Vec::with_capacity(self.idents.len());
        let mut leners = Vec::with_capacity(self.idents.len());
        for (((ident, ty), fmode), head) in self
            .idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.modes.iter())
            .zip(heads.iter())
        {
            let head_lit = syn::LitByteStr::new(head, Span::call_site());
            let write = Encode::write_expr(ty, *fmode, quote!(self.#ident))?;
            encoders.push(quote! {
                __out.extend_from_slice(#head_lit);
                #write
            });
            let len = Encode::len_expr(ty, *fmode, quote!(self.#ident))?;
            let head_len = head.len();
            leners.push(quote!(#head_len + (#len)));
        }
        Ok((encoders, leners))
    }

    fn scan_fields(&self) -> Result<Vec<TokenStream>> {
        if !self.mode.exact {
            return Ok(Vec::new());
        }
        self.idents
            .iter()
            .zip(self.tys.iter())
            .zip(self.names.iter())
            .map(|((ident, ty), name)| {
                let scan = Scan::field(name, ty, quote!(__chunks.iter().copied()))?;
                Ok(quote!(#ident: { #scan }))
            })
            .collect()
    }

    fn scan_one(&self) -> Result<Option<TokenStream>> {
        if self.mode.exact && self.idents.len() == 1 {
            Ok(Some(Scan::field(
                &self.names[0],
                &self.tys[0],
                quote!(chunks),
            )?))
        } else {
            Ok(None)
        }
    }

    fn has_owned(&self) -> bool {
        self.mode.preserve
            || self.modes.iter().any(|fmode| fmode.nested || fmode.seq)
            || self.tys.iter().any(|ty| {
                let base = ty.option_inner().unwrap_or(ty);
                base.is_plain_ident("LocalFrameBytes")
            })
    }

    fn skip(plain: bool) -> TokenStream {
        if plain {
            quote!(sark::json::Scan::skip_plain_string(__raw, &mut __idx)?;)
        } else {
            quote!(sark::json::Scan::skip_value(__raw, &mut __idx)?;)
        }
    }
}

pub(super) fn attr(mode: JsonMode, mut st: ItemStruct) -> Result<TokenStream> {
    let name = st.ident.clone();
    let raw_field = Ident::new("__json_raw", Span::call_site());
    let (fields, modes) = match &mut st.fields {
        Fields::Named(named) => {
            let modes = named
                .named
                .iter()
                .map(|field| FieldMode::from_field(field, mode.plain))
                .collect::<Result<Vec<_>>>()?;
            for field in &mut named.named {
                field.attrs.retain(|attr| {
                    !attr.path().is_ident("raw")
                        && !attr.path().is_ident("unused")
                        && !attr.path().is_ident("plain")
                        && !attr.path().is_ident("field")
                });
            }
            for (field, mode) in named.named.iter_mut().zip(modes.iter()) {
                if mode.unused {
                    field.attrs.push(syn::parse_quote!(#[allow(dead_code)]));
                }
            }
            if mode.preserve {
                named.named.push(syn::parse_quote!(
                    #[doc(hidden)]
                    #[allow(dead_code)]
                    pub __json_raw: o3::buffer::Shared
                ));
            }
            let keep = if mode.preserve {
                named.named.len().saturating_sub(1)
            } else {
                named.named.len()
            };
            (
                named.named.iter().take(keep).collect::<Vec<_>>(),
                modes.into_iter().take(keep).collect::<Vec<_>>(),
            )
        }
        _ => {
            return Err(syn::Error::new(
                Span::call_site(),
                "#[sark_gen::json] requires a named-field struct",
            ));
        }
    };

    let idents: Vec<_> = fields
        .iter()
        .map(|field| {
            field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new(Span::call_site(), "named field required"))
        })
        .collect::<Result<Vec<_>>>()?;
    let tys: Vec<_> = fields.iter().map(|field| field.ty.clone()).collect();
    let names: Vec<_> = idents
        .iter()
        .map(|ident| ident.to_string().into_bytes())
        .collect::<Vec<_>>();

    let plan = Plan {
        mode: &mode,
        idents,
        tys,
        names,
        modes,
    };

    if mode.exact && plan.modes.iter().any(|fmode| fmode.nested || fmode.seq) {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[field(nested)] and #[field(seq)] are not supported with `exact`",
        ));
    }

    let locals = plan.locals();
    let match_arms = plan.match_arms()?;
    let finals = plan.finals()?;
    let ordered_steps = plan.ordered_steps()?;
    let exact_steps = plan.exact_steps()?;
    let (encoders, leners) = plan.encoders_and_leners()?;
    let scan_fields = plan.scan_fields()?;
    let scan_one = plan.scan_one()?;
    let has_owned = plan.has_owned();

    let scan_prelude = if mode.preserve {
        quote! {
            let mut __raw_acc = o3::buffer::Owned::new();
            for __chunk in __chunks.iter().copied() {
                __raw_acc.extend_from_slice(__chunk);
            }
        }
    } else {
        quote! {}
    };
    let scan_tail = if mode.preserve {
        quote!(#raw_field: __raw_acc.freeze(),)
    } else {
        quote! {}
    };
    let scan_impl =
        if let Some(scan_one) = scan_one.as_ref().filter(|_| mode.exact && !mode.preserve) {
            let ident = &plan.idents[0];
            quote! {
                impl sark::json::JsonScan for #name {
                    fn scan_json<'a, I>(chunks: I) -> sark::json::Result<Self>
                    where
                        I: IntoIterator<Item = &'a [u8]>,
                    {
                        Ok(Self {
                            #ident: { #scan_one }
                        })
                    }
                }
            }
        } else if mode.exact {
            quote! {
                impl sark::json::JsonScan for #name {
                    fn scan_json<'a, I>(chunks: I) -> sark::json::Result<Self>
                    where
                        I: IntoIterator<Item = &'a [u8]>,
                    {
                        let __chunks: Vec<&'a [u8]> = chunks.into_iter().collect();
                        #scan_prelude
                        Ok(Self {
                            #(#scan_fields,)*
                            #scan_tail
                        })
                    }
                }
            }
        } else {
            quote! {
                impl sark::json::JsonScan for #name {
                    fn scan_json<'a, I>(chunks: I) -> sark::json::Result<Self>
                    where
                        I: IntoIterator<Item = &'a [u8]>,
                    {
                        let mut __out = o3::buffer::Owned::new();
                        for __chunk in chunks {
                            __out.extend_from_slice(__chunk);
                        }
                        <Self as sark::json::JsonDecode>::decode_json(__out.freeze())
                    }
                }
            }
        };

    let decode_body = match (mode.kind, mode.exact) {
        (JsonKind::Ordered, true) => quote! {
            #(
                #exact_steps
            )*
        },
        (JsonKind::Unordered, _) => quote! {
            loop {
                sark::json::Scan::ws(__raw, &mut __idx);
                if sark::json::Scan::eat_byte(__raw, &mut __idx, b'}') {
                    break;
                }
                let __name = sark::json::Scan::str_slice(__raw, &mut __idx)?;
                sark::json::Scan::ws(__raw, &mut __idx);
                sark::json::Scan::expect_byte(__raw, &mut __idx, b':')?;
                sark::json::Scan::ws(__raw, &mut __idx);
                let mut __handled = false;
                match __name.len() {
                    #( #match_arms )*
                    _ => {}
                }
                if !__handled {
                    sark::json::Scan::skip_value(__raw, &mut __idx)?;
                }
                sark::json::Scan::ws(__raw, &mut __idx);
                if sark::json::Scan::eat_byte(__raw, &mut __idx, b',') {
                    continue;
                }
                sark::json::Scan::expect_byte(__raw, &mut __idx, b'}')?;
                break;
            }
        },
        (JsonKind::Ordered, false) => quote! {
            #(
                #ordered_steps
            )*
            sark::json::Scan::ws(__raw, &mut __idx);
            sark::json::Scan::expect_byte(__raw, &mut __idx, b'}')?;
        },
    };

    let ctor = if mode.preserve {
        let ctor_args = plan
            .idents
            .iter()
            .zip(plan.tys.iter())
            .map(|(ident, ty)| quote!(#ident: #ty));
        let ctor_fields = plan.idents.iter();
        Some(quote! {
            impl #name {
                pub fn new(#(#ctor_args),*) -> Self {
                    Self {
                        #(#ctor_fields),*,
                        #raw_field: o3::buffer::Shared::new(),
                    }
                }
            }
        })
    } else {
        None
    };
    let preserve_impl = if mode.preserve {
        Some(quote! {
            impl sark::json::JsonPreserve for #name {
                fn raw_json(&self) -> Option<&o3::buffer::Shared> {
                    if self.#raw_field.is_empty() {
                        None
                    } else {
                        Some(&self.#raw_field)
                    }
                }
            }
        })
    } else {
        None
    };
    let init_fields = if mode.preserve {
        quote!(#(#finals,)* #raw_field: __bytes)
    } else {
        quote!(#(#finals,)*)
    };

    let decode_borrowed = if has_owned {
        quote! {}
    } else {
        quote! {
            fn decode_json_borrowed(__raw: &[u8]) -> sark::json::Result<Self> {
                let mut __idx = 0usize;
                sark::json::Scan::ws(__raw, &mut __idx);
                sark::json::Scan::expect_byte(__raw, &mut __idx, b'{')?;
                #(#locals)*
                #decode_body
                Ok(Self { #init_fields })
            }
        }
    };

    st.attrs.retain(|attr| !attr.path().is_ident("json"));
    Ok(quote! {
        #st
        #ctor

        impl sark::json::JsonDecode for #name {
            fn decode_json(__bytes: o3::buffer::Shared) -> sark::json::Result<Self> {
                let __raw = __bytes.as_ref();
                let mut __idx = 0usize;
                sark::json::Scan::ws(__raw, &mut __idx);
                sark::json::Scan::expect_byte(__raw, &mut __idx, b'{')?;
                #(#locals)*
                #decode_body
                Ok(Self { #init_fields })
            }
            #decode_borrowed
        }

        impl sark::json::JsonEncode for #name {
            fn json_len(&self) -> usize {
                1usize #( + #leners )*
            }

            fn write_json(&self, __out: &mut o3::buffer::Owned) {
                #( #encoders )*
                __out.extend_from_slice(b"}");
            }
        }

        #scan_impl
        #preserve_impl
    })
}
