mod decode;
mod encode;
mod field;
mod scalar;
mod scan;

use decode::Decoder;
use encode::ValuePlan;
use field::FieldMode;
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use scan::FieldScanner;
use syn::parse::{Parse, ParseStream};
use syn::{Fields, Ident, ItemStruct, Result, Type, Visibility};

use crate::codegen::value::LengthArms;
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
    pub(super) encode_only: bool,
}

impl Parse for JsonMode {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.is_empty() {
            return Ok(Self {
                kind: JsonKind::Unordered,
                preserve: false,
                exact: false,
                plain: false,
                encode_only: false,
            });
        }
        let mut kind = JsonKind::Unordered;
        let mut preserve = false;
        let mut exact = false;
        let mut plain = false;
        let mut encode_only = false;
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
            } else if ident == "encode" {
                encode_only = true;
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "#[sark_gen::json] supports only `ordered`, `unordered`, `preserve`, `exact`, `plain`, or `encode`",
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
        if encode_only && (preserve || exact) {
            return Err(syn::Error::new(
                Span::call_site(),
                "`encode` cannot be combined with `preserve` or `exact`",
            ));
        }
        Ok(Self {
            kind,
            preserve,
            exact,
            plain,
            encode_only,
        })
    }
}

struct Plan<'a> {
    mode: &'a JsonMode,
    fields: Vec<JsonField>,
    request_view: bool,
}

struct JsonField {
    ident: Ident,
    bind: Ident,
    ty: Type,
    vis: Visibility,
    name: Vec<u8>,
    mode: FieldMode,
}

struct ObjectEmission {
    lengths: Vec<TokenStream>,
    writes: Vec<TokenStream>,
}

fn json_encode_impl(
    impl_generics: TokenStream,
    target: TokenStream,
    object_base_len: usize,
    object_open: &TokenStream,
    lengths: &[TokenStream],
    writes: &[TokenStream],
) -> TokenStream {
    quote! {
        impl #impl_generics sark::json::JsonEncode for #target {
            fn json_len(&self) -> usize {
                #object_base_len #( + #lengths )*
            }

            fn write_into<__W: sark::json::Write>(
                &self,
                __w: &mut __W,
            ) -> ::core::result::Result<(), __W::Error> {
                #object_open
                #( #writes )*
                __w.put(b"}")?;
                Ok(())
            }
        }
    }
}

impl<'a> Plan<'a> {
    fn decoder<'field>(&self, field: &'field JsonField) -> Decoder<'field> {
        if self.request_view {
            Decoder::request_view(&field.ty, field.mode)
        } else {
            Decoder::new(&field.ty, field.mode)
        }
    }

    fn locals(&self) -> Vec<TokenStream> {
        self.fields
            .iter()
            .map(|field| {
                let bind = &field.bind;
                let ty = &field.ty;
                if field.mode.unused && self.mode.preserve {
                    quote!()
                } else if ty.option_inner().is_some() {
                    quote!(let mut #bind = None;)
                } else {
                    quote!(let mut #bind: Option<#ty> = None;)
                }
            })
            .collect()
    }

    fn match_arms(&self) -> Result<Vec<TokenStream>> {
        let entries = self
            .fields
            .iter()
            .map(|field| {
                let bind = &field.bind;
                let lit = syn::LitByteStr::new(&field.name, Span::call_site());
                Ok(if field.mode.unused && self.mode.preserve {
                    let skip = Plan::skip(field.mode.plain);
                    (
                        field.name.len(),
                        quote! {
                            if __name == #lit {
                                #skip
                                __handled = true;
                            }
                        },
                    )
                } else {
                    let decode = self.decoder(field).expr()?;
                    (
                        field.name.len(),
                        quote! {
                            if __name == #lit {
                                if #bind.is_none() {
                                    #bind = Some(#decode);
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
        Ok(LengthArms::collect(entries).emit())
    }

    fn finals(&self) -> Result<Vec<TokenStream>> {
        self.fields
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let bind = &field.bind;
                if field.mode.unused && self.mode.preserve {
                    self.decoder(field)
                        .empty()
                        .map(|empty| quote!(#ident: #empty))
                } else if field.ty.option_inner().is_some() {
                    Ok(quote!(#ident: #bind))
                } else {
                    Ok(quote! {
                        #ident: #bind.ok_or_else(|| {
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
        self.fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let bind = &field.bind;
                let name = syn::LitByteStr::new(&field.name, Span::call_site());
                let first = idx == 0;
                Ok(if field.mode.unused && self.mode.preserve {
                    let skip = Plan::skip(field.mode.plain);
                    quote! {
                        sark::json::Scan::expect_prop(__raw, &mut __idx, #first, #name)?;
                        #skip
                    }
                } else {
                    let decode = self.decoder(field).expr()?;
                    quote! {
                        sark::json::Scan::expect_prop(__raw, &mut __idx, #first, #name)?;
                        #bind = Some(#decode);
                    }
                })
            })
            .collect()
    }

    fn exact_steps(&self) -> Result<Vec<TokenStream>> {
        self.fields
            .iter()
            .map(|field| {
                let bind = &field.bind;
                let lit = syn::LitByteStr::new(&field.name, Span::call_site());
                Ok(if field.mode.unused && self.mode.preserve {
                    quote! {
                        sark::json::Scan::seek_name(__raw, &mut __idx, #lit)?;
                    }
                } else {
                    let decode = self.decoder(field).expr()?;
                    quote! {
                        sark::json::Scan::seek_name(__raw, &mut __idx, #lit)?;
                        #bind = Some(#decode);
                    }
                })
            })
            .collect()
    }

    fn field_heads(&self) -> Vec<Vec<u8>> {
        self.fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let mut head = Vec::with_capacity(field.name.len() + 4);
                if idx == 0 {
                    head.extend_from_slice(b"{\"");
                } else {
                    head.extend_from_slice(b",\"");
                }
                head.extend_from_slice(&field.name);
                head.extend_from_slice(b"\":");
                head
            })
            .collect()
    }

    fn encoding(&self) -> Result<ObjectEmission> {
        let heads = self.field_heads();
        let mut lengths = Vec::with_capacity(self.fields.len());
        let mut writes = Vec::with_capacity(self.fields.len());
        for (field, head) in self.fields.iter().zip(heads.iter()) {
            let ident = &field.ident;
            let head_lit = syn::LitByteStr::new(head, Span::call_site());
            let emission = ValuePlan::new(&field.ty, field.mode, quote!(self.#ident))?.emit();
            let write = emission.write;
            writes.push(quote! {
                __w.put(#head_lit)?;
                #write
            });
            let len = emission.len;
            let head_len = head.len();
            lengths.push(quote!(#head_len + (#len)));
        }
        Ok(ObjectEmission { lengths, writes })
    }

    fn scan_fields(&self) -> Result<Vec<TokenStream>> {
        if !self.mode.exact {
            return Ok(Vec::new());
        }
        self.fields
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let scan =
                    FieldScanner::new(&field.name, &field.ty, quote!(__chunks.iter().copied()))
                        .emit()?;
                Ok(quote!(#ident: { #scan }))
            })
            .collect()
    }

    fn scan_one(&self) -> Result<Option<TokenStream>> {
        if !self.mode.exact || self.fields.len() != 1 {
            return Ok(None);
        }
        let Some(field) = self.fields.first() else {
            return Ok(None);
        };
        Ok(Some(
            FieldScanner::new(&field.name, &field.ty, quote!(chunks)).emit()?,
        ))
    }

    fn has_owned(&self) -> bool {
        self.mode.preserve
            || self
                .fields
                .iter()
                .any(|field| field.mode.nested || field.mode.seq)
            || self.fields.iter().any(|field| {
                let ty = &field.ty;
                let base = match ty.option_inner() {
                    Some(inner) => inner,
                    None => ty,
                };
                base.is_bytes_with_storage("Retained")
            })
    }

    fn skip(plain: bool) -> TokenStream {
        if plain {
            quote!(sark::json::Scan::skip_plain_string(__raw, &mut __idx)?;)
        } else {
            quote!(sark::json::Scan::skip_value(__raw, &mut __idx)?;)
        }
    }

    fn decode_body(&self) -> Result<TokenStream> {
        Ok(match (self.mode.kind, self.mode.exact) {
            (JsonKind::Ordered, true) => {
                let exact_steps = self.exact_steps()?;
                quote! {
                    #(
                        #exact_steps
                    )*
                }
            }
            (JsonKind::Unordered, _) => {
                let match_arms = self.match_arms()?;
                quote! {
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
                }
            }
            (JsonKind::Ordered, false) => {
                let ordered_steps = self.ordered_steps()?;
                quote! {
                    #(
                        #ordered_steps
                    )*
                    sark::json::Scan::ws(__raw, &mut __idx);
                    sark::json::Scan::expect_byte(__raw, &mut __idx, b'}')?;
                }
            }
        })
    }
}

fn request_view_scalar_type(ty: &Type, mode: FieldMode) -> Type {
    if ty.is_bytes_with_storage("Retained") {
        if mode.raw || mode.plain {
            return syn::parse_quote!(
                ::sark::sark_core::http::Bytes<::sark::sark_core::http::Borrowed<'req>>
            );
        }
        return syn::parse_quote!(::sark::json::JsonBytes<'req>);
    }
    ty.clone()
}

fn request_view_type(ty: &Type, mode: FieldMode) -> Result<Type> {
    if mode.seq || mode.nested {
        return Err(syn::Error::new_spanned(
            ty,
            "request JSON views support only flat fields",
        ));
    }
    if let Some(inner) = ty.option_inner() {
        let inner = request_view_scalar_type(inner, mode);
        return Ok(syn::parse_quote!(Option<#inner>));
    }
    Ok(request_view_scalar_type(ty, mode))
}

impl JsonMode {
    pub(super) fn expand(self, mut st: ItemStruct) -> Result<TokenStream> {
        let mode = self;
        let name = st.ident.clone();
        let vis = st.vis.clone();
        let raw_field = Ident::new("__json_raw", Span::call_site());
        let fields = match &mut st.fields {
            Fields::Named(named) => {
                let parsed = named
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
                for (field, (field_mode, _)) in named.named.iter_mut().zip(parsed.iter()) {
                    if field_mode.unused {
                        field.attrs.push(syn::parse_quote!(#[allow(dead_code)]));
                    }
                }
                let fields = named
                    .named
                    .iter()
                    .zip(parsed)
                    .enumerate()
                    .map(|(idx, (field, (field_mode, name_override)))| {
                        let ident = field.ident.clone().ok_or_else(|| {
                            syn::Error::new(Span::call_site(), "named field required")
                        })?;
                        let field_name = match name_override {
                            Some(name) => name,
                            None => ident.to_string(),
                        };
                        Ok(JsonField {
                            bind: Ident::new(&format!("__f{idx}"), Span::call_site()),
                            ident,
                            ty: field.ty.clone(),
                            vis: field.vis.clone(),
                            name: field_name.into_bytes(),
                            mode: field_mode,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if mode.preserve && !mode.encode_only {
                    named.named.push(syn::parse_quote!(
                        #[doc(hidden)]
                        #[allow(dead_code)]
                        pub __json_raw: o3::buffer::Shared
                    ));
                }
                fields
            }
            _ => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    "#[sark_gen::json] requires a named-field struct",
                ));
            }
        };

        let plan = Plan {
            mode: &mode,
            fields,
            request_view: false,
        };

        if mode.exact
            && plan
                .fields
                .iter()
                .any(|field| field.mode.nested || field.mode.seq)
        {
            return Err(syn::Error::new(
                Span::call_site(),
                "#[field(nested)] and #[field(seq)] are not supported with `exact`",
            ));
        }

        let ObjectEmission { lengths, writes } = plan.encoding()?;
        let object_base_len = if plan.fields.is_empty() {
            2usize
        } else {
            1usize
        };
        let object_open = if plan.fields.is_empty() {
            quote!(__w.put(b"{")?;)
        } else {
            TokenStream::new()
        };
        let encode_impl = json_encode_impl(
            TokenStream::new(),
            quote!(#name),
            object_base_len,
            &object_open,
            &lengths,
            &writes,
        );
        if mode.encode_only {
            st.attrs.retain(|attr| !attr.path().is_ident("json"));
            return Ok(quote! {
                #st

                #encode_impl
            });
        }

        let locals = plan.locals();
        let finals = plan.finals()?;
        let scan_one = if mode.exact && !mode.preserve && plan.fields.len() == 1 {
            plan.scan_one()?
        } else {
            None
        };
        let scan_fields = if mode.exact && scan_one.is_none() {
            plan.scan_fields()?
        } else {
            Vec::new()
        };
        let has_owned = plan.has_owned();

        let scan_prelude = if mode.preserve {
            quote! {
                let mut __raw_acc = Vec::new();
                for __chunk in __chunks.iter().copied() {
                    __raw_acc.extend_from_slice(__chunk);
                }
            }
        } else {
            quote! {}
        };
        let scan_tail = if mode.preserve {
            quote!(#raw_field: o3::buffer::Shared::from(__raw_acc),)
        } else {
            quote! {}
        };
        let single_scan = scan_one.as_ref().zip(plan.fields.first());
        let scan_impl = if let Some((scan_one, field)) = single_scan {
            let ident = &field.ident;
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
                        let mut __out = Vec::new();
                        for __chunk in chunks {
                            __out.extend_from_slice(__chunk);
                        }
                        <Self as sark::json::JsonDecode>::decode_json(
                            o3::buffer::Shared::from(__out)
                        )
                    }
                }
            }
        };

        let decode_body = plan.decode_body()?;

        let ctor = if mode.preserve {
            let ctor_args = plan.fields.iter().map(|field| {
                let ident = &field.ident;
                let ty = &field.ty;
                quote!(#ident: #ty)
            });
            let ctor_fields = plan.fields.iter().map(|field| &field.ident);
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

        let request_view_impl = if mode.preserve
            || plan
                .fields
                .iter()
                .any(|field| field.mode.nested || field.mode.seq)
        {
            TokenStream::new()
        } else {
            let view_name = format_ident!("{}JsonView", name);
            let view_fields = plan
                .fields
                .iter()
                .map(|field| {
                    Ok(JsonField {
                        ident: field.ident.clone(),
                        bind: field.bind.clone(),
                        ty: request_view_type(&field.ty, field.mode)?,
                        vis: field.vis.clone(),
                        name: field.name.clone(),
                        mode: field.mode,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let view_plan = Plan {
                mode: &mode,
                fields: view_fields,
                request_view: true,
            };
            let view_field_decls = view_plan.fields.iter().map(|field| {
                let ident = &field.ident;
                let ty = &field.ty;
                let vis = &field.vis;
                quote!(#vis #ident: #ty)
            });
            let view_locals = view_plan.locals();
            let view_finals = view_plan.finals()?;
            let view_decode_body = view_plan.decode_body()?;
            let ObjectEmission {
                lengths: view_lengths,
                writes: view_writes,
            } = view_plan.encoding()?;
            let view_encode_impl = json_encode_impl(
                quote!(<'req>),
                quote!(#view_name<'req>),
                object_base_len,
                &object_open,
                &view_lengths,
                &view_writes,
            );
            quote! {
                #[allow(dead_code)]
                #vis struct #view_name<'req> {
                    #( #view_field_decls, )*
                    #[doc(hidden)]
                    __sark_json_marker: ::core::marker::PhantomData<&'req ()>,
                }

                impl sark::json::JsonRequestDecode for #name {
                    type View<'req> = #view_name<'req>;

                    fn decode_request<'req>(
                        __raw: &'req [u8],
                    ) -> sark::json::Result<Self::View<'req>> {
                        let mut __idx = 0usize;
                        sark::json::Scan::ws(__raw, &mut __idx);
                        sark::json::Scan::expect_byte(__raw, &mut __idx, b'{')?;
                        #(#view_locals)*
                        #view_decode_body
                        Ok(#view_name {
                            #(#view_finals,)*
                            __sark_json_marker: ::core::marker::PhantomData,
                        })
                    }
                }

                #view_encode_impl
            }
        };

        st.attrs.retain(|attr| !attr.path().is_ident("json"));
        Ok(quote! {
            #st
            #ctor
            #request_view_impl

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

            #encode_impl

            #scan_impl
            #preserve_impl
        })
    }
}
