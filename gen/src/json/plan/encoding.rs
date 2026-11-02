use crate::{
    json::{field, scalar},
    runtime::paths::Paths,
    types,
};

pub(in crate::json) struct Names {
    pub(super) input: syn::Lifetime,
    pub(super) sink: syn::Ident,
    pub(super) writer: syn::Ident,
    pub(super) size: syn::Ident,
    item: syn::Ident,
    len: syn::Ident,
    first: syn::Ident,
    value: syn::Ident,
}

impl Names {
    pub(in crate::json) fn new(generics: &syn::Generics) -> Self {
        let names = types::Generics::new(generics.clone());
        Self {
            input: names.lifetime("__w"),
            sink: names.ident("__W"),
            writer: names.local("__w"),
            size: names.local("__size"),
            item: names.local("__e"),
            len: names.local("__n"),
            first: names.local("__first"),
            value: names.local("value"),
        }
    }
}

pub(in crate::json) fn emit(
    paths: &Paths,
    generics: &syn::Generics,
    target: proc_macro2::TokenStream,
    names: &Names,
    lengths: &[proc_macro2::TokenStream],
    writes: &[proc_macro2::TokenStream],
) -> proc_macro2::TokenStream {
    let (imp, _, bounds) = generics.split_for_impl();
    let size = size(
        paths,
        &quote::quote!(#imp),
        &target,
        &quote::quote!(#bounds),
        &names.size,
        lengths,
    );
    let write = write(paths, generics, &target, names, writes);
    quote::quote! { #size #write }
}

fn write(
    paths: &Paths,
    generics: &syn::Generics,
    target: &proc_macro2::TokenStream,
    names: &Names,
    writes: &[proc_macro2::TokenStream],
) -> proc_macro2::TokenStream {
    let ns_json = &paths.json;
    let (imp, _, bounds) = generics.split_for_impl();
    let lifetime = &names.input;
    let sink = &names.sink;
    let writer = &names.writer;
    let open = writes
        .is_empty()
        .then(|| call(names, quote::quote!(put), quote::quote!(b"{")));
    let close = call(names, quote::quote!(put), quote::quote!(b"}"));
    quote::quote! {
        impl #imp #ns_json::writing::TryEncode for #target #bounds {
            fn try_write<#lifetime, #sink: #ns_json::o3::buffer::write::Sink>(
                &self,
                mut #writer: #ns_json::writing::Writer<#lifetime, #sink>,
            ) -> ::core::ops::ControlFlow<
                #ns_json::writing::Failure<#lifetime, #sink>,
                #ns_json::writing::Writer<#lifetime, #sink>,
            > {
                #open
                #( #writes )*
                #close
                ::core::ops::ControlFlow::Continue(#writer)
            }
        }
    }
}

fn size(
    paths: &Paths,
    imp: &proc_macro2::TokenStream,
    target: &proc_macro2::TokenStream,
    bounds: &proc_macro2::TokenStream,
    size: &syn::Ident,
    lengths: &[proc_macro2::TokenStream],
) -> proc_macro2::TokenStream {
    let ns_json = &paths.json;
    let ns_http = &paths.http;
    let base = if lengths.is_empty() { 2usize } else { 1usize };
    quote::quote! {
        impl #imp #ns_json::writing::TrySize for #target #bounds {
            fn try_size(&self) -> ::core::result::Result<::core::primitive::usize, #ns_http::payload::response::exact::Overflow> {
                let mut #size = #base;
                #( #size = #ns_http::payload::response::exact::Overflow::try_add(#size, #lengths)?; )*
                ::core::result::Result::Ok(#size)
            }
        }
    }
}

pub(in crate::json) fn call(
    names: &Names,
    method: proc_macro2::TokenStream,
    value: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let writer = &names.writer;
    quote::quote!(#writer = #writer.#method(#value)?;)
}

pub(super) struct Emission {
    pub(super) len: proc_macro2::TokenStream,
    pub(super) write: proc_macro2::TokenStream,
}

enum Value {
    U64,
    I64,
    F64,
    Bool,
    Bytes { encoding: ByteEncoding },
    Nested { borrow: bool },
    Sequence(Box<Self>),
}

#[derive(Clone, Copy)]
enum OptionAccess {
    Copy,
    Ref,
}

#[derive(Clone, Copy)]
enum ByteEncoding {
    Raw,
    Plain,
    Escaped,
}

impl Value {
    fn scalar(
        scalar: scalar::Scalar,
        mode: field::FieldMode,
        sequence: bool,
        ty: &syn::Type,
    ) -> syn::Result<Self> {
        use crate::json::scalar::Scalar;
        let encoding = ByteEncoding::from(mode);
        Ok(match scalar {
            Scalar::U64 if !sequence => Self::U64,
            Scalar::I64 if !sequence => Self::I64,
            Scalar::F64 if !sequence => Self::F64,
            Scalar::Bool if !sequence => Self::Bool,
            Scalar::String | Scalar::Shared | Scalar::Bytes | Scalar::Borrowed | Scalar::Token => {
                Self::Bytes { encoding }
            }
            Scalar::U64 | Scalar::I64 | Scalar::F64 | Scalar::Bool => {
                return Err(syn::Error::new_spanned(
                    ty,
                    "sequence field element must be a byte string",
                ));
            }
        })
    }

    fn emit(
        &self,
        paths: &Paths,
        access: proc_macro2::TokenStream,
        borrowed: bool,
        names: &Names,
    ) -> Emission {
        let ns_json = &paths.json;
        let writer = &names.writer;
        let item = &names.item;
        match self {
            Self::U64 => Emission::new(
                quote::quote!(#ns_json::writing::Size::json_size(&#access)),
                call(names, quote::quote!(put_u64), access),
            ),
            Self::I64 => Emission::new(
                quote::quote!(#ns_json::writing::Size::json_size(&#access)),
                call(names, quote::quote!(put_i64), access),
            ),
            Self::F64 => Emission::new(
                quote::quote!(#ns_json::writing::Size::json_size(&#access)),
                call(names, quote::quote!(put_f64), access),
            ),
            Self::Bool => {
                let yes = call(names, quote::quote!(put), quote::quote!(b"true"));
                let no = call(names, quote::quote!(put), quote::quote!(b"false"));
                Emission::new(
                    quote::quote!(if #access { 4usize } else { 5usize }),
                    quote::quote! {
                    if #access {
                        #yes
                    } else {
                        #no
                    }
                    },
                )
            }
            Self::Bytes { encoding } => Emission::bytes(paths, &access, *encoding, borrowed, names),
            Self::Nested { borrow } => {
                let access = if *borrow {
                    quote::quote!(&#access)
                } else {
                    access
                };
                Emission::new(
                    quote::quote!(#ns_json::writing::TrySize::try_size(#access)?),
                    quote::quote!(#writer = #ns_json::writing::TryEncode::try_write(#access, #writer)?;),
                )
            }
            Self::Sequence(element) => {
                let project = matches!(element.as_ref(), Self::Nested { .. }).then(|| {
                    quote::quote! {
                        use #ns_json::writing::Nested as _;
                        let #item = #ns_json::writing::Item(#item);
                        let #item = #item.into_ref();
                    }
                });
                element
                    .emit(paths, quote::quote!(#item), true, names)
                    .sequence(paths, &access, project, names)
            }
        }
    }
}

impl From<scalar::Scalar> for OptionAccess {
    fn from(value: scalar::Scalar) -> Self {
        match value {
            scalar::Scalar::U64
            | scalar::Scalar::I64
            | scalar::Scalar::F64
            | scalar::Scalar::Bool => Self::Copy,
            scalar::Scalar::String
            | scalar::Scalar::Shared
            | scalar::Scalar::Borrowed
            | scalar::Scalar::Bytes
            | scalar::Scalar::Token => Self::Ref,
        }
    }
}

impl OptionAccess {
    fn apply(self, value: &syn::Ident) -> proc_macro2::TokenStream {
        match self {
            Self::Copy => quote::quote!(*#value),
            Self::Ref => quote::quote!(#value),
        }
    }
}

impl From<field::FieldMode> for ByteEncoding {
    fn from(mode: field::FieldMode) -> Self {
        if mode.raw {
            Self::Raw
        } else if mode.plain {
            Self::Plain
        } else {
            Self::Escaped
        }
    }
}

impl Emission {
    pub(super) fn try_value(
        paths: &Paths,
        ty: &syn::Type,
        mode: field::FieldMode,
        access: proc_macro2::TokenStream,
        names: &Names,
    ) -> syn::Result<Self> {
        use crate::json::scalar::Classified;
        let (value, option) = if mode.seq {
            let value = if mode.nested {
                Value::Nested { borrow: false }
            } else {
                let elem = types::View::new(ty).vector().ok_or_else(|| {
                    syn::Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field")
                })?;
                let class = Classified::of(elem)?;
                if class.optional {
                    return Err(syn::Error::new_spanned(
                        elem,
                        "sequence field elements cannot be Option<T>",
                    ));
                }
                Value::scalar(class.scalar, mode, true, elem)?
            };
            (Value::Sequence(Box::new(value)), None)
        } else if mode.nested {
            (Value::Nested { borrow: true }, None)
        } else {
            let class = Classified::of(ty)?;
            (
                Value::scalar(class.scalar, mode, false, ty)?,
                class.optional.then(|| OptionAccess::from(class.scalar)),
            )
        };
        Ok(match option {
            Some(option) => value
                .emit(
                    paths,
                    option.apply(&names.value),
                    matches!(option, OptionAccess::Ref),
                    names,
                )
                .optional(&access, names),
            None => value.emit(paths, access, false, names),
        })
    }

    fn new(len: proc_macro2::TokenStream, write: proc_macro2::TokenStream) -> Self {
        Self { len, write }
    }

    fn bytes(
        paths: &Paths,
        access: &proc_macro2::TokenStream,
        encoding: ByteEncoding,
        borrowed: bool,
        names: &Names,
    ) -> Self {
        let ns_json = &paths.json;
        let ns_http = &paths.http;
        let value = if borrowed {
            access.clone()
        } else {
            quote::quote!(&#access)
        };
        match encoding {
            ByteEncoding::Raw => Self::new(
                quote::quote!((#access).len()),
                call(
                    names,
                    quote::quote!(put),
                    quote::quote!((#value).as_slice()),
                ),
            ),
            ByteEncoding::Plain => Self::new(
                quote::quote!(#ns_http::payload::response::exact::Overflow::try_add(2usize, (#access).len())?),
                call(names, quote::quote!(put_plain), value),
            ),
            ByteEncoding::Escaped => Self::new(
                quote::quote!(#ns_json::writing::TrySize::try_size(#value)?),
                call(names, quote::quote!(put_str), value),
            ),
        }
    }

    fn sequence(
        self,
        paths: &Paths,
        access: &proc_macro2::TokenStream,
        project: Option<proc_macro2::TokenStream>,
        names: &Names,
    ) -> Self {
        let ns_json = &paths.json;
        let ns_http = &paths.http;
        let len = self.len;
        let write = self.write;
        let size = &names.len;
        let first = &names.first;
        let item = &names.item;
        let open = call(names, quote::quote!(put), quote::quote!(b"["));
        let comma = call(names, quote::quote!(put), quote::quote!(b","));
        let close = call(names, quote::quote!(put), quote::quote!(b"]"));
        Self::new(
            quote::quote! {{
                use ::core::iter::IntoIterator as _;
                let mut #size = 2usize;
                let mut #first = true;
                for #item in #ns_json::writing::Item(&#access).into_iter() {
                    #project
                    if !#first {
                        #size = #ns_http::payload::response::exact::Overflow::try_add(#size, 1)?;
                    }
                    #first = false;
                    #size = #ns_http::payload::response::exact::Overflow::try_add(#size, #len)?;
                }
                #size
            }},
            quote::quote! {{
                use ::core::iter::IntoIterator as _;
                #open
                let mut #first = true;
                for #item in #ns_json::writing::Item(&#access).into_iter() {
                    #project
                    if !#first {
                        #comma
                    }
                    #first = false;
                    #write
                }
                #close
            }},
        )
    }

    fn optional(self, access: &proc_macro2::TokenStream, names: &Names) -> Self {
        let len = self.len;
        let write = self.write;
        let value = &names.value;
        let null = call(names, quote::quote!(put), quote::quote!(b"null"));
        Self::new(
            quote::quote! {
                match &#access {
                    ::core::option::Option::Some(#value) => #len,
                    ::core::option::Option::None => 4usize,
                }
            },
            quote::quote! {
                match &#access {
                    ::core::option::Option::Some(#value) => { #write }
                    ::core::option::Option::None => { #null }
                }
            },
        )
    }
}
