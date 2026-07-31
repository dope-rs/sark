use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Result, Type};

use super::{
    field::FieldMode,
    scalar::{Classified, Scalar},
};
use crate::util::TypeExt;

pub(super) struct ValuePlan {
    access: TokenStream,
    value: Value,
    optional: bool,
}

pub(super) struct Emission {
    pub(super) len: TokenStream,
    pub(super) write: TokenStream,
}

enum Value {
    U64,
    I64,
    F64,
    Bool,
    Bytes {
        view: ByteView,
        encoding: ByteEncoding,
        length: ByteLength,
    },
    Nested {
        borrow: bool,
    },
    Sequence(Box<Self>),
}

#[derive(Clone, Copy)]
enum ByteView {
    Bytes,
    Slice,
}

#[derive(Clone, Copy)]
enum ByteEncoding {
    Raw,
    Plain,
    Escaped,
}

#[derive(Clone, Copy)]
enum ByteLength {
    Direct,
    View,
}

impl ValuePlan {
    pub(super) fn new(ty: &Type, mode: FieldMode, access: TokenStream) -> Result<Self> {
        let (value, optional) = if mode.seq {
            let elem = ty
                .vec_inner()
                .ok_or_else(|| Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field"))?;
            let value = if mode.nested {
                Value::Nested { borrow: false }
            } else {
                let class = Classified::of(elem)?;
                if class.optional {
                    return Err(Error::new_spanned(
                        elem,
                        "sequence field elements cannot be Option<T>",
                    ));
                }
                Value::scalar(class.scalar, mode, true, elem)?
            };
            (Value::Sequence(Box::new(value)), false)
        } else if mode.nested {
            (Value::Nested { borrow: true }, false)
        } else {
            let class = Classified::of(ty)?;
            (
                Value::scalar(class.scalar, mode, false, ty)?,
                class.optional,
            )
        };
        Ok(Self {
            access,
            value,
            optional,
        })
    }

    pub(super) fn emit(&self) -> Emission {
        if self.optional {
            self.value
                .emit(self.value.optional_access())
                .optional(&self.access)
        } else {
            self.value.emit(self.access.clone())
        }
    }
}

impl Value {
    fn scalar(scalar: Scalar, mode: FieldMode, sequence: bool, ty: &Type) -> Result<Self> {
        let encoding = ByteEncoding::from(mode);
        Ok(match scalar {
            Scalar::U64 if !sequence => Self::U64,
            Scalar::I64 if !sequence => Self::I64,
            Scalar::F64 if !sequence => Self::F64,
            Scalar::Bool if !sequence => Self::Bool,
            Scalar::String => Self::Bytes {
                view: ByteView::Bytes,
                encoding: if sequence {
                    encoding
                } else {
                    ByteEncoding::Escaped
                },
                length: ByteLength::from_sequence(sequence),
            },
            Scalar::Shared | Scalar::Borrowed | Scalar::Retained | Scalar::JsonBytes => {
                Self::Bytes {
                    view: ByteView::Slice,
                    encoding,
                    length: ByteLength::from_sequence(sequence),
                }
            }
            Scalar::InlineToken => Self::Bytes {
                view: ByteView::Bytes,
                encoding,
                length: ByteLength::from_sequence(sequence),
            },
            Scalar::U64 | Scalar::I64 | Scalar::F64 | Scalar::Bool => {
                return Err(Error::new_spanned(
                    ty,
                    "sequence field element must be a byte string",
                ));
            }
        })
    }

    fn emit(&self, access: TokenStream) -> Emission {
        match self {
            Self::U64 => Emission::new(
                quote!(sark::json::Encode::u64_len(#access)),
                quote!(__w.put_u64(#access)?;),
            ),
            Self::I64 => Emission::new(
                quote!(sark::json::Encode::i64_len(#access)),
                quote!(__w.put_i64(#access)?;),
            ),
            Self::F64 => Emission::new(
                quote!(sark::json::Encode::f64_len(#access)),
                quote!(__w.put_f64(#access)?;),
            ),
            Self::Bool => Emission::new(
                quote!(if #access { 4usize } else { 5usize }),
                quote! {
                    if #access {
                        __w.put(b"true")?;
                    } else {
                        __w.put(b"false")?;
                    }
                },
            ),
            Self::Bytes {
                view,
                encoding,
                length,
            } => Emission::bytes(&access, *view, *encoding, *length),
            Self::Nested { borrow } => {
                let access = if *borrow { quote!(&#access) } else { access };
                Emission::new(
                    quote!(sark::json::JsonEncode::json_len(#access)),
                    quote!(sark::json::JsonEncode::write_into(#access, __w)?;),
                )
            }
            Self::Sequence(element) => element.emit(quote!(__e)).sequence(&access),
        }
    }

    fn optional_access(&self) -> TokenStream {
        match self {
            Self::U64 | Self::I64 | Self::F64 | Self::Bool => quote!(*value),
            Self::Bytes { .. } | Self::Nested { .. } | Self::Sequence(_) => quote!(value),
        }
    }
}

impl ByteView {
    fn apply(self, access: &TokenStream) -> TokenStream {
        match self {
            Self::Bytes => quote!(#access.as_bytes()),
            Self::Slice => quote!(#access.as_slice()),
        }
    }
}

impl From<FieldMode> for ByteEncoding {
    fn from(mode: FieldMode) -> Self {
        if mode.raw {
            Self::Raw
        } else if mode.plain {
            Self::Plain
        } else {
            Self::Escaped
        }
    }
}

impl ByteLength {
    fn from_sequence(sequence: bool) -> Self {
        if sequence { Self::View } else { Self::Direct }
    }

    fn apply(self, access: &TokenStream, view: ByteView) -> TokenStream {
        match self {
            Self::Direct => access.clone(),
            Self::View => view.apply(access),
        }
    }
}

impl Emission {
    fn new(len: TokenStream, write: TokenStream) -> Self {
        Self { len, write }
    }

    fn bytes(
        access: &TokenStream,
        view: ByteView,
        encoding: ByteEncoding,
        length: ByteLength,
    ) -> Self {
        match encoding {
            ByteEncoding::Raw => {
                let len_bytes = length.apply(access, view);
                let write_bytes = view.apply(access);
                Self::new(quote!(#len_bytes.len()), quote!(__w.put(#write_bytes)?;))
            }
            ByteEncoding::Plain => {
                let len_bytes = length.apply(access, view);
                let write_bytes = view.apply(access);
                Self::new(
                    quote!(2usize + #len_bytes.len()),
                    quote!(__w.put_str_plain(#write_bytes)?;),
                )
            }
            ByteEncoding::Escaped => {
                let bytes = view.apply(access);
                Self::new(
                    quote!(sark::json::Encode::str_len(#bytes)),
                    quote!(__w.put_str(#bytes)?;),
                )
            }
        }
    }

    fn sequence(self, access: &TokenStream) -> Self {
        let element_len = self.len;
        let element_write = self.write;
        Self::new(
            quote! {{
                let mut __n = 2usize;
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __n += 1;
                    }
                    __first = false;
                    __n += #element_len;
                }
                __n
            }},
            quote! {{
                __w.put(b"[")?;
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __w.put(b",")?;
                    }
                    __first = false;
                    #element_write
                }
                __w.put(b"]")?;
            }},
        )
    }

    fn optional(self, access: &TokenStream) -> Self {
        let present_len = self.len;
        let present_write = self.write;
        Self::new(
            quote! {
                match &#access {
                    Some(value) => #present_len,
                    None => 4usize,
                }
            },
            quote! {
                match &#access {
                    Some(value) => { #present_write }
                    None => { __w.put(b"null")?; }
                }
            },
        )
    }
}
