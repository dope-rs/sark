use proc_macro2::TokenStream;
use quote::quote;
use syn::{Result, Type};

use super::field::FieldMode;
use super::scalar::{Classified, Scalar};
use crate::util::TypeExt;

pub(super) struct Encoder<'a> {
    ty: &'a Type,
    mode: FieldMode,
    access: TokenStream,
    class: Option<Classified>,
}

impl<'a> Encoder<'a> {
    pub(super) fn new(ty: &'a Type, mode: FieldMode, access: TokenStream) -> Result<Self> {
        let class = if mode.nested {
            if mode.seq && ty.vec_inner().is_none() {
                return Err(syn::Error::new_spanned(
                    ty,
                    "#[field(seq)] requires a Vec<T> field",
                ));
            }
            None
        } else if mode.seq {
            let elem = ty.vec_inner().ok_or_else(|| {
                syn::Error::new_spanned(ty, "#[field(seq)] requires a Vec<T> field")
            })?;
            let class = Classified::of(elem)?;
            if class.optional {
                return Err(syn::Error::new_spanned(
                    elem,
                    "sequence field elements cannot be Option<T>",
                ));
            }
            Some(class)
        } else {
            Some(Classified::of(ty)?)
        };
        Ok(Self {
            ty,
            mode,
            access,
            class,
        })
    }

    pub(super) fn len_expr(&self) -> Result<TokenStream> {
        let fmode = self.mode;
        let access = &self.access;
        if fmode.seq {
            let elem = if fmode.nested {
                quote!(sark::json::JsonEncode::json_len(__e))
            } else {
                let Some(class) = self.class else {
                    return Err(syn::Error::new_spanned(
                        self.ty,
                        "missing sequence element classification",
                    ));
                };
                let bytes = match class.scalar {
                    Scalar::String | Scalar::InlineToken => quote!(__e.as_bytes()),
                    Scalar::Shared | Scalar::Retained => quote!(__e.as_slice()),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            self.ty,
                            "sequence field element must be a byte string",
                        ));
                    }
                };
                if fmode.raw {
                    quote!(#bytes.len())
                } else if fmode.plain {
                    quote!(2usize + #bytes.len())
                } else {
                    quote!(sark::json::Encode::str_len(#bytes))
                }
            };
            return Ok(quote! {{
                let mut __n = 2usize;
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __n += 1;
                    }
                    __first = false;
                    __n += #elem;
                }
                __n
            }});
        }
        if fmode.nested {
            return Ok(quote!(sark::json::JsonEncode::json_len(&#access)));
        }
        let Some(class) = self.class else {
            return Err(syn::Error::new_spanned(
                self.ty,
                "missing scalar classification",
            ));
        };
        let len = match class.scalar {
            Scalar::U64 => quote!(sark::json::Encode::u64_len(#access)),
            Scalar::I64 => quote!(sark::json::Encode::i64_len(#access)),
            Scalar::F64 => quote!(sark::json::Encode::f64_len(#access)),
            Scalar::Bool => quote!(if #access { 4usize } else { 5usize }),
            Scalar::String => quote!(sark::json::Encode::str_len(#access.as_bytes())),
            Scalar::Shared => {
                if fmode.raw {
                    quote!(#access.len())
                } else if fmode.plain {
                    quote!(2usize + #access.len())
                } else {
                    quote!(sark::json::Encode::str_len(#access.as_slice()))
                }
            }
            Scalar::Retained => {
                if fmode.raw {
                    quote!(#access.len())
                } else if fmode.plain {
                    quote!(2usize + #access.len())
                } else {
                    quote!(sark::json::Encode::str_len(#access.as_slice()))
                }
            }
            Scalar::InlineToken => {
                if fmode.raw {
                    quote!(#access.len())
                } else if fmode.plain {
                    quote!(2usize + #access.len())
                } else {
                    quote!(sark::json::Encode::str_len(#access.as_bytes()))
                }
            }
        };
        Ok(if class.optional {
            quote! {
                match #access {
                    Some(value) => #len,
                    None => 4usize,
                }
            }
        } else {
            len
        })
    }

    pub(super) fn write_expr(&self) -> Result<TokenStream> {
        let fmode = self.mode;
        let access = &self.access;
        if fmode.seq {
            let elem = if fmode.nested {
                quote!(sark::json::JsonEncode::write_into(__e, __w);)
            } else {
                let Some(class) = self.class else {
                    return Err(syn::Error::new_spanned(
                        self.ty,
                        "missing sequence element classification",
                    ));
                };
                let bytes = match class.scalar {
                    Scalar::String | Scalar::InlineToken => quote!(__e.as_bytes()),
                    Scalar::Shared | Scalar::Retained => quote!(__e.as_slice()),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            self.ty,
                            "sequence field element must be a byte string",
                        ));
                    }
                };
                if fmode.raw {
                    quote!(__w.put(#bytes);)
                } else if fmode.plain {
                    quote!(__w.put_str_plain(#bytes);)
                } else {
                    quote!(__w.put_str(#bytes);)
                }
            };
            return Ok(quote! {{
                __w.put(b"[");
                let mut __first = true;
                for __e in (#access).iter() {
                    if !__first {
                        __w.put(b",");
                    }
                    __first = false;
                    #elem
                }
                __w.put(b"]");
            }});
        }
        if fmode.nested {
            return Ok(quote!(sark::json::JsonEncode::write_into(&#access, __w);));
        }
        let Some(class) = self.class else {
            return Err(syn::Error::new_spanned(
                self.ty,
                "missing scalar classification",
            ));
        };
        let write = match class.scalar {
            Scalar::U64 => quote!(__w.put_u64(#access);),
            Scalar::I64 => quote!(__w.put_i64(#access);),
            Scalar::F64 => quote!(__w.put_f64(#access);),
            Scalar::Bool => quote! {
                if #access {
                    __w.put(b"true");
                } else {
                    __w.put(b"false");
                }
            },
            Scalar::String => quote!(__w.put_str(#access.as_bytes());),
            Scalar::Shared => {
                if fmode.raw {
                    quote!(__w.put(#access.as_slice());)
                } else if fmode.plain {
                    quote!(__w.put_str_plain(#access.as_slice());)
                } else {
                    quote!(__w.put_str(#access.as_slice());)
                }
            }
            Scalar::Retained => {
                if fmode.raw {
                    quote!(__w.put(#access.as_slice());)
                } else if fmode.plain {
                    quote!(__w.put_str_plain(#access.as_slice());)
                } else {
                    quote!(__w.put_str(#access.as_slice());)
                }
            }
            Scalar::InlineToken => {
                if fmode.raw {
                    quote!(__w.put(#access.as_bytes());)
                } else if fmode.plain {
                    quote!(__w.put_str_plain(#access.as_bytes());)
                } else {
                    quote!(__w.put_str(#access.as_bytes());)
                }
            }
        };
        Ok(if class.optional {
            quote! {
                match #access {
                    Some(value) => { #write }
                    None => { __w.put(b"null"); }
                }
            }
        } else {
            write
        })
    }
}
