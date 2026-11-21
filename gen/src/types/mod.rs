use syn::ext::IdentExt as _;

use crate::runtime::paths::Paths;

mod generics;
mod lifetime;

pub(crate) mod lengths;

pub(crate) use generics::{Generics, Imports};
pub(super) use lifetime::Lifetimes;

#[derive(Clone, Copy)]
pub(super) enum ValueKind {
    Bytes,
    Usize,
    U64,
    Bool,
    Encoding,
}

#[repr(transparent)]
pub(super) struct View<'a> {
    ty: &'a syn::Type,
}

impl<'a> View<'a> {
    pub(super) fn new(ty: &'a syn::Type) -> Self {
        Self { ty }
    }

    pub(super) fn option(&self) -> Option<&'a syn::Type> {
        generic_inner(self.ty, "Option")
    }

    pub(super) fn vector(&self) -> Option<&'a syn::Type> {
        generic_inner(self.ty, "Vec")
    }

    pub(super) fn value(&self) -> &'a syn::Type {
        match self.option() {
            Some(inner) => inner,
            None => self.ty,
        }
    }

    pub(super) fn is_optional(&self) -> bool {
        self.option().is_some()
    }

    pub(super) fn is_named(&self, want: &str) -> bool {
        let syn::Type::Path(path) = self.ty else {
            return false;
        };
        path.path
            .segments
            .last()
            .is_some_and(|seg| seg.ident.unraw() == want)
    }

    pub(super) fn is_slice(&self) -> bool {
        let syn::Type::Reference(input) = self.ty else {
            return false;
        };
        let syn::Type::Slice(slice) = input.elem.as_ref() else {
            return false;
        };
        input.mutability.is_none() && Self::new(&slice.elem).is_named("u8")
    }

    pub(super) fn is_fixed(&self) -> bool {
        let syn::Type::Path(path) = self.ty else {
            return false;
        };
        let mut segments = path.path.segments.iter().rev();
        let Some(body) = segments.next() else {
            return false;
        };
        if body.ident.unraw() != "Body" {
            return false;
        }
        let Some(module) = segments.next() else {
            return false;
        };
        module.ident.unraw() == "fixed"
    }

    pub(super) fn raw(&self, paths: &Paths, kind: ValueKind) -> syn::Type {
        let ns_http = &paths.http;
        match kind {
            ValueKind::Bytes => {
                syn::parse_quote! { ::core::option::Option<#ns_http::request::span::Span> }
            }
            _ => {
                if self.is_optional() {
                    self.ty.clone()
                } else {
                    let ty = self.ty;
                    syn::parse_quote! { ::core::option::Option<#ty> }
                }
            }
        }
    }

    pub(super) fn kind(&self) -> syn::Result<ValueKind> {
        let inner = self.value();
        if Self::new(inner).is_named("Shared") {
            return Ok(ValueKind::Bytes);
        }
        if Self::new(inner).is_named("usize") {
            return Ok(ValueKind::Usize);
        }
        if Self::new(inner).is_named("u64") {
            return Ok(ValueKind::U64);
        }
        if Self::new(inner).is_named("bool") {
            return Ok(ValueKind::Bool);
        }
        if Self::new(inner).is_named("AcceptEncoding") {
            return Ok(ValueKind::Encoding);
        }
        Err(syn::Error::new_spanned(
            self.ty,
            "request fields require Shared, usize, u64, or bool, optionally Option; accept-encoding also supports Option<AcceptEncoding>",
        ))
    }
}

pub(super) fn retained(ty: &syn::Type, paths: &Paths) -> syn::Type {
    let mut owned = ty.clone();
    if let Some(bytes) = shared_bytes(&mut owned) {
        let http = &paths.http;
        *bytes = syn::parse_quote!(#http::request::Bytes<'d>);
    }
    owned
}

pub(super) fn shared_bytes(ty: &mut syn::Type) -> Option<&mut syn::Type> {
    if View::new(ty).is_named("Shared") {
        return Some(ty);
    }
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last_mut()?;
    if segment.ident.unraw() != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    let syn::GenericArgument::Type(inner) = args.args.first_mut()? else {
        return None;
    };
    View::new(inner).is_named("Shared").then_some(inner)
}

fn generic_inner<'a>(ty: &'a syn::Type, want: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let seg = path.path.segments.last()?;
    if seg.ident.unraw() != want {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    let syn::GenericArgument::Type(inner) = args.args.first()? else {
        return None;
    };
    Some(inner)
}
